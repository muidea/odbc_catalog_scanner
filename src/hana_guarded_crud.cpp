//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hana_guarded_crud.cpp
//
// Restricted SAP HANA mutations on an isolated scanner-owned session.
//===----------------------------------------------------------------------===//
#include "duckdb.hpp"
#include "guarded_odbc_source_error.hpp"
#include "odbc_guarded_row_count.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "storage/odbc_catalog.hpp"
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_set>
namespace duckdb {
namespace {
static void Reject() {
	throw GuardedODBCLocalRejection("guarded HANA operation rejected");
}
static string JSON(const string &s) {
	string out = "\"";
	for (auto c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 32)
				Reject();
			out += c;
		}
	}
	return out + "\"";
}
static bool SimpleName(const string &s) {
	if (s.empty() || s.size() > 128)
		return false;
	for (auto c : s)
		if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_' && c != '$' &&
		    c != '#')
			return false;
	return true;
}
static bool IsHanaSystemSchema(const string &schema) {
	auto normalized = StringUtil::Lower(schema);
	StringUtil::Trim(normalized);
	// SYSTEM is the default user schema on standalone HANA tenants; only block metadata schemas here.
	return normalized == "information_schema" || normalized == "sys" || StringUtil::StartsWith(normalized, "_sys_");
}
static string Identifier(const string &s) {
	return ODBCUtils::WriteIdentifier(s);
}
static string Literal(const string &s) {
	return ODBCUtils::WriteLiteral(s);
}
static string Trimmed(string v) {
	StringUtil::Trim(v);
	return v;
}
static bool IsSafeHanaDefault(const string &data_type, const string &raw) {
	if (data_type == "VARCHAR" || data_type == "NVARCHAR" || data_type == "CHAR" || data_type == "NCHAR" ||
	    data_type == "CLOB" || data_type == "NCLOB") {
		if (raw.size() > 1024)
			return false;
		for (auto ch : raw) {
			if (static_cast<unsigned char>(ch) < 0x20 || ch == '(' || ch == ')')
				return false;
		}
		auto upper = StringUtil::Upper(Trimmed(raw));
		static const char *const kVolatileRegisters[] = {
		    "CURRENT_USER",    "CURRENT_SCHEMA",    "SESSION_USER",       "CURRENT_DATE",
		    "CURRENT_TIME",    "CURRENT_TIMESTAMP", "CURRENT_UTCDATE",    "CURRENT_UTCTIME",
		    "CURRENT_UTCTIMESTAMP", "SYSUUID"};
		for (const auto *reg : kVolatileRegisters) {
			if (upper == reg)
				return false;
		}
		return true;
	}
	auto s = Trimmed(raw);
	if (s.empty())
		return false;
	auto upper = StringUtil::Upper(s);
	if (upper == "NULL")
		return true;
	if (data_type == "BOOLEAN")
		return upper == "TRUE" || upper == "FALSE";
	if (data_type == "TINYINT" || data_type == "SMALLINT" || data_type == "INTEGER" || data_type == "BIGINT" ||
	    data_type == "DECIMAL" || data_type == "REAL" || data_type == "DOUBLE") {
		if (s.size() > 64)
			return false;
		idx_t pos = (s[0] == '+' || s[0] == '-') ? 1 : 0;
		if (pos >= s.size())
			return false;
		bool seen_dot = false;
		bool seen_before = false;
		bool seen_after = false;
		for (; pos < s.size(); pos++) {
			char ch = s[pos];
			if (ch >= '0' && ch <= '9') {
				if (seen_dot)
					seen_after = true;
				else
					seen_before = true;
			} else if (ch == '.' && !seen_dot) {
				seen_dot = true;
			} else {
				return false;
			}
		}
		return seen_before && (!seen_dot || seen_after);
	}
	if (data_type == "DATE" || data_type == "TIME" || data_type == "SECONDDATE" || data_type == "TIMESTAMP") {
		if (s.size() < 4 || s.size() > 32 || s[0] < '0' || s[0] > '9')
			return false;
		for (auto ch : s) {
			if (!(ch >= '0' && ch <= '9') && ch != '-' && ch != ':' && ch != ' ' && ch != '.' && ch != 'T')
				return false;
		}
		return true;
	}
	return false;
}
static bool IsValidUint(const string &v) {
	return !v.empty() && v.find_first_not_of("0123456789") == string::npos;
}
static bool IsAllowedHanaIndexType(const string &index_type) {
	return index_type == "INVERTED VALUE" || index_type == "INVERTED VALUE UNIQUE" || index_type == "INVERTED HASH" ||
	       index_type == "INVERTED HASH UNIQUE" || index_type == "INVERTED INDIVIDUAL" ||
	       index_type == "INVERTED INDIVIDUAL UNIQUE" || index_type == "CPBTREE" || index_type == "CPBTREE UNIQUE" ||
	       index_type == "BTREE" || index_type == "BTREE UNIQUE";
}
class Session {
public:
	Session(ClientContext &ctx_p, SQLHDBC dbc) : ctx(ctx_p), stmt(nullptr, ODBCStatementDelete) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS)
			Reject();
		stmt = ODBCStatementPtr(h, ODBCStatementDelete);
		// Keep deadlines local to this session instead of relying on driver
		// query timeouts that may install process-wide signal handlers.
		watcher = std::thread([this] {
			std::unique_lock<std::mutex> lock(mu);
			auto end = std::chrono::steady_clock::now() + std::chrono::seconds(30);
			while (!stopped) {
				auto now = std::chrono::steady_clock::now();
				if (!cancel_sent &&
				    ((!cleanup_mode && (ctx.IsInterrupted() || now >= end)) || now >= statement_deadline)) {
					cancelled.store(true);
					cancel_sent = true;
					SQLCancelHandle(SQL_HANDLE_STMT, stmt.get());
				}
				cv.wait_for(lock, std::chrono::milliseconds(25), [this] { return stopped; });
			}
		});
	}
	~Session() {
		{
			std::lock_guard<std::mutex> lock(mu);
			stopped = true;
		}
		cv.notify_all();
		if (watcher.joinable())
			watcher.join();
	}
	string error;
	GuardedODBCSourceError source;
	void Check(SQLRETURN rc) {
		if (rc == SQL_SUCCESS)
			return;
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "hana");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLCHAR message[1] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, message, sizeof(message), &len);

		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
		} else {
			switch (native) {
			case 301:
				error = "23505";
				break;
			case 287:
				error = "23502";
				break;
			case 461:
			case 462:
				error = "23503";
				break;
			case 456:
			case 457:
				error = "23514";
				break;
			case 293:
			case 314:
			case 339:
				error = "22003";
				break;
			case 274:
				error = "22001";
				break;
			case 303:
				error = "22007";
				break;
			case 131:
			case 146:
				error = "55P03";
				break;
			default:
				error = state[0] ? string(reinterpret_cast<char *>(state)) : "XX000";
				break;
			}
		}
		Reject();
	}
	void Start(bool cleanup = false) {
		if (!cleanup && (cancelled.load() || ctx.IsInterrupted())) {
			error = "57014";
			Reject();
		}
		{
			std::lock_guard<std::mutex> lock(mu);
			cleanup_mode = cleanup;
			cancel_sent = false;
			statement_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		}
		cv.notify_all();
		Check(SQLFreeStmt(stmt.get(), SQL_CLOSE));
		Check(SQLFreeStmt(stmt.get(), SQL_UNBIND));
		Check(SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS));
	}
	void Exec(const string &sql, bool cleanup = false) {
		Start(cleanup);
		auto rc = SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS);
		if (rc == SQL_NO_DATA)
			return;
		Check(rc);
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA)
			Check(rc);
	}
	vector<vector<string>> Rows(const string &sql, bool cleanup = false) {
		Start(cleanup);
		Check(SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols < 1 || cols > 64)
			Reject();
		vector<vector<string>> rows;
		idx_t bytes = 0;
		SQLRETURN rc;
		while ((rc = SQLFetch(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			if (rows.size() >= 8192)
				Reject();
			vector<string> row;
			for (SQLUSMALLINT col = 1; col <= cols; col++) {
				char value[4096] = {0};
				SQLLEN len = 0;
				Check(SQLGetData(stmt.get(), col, SQL_C_CHAR, value, sizeof(value), &len));
				if (len == SQL_NULL_DATA || len < 0 || len >= sizeof(value))
					Reject();
				row.emplace_back(value, static_cast<size_t>(len));
				bytes += row.back().size();
				if (bytes > 1024 * 1024)
					Reject();
			}
			rows.push_back(std::move(row));
		}
		// Metadata SELECTs must have exactly one result, fully consumed.
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		return rows;
	}
	string Scalar(const string &sql, bool cleanup = false) {
		auto r = Rows(sql, cleanup);
		if (r.size() != 1 || r[0].size() != 1)
			Reject();
		return r[0][0];
	}
	int64_t Execute(const string &sql, const vector<Value> &params, bool &sent, string &stage) {
		stage = "source_prepare";
		Start();
		Check(SQLPrepare(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != params.size() || count > 2100)
			Reject();
		vector<int64_t> values(params.size(), 0);
		vector<uint8_t> bits(params.size(), 0);
		vector<float> floats(params.size(), 0.0f);
		vector<double> doubles(params.size(), 0.0);
		vector<string> texts(params.size());
		vector<string> blobs(params.size());
		vector<SQLLEN> lengths(params.size(), 0);
		char empty_blob_sentinel = 0;
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			switch (v.type().id()) {
			case LogicalTypeId::BOOLEAN:
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
				} else {
					bits[i] = v.GetValue<bool>() ? 1 : 0;
					lengths[i] = 1;
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, 1, 0, &bits[i],
				                       sizeof(uint8_t), &lengths[i]));
				continue;
			case LogicalTypeId::FLOAT:
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
				} else {
					floats[i] = v.GetValue<float>();
					if (!std::isfinite(floats[i]))
						Reject();
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_REAL, 7, 0, &floats[i],
				                       sizeof(float), &lengths[i]));
				continue;
			case LogicalTypeId::DOUBLE:
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
				} else {
					doubles[i] = v.GetValue<double>();
					if (!std::isfinite(doubles[i]))
						Reject();
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 15, 0, &doubles[i],
				                       sizeof(double), &lengths[i]));
				continue;
			case LogicalTypeId::VARCHAR:
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 1, 0, nullptr,
					                       0, &lengths[i]));
				} else {
					// HDBODBC corrupts non-ASCII Unicode under SQL_C_WCHAR, whereas SQL_C_CHAR preserves UTF-8 cleanly.
					texts[i] = v.GetValue<string>();
					if (texts[i].size() > 1048576 || texts[i].find('\0') != string::npos)
						Reject();
					lengths[i] = static_cast<SQLLEN>(texts[i].size());
					SQLSMALLINT sql_type = texts[i].size() > 5000 ? SQL_LONGVARCHAR : SQL_VARCHAR;
					SQLULEN col_size = MaxValue<SQLULEN>(1, texts[i].size());
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, sql_type, col_size, 0,
					                       const_cast<char *>(texts[i].c_str()), texts[i].size() + 1, &lengths[i]));
				}
				continue;
			case LogicalTypeId::BLOB:
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, 1, 0,
					                       nullptr, 0, &lengths[i]));
				} else {
					blobs[i] = StringValue::Get(v);
					if (blobs[i].size() > 1048576)
						Reject();
					lengths[i] = static_cast<SQLLEN>(blobs[i].size());
					SQLSMALLINT sql_type = blobs[i].size() > 5000 ? SQL_LONGVARBINARY : SQL_VARBINARY;
					SQLULEN col_size = MaxValue<SQLULEN>(1, blobs[i].size());
					SQLPOINTER ptr = blobs[i].empty() ? static_cast<SQLPOINTER>(&empty_blob_sentinel)
					                                  : static_cast<SQLPOINTER>(const_cast<char *>(blobs[i].data()));
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, sql_type, col_size, 0, ptr,
					                       blobs[i].size(), &lengths[i]));
				}
				continue;
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::BIGINT:
				if (v.IsNull())
					lengths[i] = SQL_NULL_DATA;
				else
					values[i] = v.GetValue<int64_t>();
				break;
			default:
				if (v.IsNull())
					lengths[i] = SQL_NULL_DATA;
				else
					Reject();
				break;
			}
			Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 19, 0, &values[i],
			                       sizeof(int64_t), &lengths[i]));
		}

		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
			Reject();
		}
		stage = "source_execute";
		sent = true;
		auto rc = SQLExecute(stmt.get());
		if (rc != SQL_NO_DATA)
			Check(rc);
		SQLLEN first, second;
		std::memset(&first, 0xa5, sizeof(first));
		std::memset(&second, 0x5a, sizeof(second));
		Check(SQLRowCount(stmt.get(), &first));
		Check(SQLRowCount(stmt.get(), &second));
		int64_t affected = -1;
		if (!DecodeGuardedODBCRowCount(first, second, affected))
			Reject();
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols != 0)
			Reject();
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		return affected;
	}

private:
	ClientContext &ctx;
	ODBCStatementPtr stmt;
	std::mutex mu;
	std::condition_variable cv;
	bool stopped = false, cleanup_mode = false, cancel_sent = true;
	std::chrono::steady_clock::time_point statement_deadline = std::chrono::steady_clock::time_point::max();
	std::atomic<bool> cancelled {false};
	std::thread watcher;
};
struct Data : TableFunctionData {
	ODBCCatalog &catalog;
	string schema, table, expected, version, sql;
	vector<Value> params;
	bool execute;
	bool diagnostics = false;
	Data(ODBCCatalog &c, bool e) : catalog(c), execute(e) {
	}
};
struct State : GlobalTableFunctionState {
	bool done = false;
};
static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<State>();
}
static ODBCCatalog &CatalogFor(ClientContext &ctx, const string &name) {
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, name);
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::HANA)
		Reject();
	return catalog;
}
static unique_ptr<FunctionData> Bind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                     vector<string> &names) {
	for (auto &v : in.inputs)
		if (v.IsNull())
			Reject();
	auto d = make_uniq<Data>(CatalogFor(ctx, in.inputs[0].GetValue<string>()), in.inputs.size() == 6);
	d->schema = in.inputs[1].GetValue<string>();
	d->table = in.inputs[2].GetValue<string>();
	if (!SimpleName(d->schema) || !SimpleName(d->table) || IsHanaSystemSchema(d->schema))
		Reject();
	if (d->execute) {
		d->expected = in.inputs[3].GetValue<string>();
		d->version = in.inputs[4].GetValue<string>();
		d->sql = in.inputs[5].GetValue<string>();
		if (d->expected.empty() || d->expected.size() > 1024 * 1024 || d->sql.empty() ||
		    d->sql.size() > 4 * 1024 * 1024)
			Reject();
		auto it = in.named_parameters.find("params");
		if (it != in.named_parameters.end()) {
			if (it->second.IsNull() || it->second.type().id() != LogicalTypeId::STRUCT)
				Reject();
			d->params = StructValue::GetChildren(it->second);
		}
		if (d->params.size() > 2100)
			Reject();
	}
	auto diagnostic = in.named_parameters.find("source_diagnostics");
	if (diagnostic != in.named_parameters.end()) {
		if (diagnostic->second.IsNull())
			Reject();
		d->diagnostics = diagnostic->second.GetValue<bool>();
	}
	names = {"status", "affected_rows", "sqlstate", "stage", "version", "descriptor"};
	types = {LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::VARCHAR,
	         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	if (d->diagnostics) {
		names.push_back("source_error");
		types.push_back(LogicalType::VARCHAR);
	}
	return std::move(d);
}
static string JSONRows(const vector<vector<string>> &rows) {
	string out = "[";
	bool first = true;
	for (auto &row : rows) {
		if (!first)
			out += ",";
		first = false;
		out += "[";
		bool cell = true;
		for (auto &v : row) {
			if (!cell)
				out += ",";
			cell = false;
			out += JSON(v);
		}
		out += "]";
		if (out.size() > 1024 * 1024)
			Reject();
	}
	return out + "]";
}
static string Info(SQLHDBC dbc, SQLUSMALLINT key) {
	SQLCHAR value[256] = {0};
	SQLSMALLINT len = 0;
	if (SQLGetInfo(dbc, key, value, sizeof(value), &len) != SQL_SUCCESS || len <= 0 || len >= sizeof(value))
		Reject();
	return string(reinterpret_cast<char *>(value), len);
}
// Version is an opaque source identity. Capability is proved by the live
// session/catalog checks below, never by a fixed release interval.
static bool Compatible(const string &v) {
	if (v.empty() || v.size() > 1024 || StringUtil::CharacterIsSpace(v.front()) ||
	    StringUtil::CharacterIsSpace(v.back()))
		return false;
	for (auto c : v)
		if (static_cast<unsigned char>(c) < 32 || static_cast<unsigned char>(c) == 127)
			return false;
	return true;
}
static void Writable(SQLHDBC dbc) {
	if (SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0) !=
	        SQL_SUCCESS ||
	    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0) != SQL_SUCCESS)
		Reject();
	SQLULEN mode = SQL_AUTOCOMMIT_ON;
	if (SQLGetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, &mode, sizeof(mode), nullptr) != SQL_SUCCESS ||
	    mode != SQL_AUTOCOMMIT_OFF)
		Reject();
}
static string Metadata(Session &s, const Data &d, string &stage) {
	const auto filter = "SCHEMA_NAME=" + Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.table);
	stage = "source_metadata";
	auto identity = s.Rows("SELECT D.DATABASE_NAME,CURRENT_USER FROM SYS.M_DATABASE D");
	if (identity.size() != 1 || identity[0].size() != 2 || identity[0][0].empty())
		Reject();
	stage = "source_table";
	auto table = s.Rows("SELECT TO_VARCHAR(TABLE_OID),TABLE_TYPE,IS_COLUMN_TABLE,IS_LOGGED,IS_SYSTEM_TABLE,"
	                    "IS_INSERT_ONLY,SESSION_TYPE,IS_TEMPORARY,IS_USER_DEFINED_TYPE,"
	                    "COALESCE(TO_VARCHAR(PARTITION_SPEC),'_'),HAS_SCHEMA_FLEXIBILITY,IS_REPLICA,"
	                    "HAS_STRUCTURED_PRIVILEGE_CHECK,IS_SERIES_TABLE,COALESCE(TEMPORAL_TYPE,'_'),"
	                    "HAS_MASKED_COLUMNS,TO_VARCHAR(CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7') "
	                    "FROM SYS.TABLES WHERE " +
	                    filter);
	if (table.size() != 1)
		Reject();
	auto &t = table[0];
	if ((t[1] != "COLUMN" && t[1] != "ROW") || t[3] != "TRUE" || t[4] != "FALSE" || t[5] != "FALSE" ||
	    (t[6] != "NONE" && t[6] != "SIMPLE") || t[7] != "FALSE" || t[8] != "FALSE" || t[9] != "_" ||
	    t[10] != "FALSE" || t[11] != "FALSE" || t[12] != "FALSE" || t[13] != "FALSE" || t[14] != "_" ||
	    t[15] != "FALSE")
		Reject();
	stage = "source_columns";
	auto cols = s.Rows("SELECT COLUMN_NAME,DATA_TYPE_NAME,TO_VARCHAR(LENGTH),COALESCE(TO_VARCHAR(SCALE),'-1'),"
	                   "IS_NULLABLE,TO_VARCHAR(POSITION),COALESCE(DEFAULT_VALUE,''),"
	                   "COALESCE(GENERATION_TYPE,''),COALESCE(GENERATED_ALWAYS_AS,''),"
	                   "COALESCE(IS_HIDDEN,'FALSE'),COALESCE(IS_MASKED,'FALSE'),COALESCE(COLLATION,'_'),"
	                   "CASE WHEN DEFAULT_VALUE IS NULL THEN '0' ELSE '1' END "
	                   "FROM SYS.TABLE_COLUMNS WHERE " +
	                   filter + " ORDER BY POSITION");
	if (cols.empty() || cols.size() > 1024)
		Reject();
	for (idx_t i = 0; i < cols.size(); i++) {
		auto &c = cols[i];
		if (!SimpleName(c[0]) || c[1].empty() || (c[4] != "TRUE" && c[4] != "FALSE") ||
		    c[5] != std::to_string(i + 1) || (c[12] != "0" && c[12] != "1") || !Trimmed(c[8]).empty() ||
		    c[9] != "FALSE" || c[10] != "FALSE")
			Reject();
		if (!c[7].empty() && c[7] != "ALWAYS AS IDENTITY" && c[7] != "BY DEFAULT AS IDENTITY")
			Reject();
		if (!c[7].empty() && c[12] == "1")
			Reject();
	}
	stage = "source_constraints";
	auto constraints =
	    s.Rows("SELECT CONSTRAINT_NAME,COLUMN_NAME,TO_VARCHAR(POSITION),IS_PRIMARY_KEY,IS_UNIQUE_KEY,"
	           "COALESCE(CHECK_CONDITION,'_') FROM SYS.CONSTRAINTS WHERE " +
	           filter + " ORDER BY CONSTRAINT_NAME,POSITION");
	std::unordered_set<string> pk_names;
	for (auto &c : constraints) {
		if ((c[3] != "TRUE" && c[4] != "TRUE") || c[5] != "_")
			Reject();
		if (c[3] == "TRUE")
			pk_names.insert(c[0]);
	}
	std::unordered_set<string> single_pk_cols;
	for (const auto &pk_name : pk_names) {
		vector<string> pk_cols;
		for (const auto &c : constraints) {
			if (c[0] == pk_name && c[3] == "TRUE")
				pk_cols.push_back(c[1]);
		}
		if (pk_cols.size() == 1)
			single_pk_cols.insert(pk_cols[0]);
	}
	if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.TRIGGERS WHERE SUBJECT_TABLE_SCHEMA=" + Literal(d.schema) +
	             " AND SUBJECT_TABLE_NAME=" + Literal(d.table)) != "0" ||
	    s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE (" + filter +
	             ") OR (REFERENCED_SCHEMA_NAME=" + Literal(d.schema) +
	             " AND REFERENCED_TABLE_NAME=" + Literal(d.table) + ")") != "0")
		Reject();
	stage = "source_indexes";
	auto indexes = s.Rows("SELECT INDEX_NAME,TO_VARCHAR(INDEX_OID),INDEX_TYPE,COALESCE(CONSTRAINT,'_'),"
	                      "COALESCE(STORAGE_TYPE,'_') FROM SYS.INDEXES WHERE " +
	                      filter + " ORDER BY INDEX_NAME");
	for (auto &i : indexes)
		if (!IsAllowedHanaIndexType(i[2]))
			Reject();
	string revision = JSONRows(identity) + JSONRows(table) + JSONRows(cols) + JSONRows(constraints) + JSONRows(indexes);
	string result = "{\"database\":" + JSON(identity[0][0]) + ",\"schema\":" + JSON(d.schema) +
	                ",\"table\":" + JSON(d.table) + ",\"table_type\":" + JSON(t[1]) +
	                ",\"object_id\":" + JSON(t[0] + ":" + t[16]) + ",\"revision\":" + JSON(revision) +
	                ",\"columns\":[";
	bool first = true;
	for (auto &c : cols) {
		if (!first)
			result += ",";
		first = false;
		result += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(c[1]) +
		          ",\"nullable\":" + (c[4] == "TRUE" ? "true" : "false");
		if (!c[7].empty()) {
			result += ",\"is_identity\":true";
			if (c[7] == "ALWAYS AS IDENTITY")
				result += ",\"identity_always\":true";
		}
		if (c[12] == "1") {
			result += ",\"has_default\":true";
			if (IsSafeHanaDefault(c[1], c[6]))
				result += ",\"default_safe\":true";
		}
		if (single_pk_cols.count(c[0]))
			result += ",\"single_column_primary_key\":true";
		if (c[1] == "DECIMAL") {
			if (IsValidUint(c[2]) && IsValidUint(c[3]))
				result += ",\"precision\":" + c[2] + ",\"scale\":" + c[3];
		} else if (c[1] == "TINYINT") {
			result += ",\"precision\":3,\"scale\":0";
		} else if (c[1] == "TIMESTAMP") {
			if (IsValidUint(c[3]))
				result += ",\"scale\":" + c[3];
		} else if (c[1] == "VARCHAR" || c[1] == "NVARCHAR" || c[1] == "CHAR" || c[1] == "NCHAR" || c[1] == "CLOB" ||
		           c[1] == "NCLOB" || c[1] == "VARBINARY" || c[1] == "BLOB") {
			if (IsValidUint(c[2]))
				result += ",\"max_length\":" + c[2];
		}
		result += "}";
	}
	result += "]}";
	if (result.size() > 1024 * 1024)
		Reject();
	return result;
}
static void Scan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<State>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<Data>();
	string status = "CONFIRMED_NOT_EXECUTED", error, stage = "source_metadata", version, descriptor;
	string source_error;
	int64_t affected = -1;
	bool sent = false, commit_started = false, local_rejection = false;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<Session> session;
	try {
		if (ctx.IsInterrupted())
			throw InterruptException();
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::HANA), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		if (Info(dbc, SQL_DBMS_NAME) != "HDB")
			Reject();
		version = Info(dbc, SQL_DBMS_VER);
		if (!Compatible(version) || (d.execute && version != d.version))
			Reject();
		Writable(dbc);
		session = make_uniq<Session>(ctx, dbc);
		auto &s = *session;
		stage = "source_metadata";
		s.Exec("SET TRANSACTION LOCK WAIT TIMEOUT 5000");
		s.Exec("LOCK TABLE " + Identifier(d.schema) + "." + Identifier(d.table) + " IN EXCLUSIVE MODE");
		descriptor = Metadata(s, d, stage);
		if (d.execute) {
			stage = "source_descriptor";
			if (descriptor != d.expected) {
				error = "0A000";
				Reject();
			}
			stage = "source_execute";
			affected = s.Execute(d.sql, d.params, sent, stage);
			stage = "source_commit";
			commit_started = true;
			status = "COMMIT_STATUS_UNKNOWN";
			s.Exec("COMMIT");
			status = "COMMITTED";
			stage.clear();
		} else {
			s.Exec("ROLLBACK");
			stage.clear();
		}
	} catch (...) {
		try {
			throw;
		} catch (const GuardedODBCLocalRejection &) {
			local_rejection = true;
		} catch (const GuardedODBCConnectionFailure &failure) {
			auto source = failure.diagnostic;
			source.product = "hana";
			source_error = source.JSON();
			error = source.state;
			stage = "source_connection";
		} catch (...) {
		}
		// Snapshot before rollback/cancellation cleanup replaces handle diagnostics.
		if (session && source_error.empty())
			source_error = session->source.JSON();
		if (error.empty())
			error = session && !session->error.empty()
			            ? session->error
			            : (ctx.IsInterrupted() ? "57014" : (local_rejection ? "0A000" : "XX000"));
		if (session)
			try {
				session->Exec("ROLLBACK", true);
				if (sent && !commit_started)
					status = "CONFIRMED_ROLLED_BACK";
			} catch (...) {
				if (sent)
					status = "COMMIT_STATUS_UNKNOWN";
			}
	}
	session.reset();
	if (connection)
		connection->Close();
	out.SetCardinality(1);
	out.SetValue(0, 0, Value(status));
	out.SetValue(1, 0, Value::BIGINT(affected));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value(version));
	out.SetValue(5, 0, Value(descriptor));
	if (d.diagnostics)
		out.SetValue(6, 0, Value(source_error));
}
} // namespace
void RegisterHanaGuardedCRUD(ExtensionLoader &loader) {
	TableFunction metadata("hana_crud_metadata_v1", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(metadata);
	TableFunction exec("hana_guarded_mutation_v1", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.named_parameters["params"] = LogicalType::ANY;
	exec.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec);
}
} // namespace duckdb
