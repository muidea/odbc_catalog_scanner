//===----------------------------------------------------------------------===//
//                         DuckDB
//
// db2_guarded_crud.cpp
//
// Restricted Db2 mutations on an isolated scanner-owned session.
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
	throw GuardedODBCLocalRejection("guarded Db2 operation rejected");
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
static string Identifier(const string &s) {
	return ODBCUtils::WriteIdentifier(s);
}
static string Literal(const string &s) {
	return ODBCUtils::WriteLiteral(s);
}
static string StripBalancedOuterParens(string raw) {
	StringUtil::Trim(raw);
	while (raw.size() >= 2 && raw.front() == '(' && raw.back() == ')') {
		int depth = 0;
		bool matches_outer = true;
		for (idx_t i = 0; i < raw.size(); i++) {
			if (raw[i] == '(')
				depth++;
			else if (raw[i] == ')') {
				depth--;
				if (depth == 0 && i + 1 < raw.size()) {
					matches_outer = false;
					break;
				}
				if (depth < 0)
					return raw;
			}
		}
		if (!matches_outer || depth != 0)
			break;
		raw = raw.substr(1, raw.size() - 2);
		StringUtil::Trim(raw);
	}
	return raw;
}
static bool IsSafeDB2Default(const string &raw) {
	string s = StripBalancedOuterParens(raw);
	if (s.empty())
		return false;
	string upper = StringUtil::Upper(s);
	if (upper == "NULL" || upper == "TRUE" || upper == "FALSE")
		return true;
	if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'' && s.size() <= 1024) {
		for (idx_t i = 1; i + 1 < s.size();) {
			if (s[i] == '\0')
				return false;
			if (s[i] == '\'') {
				if (i + 2 >= s.size() || s[i + 1] != '\'')
					return false;
				i += 2;
				continue;
			}
			i++;
		}
		return true;
	}
	if (s.size() > 64)
		return false;
	idx_t pos = (s[0] == '+' || s[0] == '-') ? 1 : 0;
	if (pos >= s.size())
		return false;
	bool seen_dot = false;
	bool seen_digit_before = false;
	bool seen_digit_after = false;
	for (; pos < s.size(); pos++) {
		char ch = s[pos];
		if (ch >= '0' && ch <= '9') {
			if (seen_dot)
				seen_digit_after = true;
			else
				seen_digit_before = true;
		} else if (ch == '.' && !seen_dot) {
			seen_dot = true;
		} else {
			return false;
		}
	}
	return seen_digit_before && (!seen_dot || seen_digit_after);
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
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "db2");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLCHAR message[1] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, message, sizeof(message), &len);
		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
		} else {
			switch (native) {
			case -803:
				error = "23505";
				break;
			case -407:
				error = "23502";
				break;
			case -530:
			case -531:
			case -532:
				error = "23503";
				break;
			case -545:
				error = "23514";
				break;
			case -802:
			case -413:
				error = "22003";
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
	struct BoundParams {
		vector<int64_t> values;
		vector<uint8_t> bits;
		vector<float> floats;
		vector<double> doubles;
		vector<string> text;
		vector<string> blobs;
		vector<SQLLEN> lengths;
		char empty_blob_sentinel = 0;
		explicit BoundParams(idx_t n)
		    : values(n, 0), bits(n, 0), floats(n, 0.0f), doubles(n, 0.0), text(n), blobs(n), lengths(n, 0) {
		}
	};
	void BindPreparedInputParams(const vector<Value> &params, BoundParams &b) {
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			switch (v.type().id()) {
			case LogicalTypeId::BOOLEAN:
				if (v.IsNull()) {
					b.lengths[i] = SQL_NULL_DATA;
				} else {
					b.bits[i] = v.GetValue<bool>() ? 1 : 0;
					b.lengths[i] = 1;
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, 1, 0, &b.bits[i],
				                       sizeof(uint8_t), &b.lengths[i]));
				continue;
			case LogicalTypeId::FLOAT:
				if (v.IsNull()) {
					b.lengths[i] = SQL_NULL_DATA;
				} else {
					b.floats[i] = v.GetValue<float>();
					if (!std::isfinite(b.floats[i]))
						Reject();
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_REAL, 7, 0, &b.floats[i],
				                       sizeof(float), &b.lengths[i]));
				continue;
			case LogicalTypeId::DOUBLE:
				if (v.IsNull()) {
					b.lengths[i] = SQL_NULL_DATA;
				} else {
					b.doubles[i] = v.GetValue<double>();
					if (!std::isfinite(b.doubles[i]))
						Reject();
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 15, 0,
				                       &b.doubles[i], sizeof(double), &b.lengths[i]));
				continue;
			case LogicalTypeId::VARCHAR:
				if (v.IsNull()) {
					b.lengths[i] = SQL_NULL_DATA;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 1, 0, nullptr,
					                       0, &b.lengths[i]));
				} else {
					b.text[i] = v.GetValue<string>();
					if (b.text[i].size() > 1048576 || b.text[i].find('\0') != string::npos)
						Reject();
					b.lengths[i] = static_cast<SQLLEN>(b.text[i].size());
					SQLSMALLINT sql_type = b.text[i].size() > 4000 ? SQL_LONGVARCHAR : SQL_VARCHAR;
					SQLULEN col_size = MaxValue<SQLULEN>(1, b.text[i].size());
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, sql_type, col_size, 0,
					                       const_cast<char *>(b.text[i].c_str()), b.text[i].size() + 1,
					                       &b.lengths[i]));
				}
				continue;
			case LogicalTypeId::BLOB:
				if (v.IsNull()) {
					b.lengths[i] = SQL_NULL_DATA;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, 1, 0,
					                       nullptr, 0, &b.lengths[i]));
				} else {
					b.blobs[i] = StringValue::Get(v);
					if (b.blobs[i].size() > 1048576)
						Reject();
					b.lengths[i] = static_cast<SQLLEN>(b.blobs[i].size());
					SQLSMALLINT sql_type = b.blobs[i].size() > 4000 ? SQL_LONGVARBINARY : SQL_VARBINARY;
					SQLULEN col_size = MaxValue<SQLULEN>(1, b.blobs[i].size());
					SQLPOINTER ptr = b.blobs[i].empty()
					                     ? static_cast<SQLPOINTER>(&b.empty_blob_sentinel)
					                     : static_cast<SQLPOINTER>(const_cast<char *>(b.blobs[i].data()));
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, sql_type, col_size, 0, ptr,
					                       b.blobs[i].size(), &b.lengths[i]));
				}
				continue;
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::BIGINT:
				if (v.IsNull())
					b.lengths[i] = SQL_NULL_DATA;
				else
					b.values[i] = v.GetValue<int64_t>();
				break;
			default:
				if (v.IsNull())
					b.lengths[i] = SQL_NULL_DATA;
				else
					Reject();
				break;
			}
			Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 19, 0, &b.values[i],
			                       sizeof(int64_t), &b.lengths[i]));
		}
	}
	int64_t Execute(const string &sql, const vector<Value> &params, bool &sent, string &stage) {
		stage = "source_prepare";
		Start();
		Check(SQLPrepare(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != params.size() || count > 2100)
			Reject();
		BoundParams b(params.size());
		BindPreparedInputParams(params, b);

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
	int64_t ExecuteReturning(const string &sql, const vector<Value> &params, string &returned_rows, bool &sent,
	                         string &stage) {
		stage = "source_prepare";
		Start();
		Check(SQLPrepare(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != params.size() || count > 2100)
			Reject();
		BoundParams b(params.size());
		BindPreparedInputParams(params, b);

		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
			Reject();
		}
		stage = "source_execute";
		sent = true;
		auto rc = SQLExecute(stmt.get());
		if (rc != SQL_NO_DATA)
			Check(rc);
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols != 1)
			Reject();
		string out = "[";
		int64_t count_rows = 0;
		while ((rc = SQLFetch(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			if (count_rows >= 4096)
				Reject();
			char value[128] = {0};
			SQLLEN len = 0;
			Check(SQLGetData(stmt.get(), 1, SQL_C_CHAR, value, sizeof(value), &len));
			if (count_rows > 0)
				out += ",";
			if (len == SQL_NULL_DATA) {
				out += "[null]";
			} else {
				if (len <= 0 || len >= static_cast<SQLLEN>(sizeof(value)))
					Reject();
				string cell(value, static_cast<size_t>(len));
				while (!cell.empty() && cell.back() == '\0')
					cell.pop_back();
				StringUtil::Trim(cell);
				idx_t pos = (!cell.empty() && (cell[0] == '+' || cell[0] == '-')) ? 1 : 0;
				if (pos >= cell.size() || cell.size() - pos > 20 ||
				    cell.find_first_not_of("0123456789", pos) != string::npos)
					Reject();
				out += "[" + JSON(cell) + "]";
			}
			if (out.size() > 4 * 1024 * 1024)
				Reject();
			count_rows++;
		}
		out += "]";
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			SQLSMALLINT more_cols = 0;
			Check(SQLNumResultCols(stmt.get(), &more_cols));
			if (more_cols != 0)
				Reject();
		}
		returned_rows = std::move(out);
		return count_rows;
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
	bool returning = false;
	string returning_name;
	int64_t returning_oid = 0;
	Data(ODBCCatalog &c, bool e) : catalog(c), execute(e) {
	}
};
struct State : GlobalTableFunctionState {
	bool done = false;
};
static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<State>();
}
static ODBCCatalog &CatalogFor(ClientContext &ctx, const string &name, bool admin = false) {
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, duckdb::Identifier(name));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::DB2 &&
	    !(admin && (catalog.GetDialect() == ODBCDialect::DAMENG || catalog.GetDialect() == ODBCDialect::HANA)))
		Reject();
	return catalog;
}
static unique_ptr<FunctionData> BindInternal(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                             vector<duckdb::Identifier> &names, bool is_returning) {
	for (auto &v : in.inputs)
		if (v.IsNull())
			Reject();
	auto d = make_uniq<Data>(CatalogFor(ctx, in.inputs[0].GetValue<string>()), in.inputs.size() == 6);
	d->returning = is_returning;
	d->schema = in.inputs[1].GetValue<string>();
	d->table = in.inputs[2].GetValue<string>();
	if (!SimpleName(d->schema) || !SimpleName(d->table) ||
	    ODBCDialectUtil::IsInternalSchema(ODBCDialect::DB2, d->schema))
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
	if (d->returning) {
		if (!d->execute)
			Reject();
		auto name_it = in.named_parameters.find("returning_name");
		auto oid_it = in.named_parameters.find("returning_oid");
		if (name_it == in.named_parameters.end() || name_it->second.IsNull() ||
		    name_it->second.type().id() != LogicalTypeId::VARCHAR || oid_it == in.named_parameters.end() ||
		    oid_it->second.IsNull() || oid_it->second.type().id() != LogicalTypeId::BIGINT)
			Reject();
		d->returning_name = name_it->second.GetValue<string>();
		d->returning_oid = oid_it->second.GetValue<int64_t>();
		if (!SimpleName(d->returning_name) ||
		    (d->returning_oid != 20 && d->returning_oid != 21 && d->returning_oid != 23))
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
	if (d->returning) {
		names.push_back("returned_rows");
		types.push_back(LogicalType::VARCHAR);
	}
	if (d->diagnostics) {
		names.push_back("source_error");
		types.push_back(LogicalType::VARCHAR);
	}
	return std::move(d);
}
static unique_ptr<FunctionData> Bind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                     vector<duckdb::Identifier> &names) {
	return BindInternal(ctx, in, types, names, false);
}
static unique_ptr<FunctionData> BindReturning(ClientContext &ctx, TableFunctionBindInput &in,
                                              vector<LogicalType> &types, vector<duckdb::Identifier> &names) {
	return BindInternal(ctx, in, types, names, true);
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
static string Trimmed(string v) {
	StringUtil::Trim(v);
	return v;
}
static string Metadata(Session &s, const Data &d, string &stage, std::unordered_set<string> *column_names = nullptr) {
	const auto filter = "TABSCHEMA=" + Literal(d.schema) + " AND TABNAME=" + Literal(d.table);
	stage = "source_metadata";
	auto identity = s.Rows("SELECT RTRIM(CURRENT SERVER),RTRIM(CURRENT USER) FROM SYSIBM.SYSDUMMY1");
	if (identity.size() != 1 || identity[0].size() != 2)
		Reject();
	stage = "source_table";
	auto table =
	    s.Rows("SELECT RTRIM(OWNER),TYPE,STATUS,CHAR(TABLEID),CHAR(TBSPACEID),CHAR(CREATE_TIME),CHAR(ALTER_TIME),"
	           "PARTITION_MODE,CHAR(PARENTS),CHAR(CHILDREN),CHAR(SELFREFS),CHAR(CHECKCOUNT),ACCESS_MODE,"
	           "CHAR(COALESCE(SECPOLICYID,0)),TEMPORALTYPE,TABLEORG,COALESCE(CONTROL,'') FROM SYSCAT.TABLES WHERE " +
	           filter);
	if (table.size() != 1)
		Reject();
	auto &t = table[0];
	if (t[0] != identity[0][1] || t[1] != "T" || t[2] != "N" || t[7] != " " || Trimmed(t[8]) != "0" ||
	    Trimmed(t[9]) != "0" || Trimmed(t[10]) != "0" || Trimmed(t[11]) != "0" || t[12] != "F" ||
	    Trimmed(t[13]) != "0" || t[14] != "N" || t[15] != "R" || !Trimmed(t[16]).empty())
		Reject();
	stage = "source_columns";
	auto cols = s.Rows("SELECT COLNAME,TYPESCHEMA,TYPENAME,CHAR(COLNO),NULLS,COALESCE(DEFAULT,''),HIDDEN,IDENTITY,"
	                   "GENERATED,COALESCE(TEXT,''),ROWCHANGETIMESTAMP,ROWBEGIN,ROWEND,TRANSACTIONSTARTID,"
	                   "COALESCE(SECLABELNAME,''),CHAR(LENGTH),CHAR(SCALE),CHAR(CODEPAGE),COALESCE(TYPESTRINGUNITS,''),"
	                   "CHAR(COALESCE(STRINGUNITSLENGTH,0)),CASE WHEN DEFAULT IS NULL THEN '0' ELSE '1' END "
	                   "FROM SYSCAT.COLUMNS WHERE " +
	                   filter + " ORDER BY COLNO");
	if (cols.empty() || cols.size() > 1024)
		Reject();
	idx_t ordinal = 0;
	for (auto &c : cols) {
		if (!SimpleName(c[0]) || c[2].empty() || Trimmed(c[3]) != std::to_string(ordinal++) ||
		    (c[4] != "Y" && c[4] != "N") || c[6] != " " || !c[9].empty() || c[10] != "N" || c[11] != "N" ||
		    c[12] != "N" || c[13] != "N" || !c[14].empty() || (c[20] != "0" && c[20] != "1"))
			Reject();
		bool is_identity = (c[7] == "Y");
		bool has_default = (c[20] == "1");
		if (is_identity) {
			if ((c[8] != "A" && c[8] != "D") || has_default)
				Reject();
		} else if (c[7] != "N" || c[8] != " ") {
			Reject();
		}
	}
	stage = "source_constraints";
	auto constraints = s.Rows("SELECT CONSTNAME,TYPE,ENFORCED,TRUSTED,CHECKEXISTINGDATA FROM SYSCAT.TABCONST WHERE " +
	                          filter + " ORDER BY CONSTNAME");
	std::unordered_set<string> pk_names;
	for (auto &c : constraints) {
		if ((c[1] != "P" && c[1] != "U") || c[2] != "Y" || c[3] != " " || c[4] != "I")
			Reject();
		if (c[1] == "P")
			pk_names.insert(c[0]);
	}
	auto key_cols = s.Rows(
	    "SELECT TC.CONSTNAME,TC.TYPE,K.COLNAME,CHAR(K.COLSEQ) FROM SYSCAT.TABCONST TC JOIN SYSCAT.KEYCOLUSE K "
	    "ON TC.TABSCHEMA=K.TABSCHEMA AND TC.TABNAME=K.TABNAME AND TC.CONSTNAME=K.CONSTNAME WHERE TC.TABSCHEMA=" +
	    Literal(d.schema) + " AND TC.TABNAME=" + Literal(d.table) + " ORDER BY TC.CONSTNAME,K.COLSEQ");
	std::unordered_set<string> single_pk_cols;
	for (const auto &pk_name : pk_names) {
		vector<string> pk_cols;
		for (const auto &kc : key_cols) {
			if (kc[0] == pk_name)
				pk_cols.push_back(kc[2]);
		}
		if (pk_cols.size() == 1)
			single_pk_cols.insert(pk_cols[0]);
	}
	if (s.Scalar("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.TRIGGERS WHERE " + filter) != "0" ||
	    s.Scalar("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.REFERENCES WHERE (" + filter +
	             ") OR (REFTABSCHEMA=" + Literal(d.schema) + " AND REFTABNAME=" + Literal(d.table) + ")") != "0" ||
	    s.Scalar("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.CONTROLS WHERE " + filter) != "0")
		Reject();
	stage = "source_indexes";
	auto indexes = s.Rows(
	    "SELECT INDSCHEMA,INDNAME,CHAR(IID),UNIQUERULE,INDEXTYPE,COLNAMES,CHAR(CREATE_TIME),CHAR(INDEX_OBJECTID),"
	    "CHAR(TBSPACEID),COALESCE(PERIODNAME,''),COALESCE(PERIODPOLICY,'') FROM SYSCAT.INDEXES WHERE " +
	    filter + " ORDER BY INDSCHEMA,INDNAME");
	for (auto &i : indexes)
		if ((Trimmed(i[4]) != "REG" && Trimmed(i[4]) != "CLUS") || !Trimmed(i[9]).empty() || i[10] != "N")
			Reject();
	string revision = JSONRows(identity) + JSONRows(table) + JSONRows(cols) + JSONRows(constraints) +
	                  JSONRows(key_cols) + JSONRows(indexes);
	string result = "{\"database\":" + JSON(identity[0][0]) + ",\"schema\":" + JSON(d.schema) +
	                ",\"table\":" + JSON(d.table) + ",\"object_id\":" + JSON(t[3] + ":" + t[4] + ":" + t[5]) +
	                ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	auto valid_uint = [](const string &v) {
		return !v.empty() && v.find_first_not_of("0123456789") == string::npos;
	};
	bool first = true;
	for (auto &c : cols) {
		if (column_names)
			column_names->insert(c[0]);
		if (!first)
			result += ",";
		first = false;
		auto schema_type = Trimmed(c[1]);
		auto dtype = schema_type == "SYSIBM" ? c[2] : (schema_type + "." + c[2]);
		result += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(dtype) +
		          ",\"nullable\":" + (c[4] == "Y" ? "true" : "false");
		if (c[7] == "Y") {
			result += ",\"is_identity\":true";
			if (c[8] == "A")
				result += ",\"identity_always\":true";
		}
		if (c[20] == "1") {
			result += ",\"has_default\":true";
			if (IsSafeDB2Default(c[5]))
				result += ",\"default_safe\":true";
		}
		if (single_pk_cols.count(c[0]))
			result += ",\"single_column_primary_key\":true";
		if (dtype == "DECIMAL") {
			auto p = Trimmed(c[15]), sc = Trimmed(c[16]);
			if (valid_uint(p) && valid_uint(sc))
				result += ",\"precision\":" + p + ",\"scale\":" + sc;
		} else if (dtype == "TIMESTAMP") {
			auto sc = Trimmed(c[16]);
			if (valid_uint(sc))
				result += ",\"scale\":" + sc;
		} else if (dtype == "CHARACTER" || dtype == "VARCHAR" || dtype == "CLOB") {
			auto ml = Trimmed(c[15]), cp = Trimmed(c[17]), units = Trimmed(c[18]);
			if (valid_uint(ml))
				result += ",\"max_length\":" + ml;
			if (valid_uint(cp))
				result += ",\"codepage\":" + cp;
			if (!units.empty())
				result += ",\"string_units\":" + JSON(units);
		} else if (dtype == "BINARY" || dtype == "VARBINARY" || dtype == "BLOB") {
			auto ml = Trimmed(c[15]);
			if (valid_uint(ml))
				result += ",\"max_length\":" + ml;
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
	string source_error, returned_rows;
	int64_t affected = -1;
	bool sent = false, commit_started = false, local_rejection = false;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<Session> session;
	try {
		if (ctx.IsInterrupted())
			throw InterruptException();
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::DB2), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		if (Info(dbc, SQL_DBMS_NAME).find("DB2/") != 0)
			Reject();
		version = Info(dbc, SQL_DBMS_VER);
		if (!Compatible(version) || (d.execute && version != d.version))
			Reject();
		Writable(dbc);
		session = make_uniq<Session>(ctx, dbc);
		auto &s = *session;
		stage = "source_metadata";
		s.Exec("SET CURRENT LOCK TIMEOUT 5");
		s.Exec("LOCK TABLE " + Identifier(d.schema) + "." + Identifier(d.table) + " IN EXCLUSIVE MODE");
		std::unordered_set<string> column_names;
		descriptor = Metadata(s, d, stage, &column_names);
		if (d.execute) {
			stage = "source_descriptor";
			if (descriptor != d.expected || (d.returning && column_names.count(d.returning_name) != 1)) {
				error = "0A000";
				Reject();
			}
			stage = "source_execute";
			if (d.returning)
				affected = s.ExecuteReturning(d.sql, d.params, returned_rows, sent, stage);
			else
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
			source.product = "db2";
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
	out.SetValue(0, 0, Value(status));
	out.SetValue(1, 0, Value::BIGINT(affected));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value(version));
	out.SetValue(5, 0, Value(descriptor));
	idx_t out_col = 6;
	if (d.returning)
		out.SetValue(out_col++, 0, Value(status == "COMMITTED" ? returned_rows : ""));
	if (d.diagnostics)
		out.SetValue(out_col++, 0, Value(source_error));
	out.SetChildCardinality(1);
}
struct AdminData : TableFunctionData {
	ODBCCatalog &catalog;
	string sql;
	AdminData(ODBCCatalog &c, string q) : catalog(c), sql(std::move(q)) {
	}
};
static unique_ptr<FunctionData> AdminBind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                          vector<duckdb::Identifier> &names) {
	if (in.inputs[0].IsNull() || in.inputs[1].IsNull())
		Reject();
	auto &catalog = CatalogFor(ctx, in.inputs[0].GetValue<string>(), true);
	auto sql = in.inputs[1].GetValue<string>();
	if (catalog.access_mode != AccessMode::READ_WRITE || sql.empty() || sql.size() > 4 * 1024 * 1024)
		Reject();
	types = {LogicalType::VARCHAR};
	names = {"rows"};
	return make_uniq<AdminData>(catalog, std::move(sql));
}
static void AdminScan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<State>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<AdminData>();
	auto connection = ODBCConnection::Open(ODBCTypeConfig(ctx, d.catalog.GetDialect()), d.catalog.connection_string, 5);
	string rows = "[]";
	unique_ptr<Session> session;
	try {
		Writable(connection->GetConn());
		session = make_uniq<Session>(ctx, connection->GetConn());
		auto &s = *session;
		auto sql = StringUtil::Upper(Trimmed(d.sql));
		if (StringUtil::StartsWith(sql, "SELECT ") || StringUtil::StartsWith(sql, "VALUES "))
			rows = JSONRows(s.Rows(d.sql));
		else
			s.Exec(d.sql);
		s.Exec("COMMIT");
	} catch (...) {
		auto state = session && !session->error.empty() ? session->error : "08006";
		if (session) {
			try {
				session->Exec("ROLLBACK", true);
			} catch (...) {
			}
		}
		session.reset();
		connection->Close();
		throw InvalidInputException("guarded ODBC admin operation rejected (SQLSTATE %s)", state);
	}
	session.reset();
	connection->Close();
	out.SetValue(0, 0, Value(rows));
	out.SetChildCardinality(1);
}
} // namespace
void RegisterDb2GuardedCRUD(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("odbc_execute", vector<LogicalType>(2, LogicalType::VARCHAR), AdminScan, AdminBind, Init));
	TableFunction metadata("db2_crud_metadata", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("source_diagnostics", LogicalType::BOOLEAN); });
	loader.RegisterFunction(metadata);
	TableFunction exec("db2_guarded_mutation", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("params", LogicalType::ANY);
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
	});
	loader.RegisterFunction(exec);
	TableFunction exec_returning("db2_guarded_insert_returning", vector<LogicalType>(6, LogicalType::VARCHAR), Scan,
	                             BindReturning, Init);
	exec_returning.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("params", LogicalType::ANY);
		options.Add("returning_name", LogicalType::VARCHAR);
		options.Add("returning_oid", LogicalType::BIGINT);
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
	});
	loader.RegisterFunction(exec_returning);
}
} // namespace duckdb
