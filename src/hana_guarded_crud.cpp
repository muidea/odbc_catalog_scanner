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
#include <cstdint>
#include <mutex>
#include <thread>

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
static string Identifier(const string &s) {
	return ODBCUtils::WriteIdentifier(s);
}
static string Literal(const string &s) {
	return ODBCUtils::WriteLiteral(s);
}
class Session {
public:
	Session(ClientContext &ctx_p, SQLHDBC dbc) : ctx(ctx_p), stmt(nullptr, ODBCStatementDelete) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS)
			Reject();
		stmt = ODBCStatementPtr(h, ODBCStatementDelete);
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
			case 146:
			case 131:
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
		Check(SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS));
	}
	void Exec(const string &sql, bool cleanup = false) {
		Start(cleanup);
		auto rc = SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS);
		if (rc != SQL_NO_DATA)
			Check(rc);
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols != 0)
			Reject();
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
	}
	vector<vector<string>> Rows(const string &sql, bool cleanup = false) {
		Start(cleanup);
		Check(SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols <= 0 || cols > 64)
			Reject();
		vector<vector<string>> rows;
		while (true) {
			auto rc = SQLFetch(stmt.get());
			if (rc == SQL_NO_DATA)
				break;
			Check(rc);
			vector<string> row;
			for (SQLUSMALLINT col = 1; col <= cols; col++) {
				SQLCHAR buffer[4096] = {0};
				SQLLEN ind = 0;
				Check(SQLGetData(stmt.get(), col, SQL_C_CHAR, buffer, sizeof(buffer), &ind));
				if (ind == SQL_NULL_DATA || ind == SQL_NO_TOTAL || ind >= static_cast<SQLLEN>(sizeof(buffer)))
					Reject();
				row.emplace_back(reinterpret_cast<char *>(buffer));
			}
			rows.push_back(std::move(row));
			if (rows.size() > 2048)
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
	void BindInputParams(const vector<Value> &params, vector<int64_t> &values, vector<int16_t> &bools,
	                     vector<double> &doubles, vector<string> &text, vector<string> &blobs,
	                     vector<SQLLEN> &lengths) {
		static char empty_blob_byte = 0;
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			auto t = v.type().id();
			if (t == LogicalTypeId::BLOB) {
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
				} else {
					blobs[i] = StringValue::Get(v);
					if (blobs[i].size() > 4 * 1024 * 1024)
						Reject();
					lengths[i] = static_cast<SQLLEN>(blobs[i].size());
				}
				SQLSMALLINT sql_type = blobs[i].size() > 4000 ? SQL_LONGVARBINARY : SQL_VARBINARY;
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, sql_type,
				                       MaxValue<idx_t>(1, blobs[i].size()), 0,
				                       blobs[i].empty() ? &empty_blob_byte : const_cast<char *>(blobs[i].data()),
				                       blobs[i].size(), &lengths[i]));
				continue;
			}
			if (v.IsNull()) {
				lengths[i] = SQL_NULL_DATA;
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 19, 0,
				                       &values[i], sizeof(int64_t), &lengths[i]));
				continue;
			}
			if (t == LogicalTypeId::VARCHAR) {
				text[i] = v.GetValue<string>();
				if (text[i].size() > 65536 || text[i].find('\0') != string::npos)
					Reject();
				lengths[i] = text[i].size();
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
				                       MaxValue<idx_t>(1, text[i].size()), 0, const_cast<char *>(text[i].c_str()),
				                       text[i].size() + 1, &lengths[i]));
			} else if (t == LogicalTypeId::BOOLEAN) {
				bools[i] = v.GetValue<bool>() ? 1 : 0;
				lengths[i] = sizeof(int16_t);
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 5, 0,
				                       &bools[i], sizeof(int16_t), &lengths[i]));
			} else if (t == LogicalTypeId::FLOAT || t == LogicalTypeId::DOUBLE) {
				double dval = t == LogicalTypeId::FLOAT ? static_cast<double>(v.GetValue<float>())
				                                        : v.GetValue<double>();
				if (!std::isfinite(dval))
					Reject();
				doubles[i] = dval;
				lengths[i] = sizeof(double);
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 53, 0,
				                       &doubles[i], sizeof(double), &lengths[i]));
			} else if (t == LogicalTypeId::SMALLINT || t == LogicalTypeId::INTEGER || t == LogicalTypeId::BIGINT) {
				values[i] = v.GetValue<int64_t>();
				lengths[i] = sizeof(int64_t);
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 19, 0,
				                       &values[i], sizeof(int64_t), &lengths[i]));
			} else {
				Reject();
			}
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
		vector<int64_t> values(params.size(), 0);
		vector<int16_t> bools(params.size(), 0);
		vector<double> doubles(params.size(), 0.0);
		vector<string> text(params.size());
		vector<string> blobs(params.size());
		vector<SQLLEN> lengths(params.size(), 0);
		BindInputParams(params, values, bools, doubles, text, blobs, lengths);

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
	int64_t ExecuteReturning(const string &sql, const vector<Value> &params, bool &sent, string &stage,
	                         string &returned_rows) {
		idx_t pos = 0;
		while (pos < sql.size() && StringUtil::CharacterIsSpace(sql[pos]))
			pos++;
		auto prefix = StringUtil::Upper(sql.substr(pos, 6));
		if (prefix != "INSERT" || (pos + 6 < sql.size() && !StringUtil::CharacterIsSpace(sql[pos + 6]))) {
			error = "0A000";
			Reject();
		}
		int64_t affected = Execute(sql, params, sent, stage);
		if (affected != 1) {
			error = "0A000";
			Reject();
		}
		auto val = Scalar("SELECT CAST(CURRENT_IDENTITY_VALUE() AS VARCHAR(64)) FROM DUMMY");
		if (val.empty() || val.size() > 20 || val.find_first_not_of("0123456789-") != string::npos) {
			error = "0A000";
			Reject();
		}
		returned_rows = "[[" + JSON(val) + "]]";
		return 1;
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
static ODBCCatalog &CatalogFor(ClientContext &ctx, const string &name) {
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, duckdb::Identifier(name));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::HANA)
		Reject();
	return catalog;
}
static unique_ptr<FunctionData> Bind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                     vector<duckdb::Identifier> &names) {
	for (auto &v : in.inputs)
		if (v.IsNull())
			Reject();
	auto d = make_uniq<Data>(CatalogFor(ctx, in.inputs[0].GetValue<string>()), in.inputs.size() == 6);
	d->schema = in.inputs[1].GetValue<string>();
	d->table = in.inputs[2].GetValue<string>();
	if (!SimpleName(d->schema) || !SimpleName(d->table) ||
	    ODBCDialectUtil::IsInternalSchema(ODBCDialect::HANA, d->schema))
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
	d->returning = in.table_function.GetName().GetIdentifierName() == "hana_guarded_insert_returning";
	if (d->returning) {
		auto rname = in.named_parameters.find("returning_name"), roid = in.named_parameters.find("returning_oid");
		if (rname == in.named_parameters.end() || roid == in.named_parameters.end() || rname->second.IsNull() ||
		    roid->second.IsNull())
			Reject();
		d->returning_name = rname->second.GetValue<string>();
		d->returning_oid = roid->second.GetValue<int64_t>();
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
	    SQL_SUCCESS)
		Reject();
	if (SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0) != SQL_SUCCESS)
		Reject();
	SQLULEN autocommit = SQL_AUTOCOMMIT_ON;
	if (SQLGetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, &autocommit, sizeof(autocommit), nullptr) != SQL_SUCCESS ||
	    autocommit != SQL_AUTOCOMMIT_OFF)
		Reject();
}
static string Metadata(Session &s, const Data &d, string &stage, int64_t *returning_col_oid = nullptr) {
	const auto filter = "SCHEMA_NAME=" + Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.table);
	stage = "source_metadata";
	auto identity = s.Rows("SELECT DATABASE_NAME,VERSION FROM SYS.M_DATABASE");
	if (identity.size() != 1 || identity[0].size() != 2)
		Reject();
	stage = "source_table";
	auto table =
	    s.Rows("SELECT TABLE_OID,TABLE_TYPE,IS_TEMPORARY,IS_USER_DEFINED_TYPE,COALESCE(PARTITION_SPEC,'') "
	           "FROM SYS.TABLES WHERE " +
	           filter);
	if (table.size() != 1 || table[0][2] != "FALSE" || table[0][3] != "FALSE" || !table[0][4].empty())
		Reject();
	if (s.Scalar("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.TRIGGERS WHERE SUBJECT_TABLE_SCHEMA=" +
	             Literal(d.schema) + " AND SUBJECT_TABLE_NAME=" + Literal(d.table)) != "0")
		Reject();
	stage = "source_columns";
	auto cols =
	    s.Rows("SELECT COLUMN_NAME,DATA_TYPE_NAME,POSITION,IS_NULLABLE,COALESCE(CAST(LENGTH AS VARCHAR(20)),'0'),"
	           "COALESCE(CAST(SCALE AS VARCHAR(20)),'0'),COALESCE(DEFAULT_VALUE,''),COALESCE(GENERATION_TYPE,'') "
	           "FROM SYS.TABLE_COLUMNS WHERE " +
	           filter + " ORDER BY POSITION");
	if (cols.empty() || cols.size() > 1024)
		Reject();
	for (auto &c : cols) {
		const auto &ty = c[1];
		bool scalar = ty == "TINYINT" || ty == "SMALLINT" || ty == "INTEGER" || ty == "BIGINT" || ty == "DECIMAL" ||
		              ty == "SMALLDECIMAL" || ty == "REAL" || ty == "DOUBLE" || ty == "BOOLEAN" ||
		              ty == "VARCHAR" || ty == "NVARCHAR" || ty == "ALPHANUM" || ty == "SHORTTEXT" ||
		              ty == "CLOB" || ty == "NCLOB" || ty == "BLOB" || ty == "DATE" || ty == "TIME" ||
		              ty == "TIMESTAMP" || ty == "SECONDDATE" || ty == "VARBINARY" || ty == "BINARY";
		bool identity_gen = c[7] == "ALWAYS AS IDENTITY" || c[7] == "BY DEFAULT AS IDENTITY";
		if (!SimpleName(c[0]) || !scalar || (c[3] != "TRUE" && c[3] != "FALSE") || (!c[7].empty() && !identity_gen))
			Reject();
	}
	stage = "source_constraints";
	auto constraints =
	    s.Rows("SELECT CONSTRAINT_NAME,IS_PRIMARY_KEY,IS_UNIQUE_KEY,COALESCE(CHECK_CONDITION,'') "
	           "FROM SYS.CONSTRAINTS WHERE " +
	           filter + " ORDER BY CONSTRAINT_NAME");
	auto indexes =
	    s.Rows("SELECT INDEX_NAME,INDEX_TYPE,CONSTRAINT FROM SYS.INDEXES WHERE " + filter + " ORDER BY INDEX_NAME");
	string revision = JSONRows(identity) + JSONRows(table) + JSONRows(cols) + JSONRows(constraints) + JSONRows(indexes);
	string result = "{\"database\":" + JSON(identity[0][0]) + ",\"schema\":" + JSON(d.schema) +
	                ",\"table\":" + JSON(d.table) + ",\"object_id\":" + JSON(table[0][0]) +
	                ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	bool first = true;
	for (auto &c : cols) {
		if (!first)
			result += ",";
		first = false;
		auto upper_ty = StringUtil::Upper(c[1]);
		int64_t type_oid = 0;
		if (upper_ty == "SMALLINT")
			type_oid = 21;
		else if (upper_ty == "INTEGER" || upper_ty == "INT")
			type_oid = 23;
		else if (upper_ty == "BIGINT")
			type_oid = 20;
		if (returning_col_oid && c[0] == d.returning_name)
			*returning_col_oid = type_oid;
		bool identity_gen = c[7] == "ALWAYS AS IDENTITY" || c[7] == "BY DEFAULT AS IDENTITY";
		result += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(c[1]) +
		          ",\"nullable\":" + (c[3] == "TRUE" ? "true" : "false") +
		          ",\"has_default\":" + (!c[6].empty() ? "true" : "false");
		if (identity_gen)
			result += ",\"identity\":true";
		if (type_oid != 0)
			result += ",\"type_oid\":" + std::to_string(type_oid);
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
	int64_t ret_oid = 0;
	bool sent = false, commit_started = false, local_rejection = false;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<Session> session;
	try {
		if (ctx.IsInterrupted())
			throw InterruptException();
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		auto dbms = StringUtil::Lower(Info(dbc, SQL_DBMS_NAME));
		if (dbms.find("hdb") == string::npos && dbms.find("hana") == string::npos)
			Reject();
		Writable(dbc);
		session = make_uniq<Session>(ctx, dbc);
		auto &s = *session;
		version = s.Scalar("SELECT VERSION FROM SYS.M_DATABASE");
		if (!Compatible(version) || (d.execute && version != d.version))
			Reject();
		if (s.Scalar("SELECT CAST(CURRENT_UPDATE_TRANSACTION() AS VARCHAR(32)) FROM DUMMY") != "0")
			Reject();
		stage = "source_metadata";
		s.Exec("LOCK TABLE " + Identifier(d.schema) + "." + Identifier(d.table) + " IN EXCLUSIVE MODE");
		descriptor = Metadata(s, d, stage, d.returning ? &ret_oid : nullptr);
		if (d.execute) {
			stage = "source_descriptor";
			if (descriptor != d.expected) {
				error = "0A000";
				Reject();
			}
			if (d.returning && (ret_oid == 0 || ret_oid != d.returning_oid)) {
				error = "0A000";
				Reject();
			}
			stage = "source_execute";
			affected = d.returning ? s.ExecuteReturning(d.sql, d.params, sent, stage, returned_rows)
			                       : s.Execute(d.sql, d.params, sent, stage);
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
		if (session && source_error.empty())
			source_error = session->source.JSON();
		if (error.empty())
			error = session && !session->error.empty()
			            ? session->error
			            : (ctx.IsInterrupted() ? "57014" : (local_rejection ? "0A000" : "XX000"));
		if (session) {
			try {
				session->Exec("ROLLBACK", true);
			} catch (...) {
			}
		}
		if (sent && !commit_started)
			status = "CONFIRMED_ROLLED_BACK";
		else if (commit_started)
			status = "COMMIT_STATUS_UNKNOWN";
	}
	if (connection) {
		session.reset();
		connection->Close();
	}
	out.SetValue(0, 0, Value(status));
	out.SetValue(1, 0, Value::BIGINT(affected));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value(version));
	out.SetValue(5, 0, Value(descriptor));
	if (d.returning)
		out.SetValue(6, 0, Value(status == "COMMITTED" ? returned_rows : ""));
	if (d.diagnostics)
		out.SetValue(6 + (d.returning ? 1 : 0), 0, Value(source_error));
	out.SetChildCardinality(1);
}
} // namespace

void RegisterHanaGuardedCRUD(ExtensionLoader &loader) {
	TableFunction meta("hana_crud_metadata",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, Scan, Bind, Init);
	meta.GetSignature().WithTypedKwargs("options",
	                                    [](TypedKwargs &options) { options.Add("source_diagnostics", LogicalType::BOOLEAN); });
	loader.RegisterFunction(meta);
	TableFunction exec("hana_guarded_mutation",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                    LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   Scan, Bind, Init);
	exec.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
		options.Add("params", LogicalType::ANY);
	});
	loader.RegisterFunction(exec);
	TableFunction ret("hana_guarded_insert_returning",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  Scan, Bind, Init);
	ret.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
		options.Add("params", LogicalType::ANY);
		options.Add("returning_name", LogicalType::VARCHAR);
		options.Add("returning_oid", LogicalType::BIGINT);
	});
	loader.RegisterFunction(ret);
}
} // namespace duckdb
