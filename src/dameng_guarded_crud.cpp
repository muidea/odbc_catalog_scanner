//===----------------------------------------------------------------------===//
//                         DuckDB
//
// dameng_guarded_crud.cpp
//
// Restricted Dameng mutations on an isolated scanner-owned session.
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
#include <condition_variable>
#include <mutex>
#include <thread>
namespace duckdb {
namespace {
static void Reject() {
	throw GuardedODBCLocalRejection("guarded Dameng operation rejected");
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
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "dameng");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLCHAR message[1] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, message, sizeof(message), &len);

		if (cancelled.load() || ctx.IsInterrupted())
			error = "57014";
		else if (native == -6602)
			error = "23505";
		else if (native == -6609)
			error = "23502";
		else
			error = state[0] ? string(reinterpret_cast<char *>(state)) : "XX000";
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
		vector<SQLLEN> lengths(params.size(), 0);
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			if (v.IsNull())
				lengths[i] = SQL_NULL_DATA;
			else {
				auto t = v.type().id();
				if (t != LogicalTypeId::SMALLINT && t != LogicalTypeId::INTEGER && t != LogicalTypeId::BIGINT)
					Reject();
				values[i] = v.GetValue<int64_t>();
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
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, duckdb::Identifier(name));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::DAMENG)
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
	    ODBCDialectUtil::IsInternalSchema(ODBCDialect::DAMENG, d->schema))
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
static string Trimmed(string v) {
	StringUtil::Trim(v);
	return v;
}
static string Metadata(Session &s, const Data &d, string &stage) {
	const auto filter = "OWNER=" + Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.table);
	stage = "source_metadata";
	auto identity = s.Rows("SELECT NAME,USER FROM V$DATABASE");
	if (identity.size() != 1 || identity[0].size() != 2 || identity[0][0].empty() ||
	    (identity[0][1] != d.schema && identity[0][1] != "SYSDBA"))
		Reject();
	auto modes = s.Rows("SELECT PARA_NAME,PARA_VALUE FROM V$DM_INI WHERE PARA_NAME='COMPATIBLE_MODE'");
	if (modes.size() != 1 || modes[0][1] != "0")
		Reject();
	stage = "source_table";
	auto table =
	    s.Rows("SELECT COALESCE(STATUS,'_'),COALESCE(PARTITIONED,'_'),COALESCE(TEMPORARY,'_'),"
	           "COALESCE(NESTED,'_'),COALESCE(SECONDARY,'_'),COALESCE(IOT_TYPE,'_'),COALESCE(CLUSTER_NAME,'_'),"
	           "COALESCE(READ_ONLY,'_'),COALESCE(COMPRESSION,'_'),COALESCE(DROPPED,'_') FROM DBA_TABLES WHERE " +
	           filter);
	if (table.size() != 1 ||
	    table[0] != vector<string> {"VALID", "NO", "N", "_", "_", "IOT", "_", "_", "DISABLED", "_"})
		Reject();
	auto object = s.Rows("SELECT O.ID,O.SCHID,O.TYPE$,O.SUBTYPE$,COALESCE(O.VERSION,-1),"
	                     "TO_CHAR(O.CRTDATE,'YYYY-MM-DD HH24:MI:SS'),COALESCE(O.INFO1,-1),COALESCE(O.INFO2,-1),"
	                     "COALESCE(O.INFO3,-1),COALESCE(O.INFO4,-1),COALESCE(O.INFO5,'_'),COALESCE(O.INFO6,'_'),"
	                     "COALESCE(O.INFO7,-1),COALESCE(O.INFO8,'_'),O.VALID FROM SYS.SYSOBJECTS O JOIN DBA_OBJECTS D "
	                     "ON O.ID=D.OBJECT_ID WHERE D.OWNER=" +
	                     Literal(d.schema) + " AND D.OBJECT_NAME=" + Literal(d.table) + " AND D.OBJECT_TYPE='TABLE'");
	if (object.size() != 1 || object[0][2] != "SCHOBJ" || object[0][3] != "UTAB" || object[0][6] != "2097152" ||
	    object[0][14] != "Y")
		Reject();
	stage = "source_columns";
	auto cols =
	    s.Rows("SELECT COLUMN_NAME,DATA_TYPE,COALESCE(DATA_TYPE_OWNER,'_'),DATA_LENGTH,"
	           "COALESCE(DATA_SCALE,-1),NULLABLE,COLUMN_ID,COALESCE(DEFAULT_LENGTH,0),COALESCE(DATA_DEFAULT,'_'),"
	           "COALESCE(HIDDEN_COLUMN,'_'),COALESCE(VIRTUAL_COLUMN,'_') FROM DBA_TAB_COLS WHERE " +
	           filter + " ORDER BY COLUMN_ID");
	auto raw = s.Rows("SELECT NAME,COLID,TYPE$,LENGTH$,SCALE,NULLABLE$,COALESCE(DEFVAL,'_'),INFO1,INFO2 "
	                  "FROM SYS.SYSCOLUMNS WHERE ID=" +
	                  object[0][0] + " ORDER BY COLID");
	if (cols.empty() || cols.size() > 1024 || cols.size() != raw.size())
		Reject();
	for (idx_t i = 0; i < cols.size(); i++) {
		auto &c = cols[i];
		auto &r = raw[i];
		string length =
		    c[1] == "SMALLINT" ? "2" : ((c[1] == "INT" || c[1] == "INTEGER") ? "4" : (c[1] == "BIGINT" ? "8" : ""));
		if (!SimpleName(c[0]) || length.empty() || c[2] != "_" || c[3] != length || c[4] != "0" ||
		    (c[5] != "Y" && c[5] != "N") || c[6] != std::to_string(i + 1) || c[7] != "0" || c[8] != "_" ||
		    c[9] != "NO" || c[10] != "NO" || r[0] != c[0] || r[1] != std::to_string(i) || r[2] != c[1] ||
		    r[3] != length || r[4] != "0" || r[5] != c[5] || r[6] != "_" || r[7] != "0" || r[8] != "0")
			Reject();
	}
	stage = "source_constraints";
	auto constraints = s.Rows("SELECT CONSTRAINT_NAME,CONSTRAINT_TYPE,STATUS,DEFERRABLE,DEFERRED,VALIDATED,"
	                          "COALESCE(SEARCH_CONDITION,'_') FROM DBA_CONSTRAINTS WHERE " +
	                          filter + " ORDER BY CONSTRAINT_NAME");
	for (auto &c : constraints)
		if ((c[1] != "P" && c[1] != "U") || c[2] != "ENABLED" || c[3] != "NOT DEFERRABLE" || c[4] != "IMMEDIATE" ||
		    c[5] != "VALIDATED" || c[6] != "_")
			Reject();
	auto constraint_cols = s.Rows("SELECT CONSTRAINT_NAME,COLUMN_NAME,POSITION FROM DBA_CONS_COLUMNS WHERE " + filter +
	                              " ORDER BY CONSTRAINT_NAME,POSITION");
	if (s.Scalar("SELECT COUNT(*) FROM DBA_CONSTRAINTS WHERE CONSTRAINT_TYPE='R' AND R_OWNER=" + Literal(d.schema) +
	             " AND R_CONSTRAINT_NAME IN (SELECT CONSTRAINT_NAME FROM DBA_CONSTRAINTS WHERE " + filter + ")") !=
	        "0" ||
	    s.Scalar("SELECT COUNT(*) FROM DBA_TRIGGERS WHERE TABLE_OWNER=" + Literal(d.schema) +
	             " AND TABLE_NAME=" + Literal(d.table)) != "0" ||
	    s.Scalar("SELECT COUNT(*) FROM DBA_POLICIES WHERE OBJECT_OWNER=" + Literal(d.schema) +
	             " AND OBJECT_NAME=" + Literal(d.table)) != "0")
		Reject();
	stage = "source_indexes";
	const auto index_filter = "TABLE_OWNER=" + Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.table);
	auto indexes = s.Rows("SELECT OWNER,INDEX_NAME,INDEX_TYPE,STATUS,UNIQUENESS,PARTITIONED,TEMPORARY,"
	                      "GENERATED,COALESCE(FUNCIDX_STATUS,'_') FROM DBA_INDEXES WHERE " +
	                      index_filter + " ORDER BY OWNER,INDEX_NAME");
	for (auto &i : indexes)
		if (i[0] != d.schema || (i[2] != "NORMAL" && i[2] != "CLUSTER") || i[3] != "VALID" ||
		    (i[4] != "UNIQUE" && i[4] != "NONUNIQUE") || i[5] != "NO" || i[6] != "N" || i[8] != "_")
			Reject();
	stage = "source_index_columns";
	auto index_cols = s.Rows(
	    "SELECT INDEX_OWNER,INDEX_NAME,COLUMN_NAME,COLUMN_POSITION,COALESCE(DESCEND,'_') FROM DBA_IND_COLUMNS WHERE " +
	    index_filter + " ORDER BY INDEX_OWNER,INDEX_NAME,COLUMN_POSITION");
	stage = "source_index_objects";
	auto index_objects =
	    s.Rows("SELECT OWNER,OBJECT_NAME,OBJECT_ID,COALESCE(TO_CHAR(CREATED,'YYYY-MM-DD HH24:MI:SS'),'_'),"
	           "COALESCE(TO_CHAR(LAST_DDL_TIME,'YYYY-MM-DD HH24:MI:SS'),'_'),STATUS FROM DBA_OBJECTS WHERE "
	           "OBJECT_TYPE='INDEX' AND OWNER=" +
	           Literal(d.schema) + " AND OBJECT_NAME IN (SELECT INDEX_NAME FROM DBA_INDEXES WHERE " + index_filter +
	           ") ORDER BY OBJECT_NAME");
	string revision = JSONRows(identity) + JSONRows(modes) + JSONRows(table) + JSONRows(object) + JSONRows(cols) +
	                  JSONRows(raw) + JSONRows(constraints) + JSONRows(constraint_cols) + JSONRows(indexes) +
	                  JSONRows(index_cols) + JSONRows(index_objects);
	string result = "{\"database\":" + JSON(identity[0][0]) + ",\"schema\":" + JSON(d.schema) +
	                ",\"table\":" + JSON(d.table) +
	                ",\"object_id\":" + JSON(object[0][0] + ":" + object[0][1] + ":" + object[0][5]) +
	                ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	bool first = true;
	for (auto &c : cols) {
		if (!first)
			result += ",";
		first = false;
		result += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(c[1]) +
		          ",\"nullable\":" + (c[5] == "Y" ? "true" : "false") + "}";
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
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		version = Info(dbc, SQL_DBMS_VER);
		if (Info(dbc, SQL_DBMS_NAME) != "DM DATABASE MANAGEMENT SYSTEM")
			Reject();
		version = Info(dbc, SQL_DBMS_VER);
		if (!Compatible(version) || (d.execute && version != d.version))
			Reject();
		Writable(dbc);
		session = make_uniq<Session>(ctx, dbc);
		auto &s = *session;
		stage = "source_metadata";
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
			source.product = "dameng";
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
	if (d.diagnostics)
		out.SetValue(6, 0, Value(source_error));
	out.SetChildCardinality(1);
}
} // namespace
void RegisterDamengGuardedCRUD(ExtensionLoader &loader) {
	TableFunction metadata("dameng_crud_metadata", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("source_diagnostics", LogicalType::BOOLEAN); });
	loader.RegisterFunction(metadata);
	TableFunction exec("dameng_guarded_mutation", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("params", LogicalType::ANY);
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
	});
	loader.RegisterFunction(exec);
}
} // namespace duckdb
