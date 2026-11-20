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
#include <condition_variable>
#include <mutex>
#include <thread>
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
		if (cancelled.load() || ctx.IsInterrupted())
			error = "57014";
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
		vector<string> text(params.size());
		vector<SQLLEN> lengths(params.size(), 0);
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			if (!v.IsNull() && v.type().id() == LogicalTypeId::VARCHAR) {
				// Exact DECIMAL payload; source CAST owns scale/range semantics.
				text[i] = v.GetValue<string>();
				if (text[i].empty() || text[i].size() > 128 ||
				    text[i].find_first_not_of("+-0123456789.") != string::npos)
					Reject();
				lengths[i] = text[i].size();
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, text[i].size(), 0,
				                       const_cast<char *>(text[i].c_str()), text[i].size() + 1, &lengths[i]));
				continue;
			}
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
static ODBCCatalog &CatalogFor(ClientContext &ctx, const string &name, bool admin = false) {
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, duckdb::Identifier(name));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::DB2 && !(admin && catalog.GetDialect() == ODBCDialect::DAMENG))
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
	auto cols =
	    s.Rows("SELECT COLNAME,TYPESCHEMA,TYPENAME,CHAR(COLNO),NULLS,COALESCE(DEFAULT,''),HIDDEN,IDENTITY,GENERATED,"
	           "COALESCE(TEXT,''),ROWCHANGETIMESTAMP,ROWBEGIN,ROWEND,TRANSACTIONSTARTID,COALESCE(SECLABELNAME,''),"
	           "CHAR(LENGTH),CHAR(SCALE) FROM SYSCAT.COLUMNS WHERE " +
	           filter + " ORDER BY COLNO");
	if (cols.empty() || cols.size() > 1024)
		Reject();
	idx_t ordinal = 0;
	for (auto &c : cols) {
		if (!SimpleName(c[0]) || Trimmed(c[1]) != "SYSIBM" ||
		    (c[2] != "SMALLINT" && c[2] != "INTEGER" && c[2] != "BIGINT" && c[2] != "DECIMAL") ||
		    Trimmed(c[3]) != std::to_string(ordinal++) || (c[4] != "Y" && c[4] != "N") || !c[5].empty() ||
		    c[6] != " " || c[7] != "N" || c[8] != " " || !c[9].empty() || c[10] != "N" || c[11] != "N" ||
		    c[12] != "N" || c[13] != "N" || !c[14].empty())
			Reject();
		if (c[2] == "DECIMAL") {
			auto precision = std::stoi(Trimmed(c[15])), scale = std::stoi(Trimmed(c[16]));
			if (precision < 1 || precision > 31 || scale < 0 || scale > precision)
				Reject();
		}
	}
	stage = "source_constraints";
	auto constraints = s.Rows("SELECT CONSTNAME,TYPE,ENFORCED,TRUSTED,CHECKEXISTINGDATA FROM SYSCAT.TABCONST WHERE " +
	                          filter + " ORDER BY CONSTNAME");
	for (auto &c : constraints)
		if ((c[1] != "P" && c[1] != "U") || c[2] != "Y" || c[3] != " " || c[4] != "I")
			Reject();
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
	string revision = JSONRows(identity) + JSONRows(table) + JSONRows(cols) + JSONRows(constraints) + JSONRows(indexes);
	string result = "{\"database\":" + JSON(identity[0][0]) + ",\"schema\":" + JSON(d.schema) +
	                ",\"table\":" + JSON(d.table) + ",\"object_id\":" + JSON(t[3] + ":" + t[4] + ":" + t[5]) +
	                ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	bool first = true;
	for (auto &c : cols) {
		if (!first)
			result += ",";
		first = false;
		result += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(c[2]) +
		          ",\"nullable\":" + (c[4] == "Y" ? "true" : "false");
		if (c[2] == "DECIMAL")
			result += ",\"precision\":" + Trimmed(c[15]) + ",\"scale\":" + Trimmed(c[16]);
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
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx), d.catalog.connection_string, 5);
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
	auto connection = ODBCConnection::Open(ODBCTypeConfig(ctx), d.catalog.connection_string, 5);
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
	out.SetCardinality(1);
	out.SetValue(0, 0, Value(rows));
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
}
} // namespace duckdb
