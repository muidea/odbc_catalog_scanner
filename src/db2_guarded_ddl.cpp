//===----------------------------------------------------------------------===//
//                         DuckDB
//
// db2_guarded_ddl.cpp
//
// Guarded IBM Db2 capability observation, catalog description (including
// REORG_PENDING inspection), and transactional core-subset DDL execution.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "guarded_odbc_source_error.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "storage/odbc_catalog.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace duckdb {
namespace {
class DDLRejectException : public std::runtime_error {
public:
	string sqlstate;
	string stage;
	DDLRejectException(string state_p, string stage_p, const string &msg)
	    : std::runtime_error(msg), sqlstate(std::move(state_p)), stage(std::move(stage_p)) {
	}
};

[[noreturn]] static void RejectDDL(const string &sqlstate = "0A000", const string &stage = "source_identity",
                                   const string &msg = "guarded Db2 DDL rejected") {
	throw DDLRejectException(sqlstate, stage, msg);
}

[[noreturn]] static void RejectLocal() {
	throw GuardedODBCLocalRejection("guarded Db2 DDL input invalid");
}

static string JSON(const string &s) {
	const char *hex = "0123456789abcdef";
	string out = "\"";
	for (unsigned char c : s) {
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
			if (c < 32) {
				out += "\\u00";
				out += hex[c >> 4];
				out += hex[c & 15];
			} else {
				out += static_cast<char>(c);
			}
		}
	}
	return out + "\"";
}

static bool ValidName(const string &s) {
	return !s.empty() && s.size() <= 63 && s.find('\0') == string::npos;
}

static bool ValidUUID(const string &s) {
	if (s.size() != 36) {
		return false;
	}
	for (idx_t i = 0; i < s.size(); i++) {
		char c = s[i];
		if (i == 8 || i == 13 || i == 18 || i == 23) {
			if (c != '-') {
				return false;
			}
		} else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
			return false;
		}
	}
	return true;
}

static string Identifier(const string &s) {
	if (s.find('\0') != string::npos) {
		RejectLocal();
	}
	return ODBCUtils::WriteIdentifier(s);
}

static string Literal(const string &s) {
	if (s.find('\0') != string::npos) {
		RejectLocal();
	}
	return ODBCUtils::WriteLiteral(s);
}

static string TextVal(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::VARCHAR) {
		RejectLocal();
	}
	auto s = v.GetValue<string>();
	if (s.find('\0') != string::npos) {
		RejectLocal();
	}
	return s;
}

static int32_t IntVal(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::INTEGER) {
		RejectLocal();
	}
	return v.GetValue<int32_t>();
}

static bool BoolVal(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::BOOLEAN) {
		RejectLocal();
	}
	return v.GetValue<bool>();
}

static bool IsSignedInt64Literal(const string &s) {
	if (s.empty() || s == "-") {
		return false;
	}
	for (idx_t i = 0; i < s.size(); i++) {
		char c = s[i];
		if (!((c >= '0' && c <= '9') || (i == 0 && c == '-'))) {
			return false;
		}
	}
	try {
		size_t pos = 0;
		long long val = std::stoll(s, &pos, 10);
		return pos == s.size() && std::to_string(val) == s;
	} catch (...) {
		return false;
	}
}

static string Trimmed(string s) {
	StringUtil::Trim(s);
	return s;
}

static string ValidSQLState(const string &candidate, const string &fallback) {
	if (candidate.size() == 5) {
		bool ok = true;
		for (char c : candidate) {
			if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z'))) {
				ok = false;
				break;
			}
		}
		if (ok) {
			return candidate;
		}
	}
	return fallback;
}

static bool IsDb2LockTimeout(SQLINTEGER native, const char *odbc_state, const string &message) {
	if (native == -913) {
		return true;
	}
	if (native == -911) {
		auto lower = StringUtil::Lower(message);
		if (StringUtil::Contains(lower, "reason code \"68\"") || StringUtil::Contains(lower, "reason code 68") ||
		    StringUtil::Contains(lower, "timeout")) {
			return true;
		}
		// Without deadlock reason "2", treat SQL0911N conservatively as lock timeout.
		return !StringUtil::Contains(lower, "reason code \"2\"") && !StringUtil::Contains(lower, "deadlock");
	}
	return odbc_state && string(odbc_state) == "57033";
}

static string MapDb2DDLSQLState(SQLINTEGER native, const char *odbc_state, const string &message) {
	if (IsDb2LockTimeout(native, odbc_state, message)) {
		return "55P03";
	}
	if (native == -911) {
		return "40P01";
	}
	switch (native) {
	case -601:
		return "42P07";
	case -204:
		if (odbc_state && string(odbc_state) == "42S02") {
			return "42P01";
		}
		return "42704";
	case -205:
	case -206:
		return "42703";
	case -612:
		return "42701";
	case -542:
		return "42831";
	case -573:
		return "42830";
	case -624:
		return "42P16";
	case -803:
		return "23505";
	case -530:
	case -531:
	case -532:
		return "23503";
	case -544:
	case -545:
		return "23514";
	case -407:
	case -193:
		return "23502";
	case -551:
	case -552:
		return "42501";
	case -668:
		return "57007";
	case -669:
	case -750:
	case -20356:
		return "2BP01";
	case -428:
		return "25001";
	default:
		break;
	}
	if (odbc_state) {
		return ValidSQLState(odbc_state, "HY000");
	}
	return "HY000";
}

// Guaranteed not to have committed a TRUNCATE ... IMMEDIATE when these errors fire.
static bool IsDb2TruncateConfirmedNotExecuted(SQLINTEGER native, const char *odbc_state, const string &message) {
	if (IsDb2LockTimeout(native, odbc_state, message)) {
		return true;
	}
	switch (native) {
	case -20356:
	case -668:
	case -428:
	case -551:
	case -552:
	case -204:
		return true;
	default:
		return false;
	}
}

// Never configure SQL_ATTR_QUERY_TIMEOUT: unixODBC / vendor drivers may install
// process-wide SIGALRM handlers. Cancel via a session-scoped SQLCancelHandle watcher.
class Db2DDLSession {
public:
	Db2DDLSession(ClientContext &ctx_p, SQLHDBC dbc_p, uint32_t stmt_timeout_sec = 15)
	    : ctx(ctx_p), dbc(dbc_p), stmt(nullptr, ODBCStatementDelete), timeout_sec(stmt_timeout_sec) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS) {
			RejectDDL("08006", "source_identity", "failed to allocate Db2 statement");
		}
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
	~Db2DDLSession() {
		{
			std::lock_guard<std::mutex> lock(mu);
			stopped = true;
		}
		cv.notify_all();
		if (watcher.joinable()) {
			watcher.join();
		}
	}

	string error;
	SQLINTEGER last_native = 0;
	string last_odbc_state;
	string last_message;
	int64_t last_warning_count = 0;
	GuardedODBCSourceError source;
	string current_stage = "source_identity";

	void Check(SQLRETURN rc) {
		if (rc == SQL_SUCCESS) {
			return;
		}
		if (rc == SQL_SUCCESS_WITH_INFO) {
			for (SQLSMALLINT rec = 1; rec <= 32; rec++) {
				SQLCHAR state[6] = {0};
				SQLINTEGER native = 0;
				SQLSMALLINT len = 0;
				if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), rec, state, &native, nullptr, 0, &len) == SQL_NO_DATA) {
					break;
				}
				last_warning_count++;
			}
			return;
		}
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "db2");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLCHAR msg[1024] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, msg, sizeof(msg), &len);
		last_native = native;
		last_odbc_state = state[0] ? string(reinterpret_cast<char *>(state)) : "";
		last_message = !source.message.empty() ? source.message : string(reinterpret_cast<char *>(msg));
		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
		} else {
			error = MapDb2DDLSQLState(native, reinterpret_cast<const char *>(state), last_message);
		}
		RejectDDL(error, current_stage, last_message.empty() ? "Db2 DDL error" : last_message);
	}

	void Start(bool cleanup = false) {
		if (!cleanup && (cancelled.load() || ctx.IsInterrupted())) {
			RejectDDL("57014", current_stage, "operation cancelled");
		}
		{
			std::lock_guard<std::mutex> lock(mu);
			cleanup_mode = cleanup;
			cancel_sent = false;
			statement_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
		}
		cv.notify_all();
		auto rc1 = SQLFreeStmt(stmt.get(), SQL_CLOSE);
		if (!cleanup) {
			Check(rc1);
		}
		auto rc2 = SQLFreeStmt(stmt.get(), SQL_UNBIND);
		if (!cleanup) {
			Check(rc2);
		}
		auto rc3 = SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS);
		if (!cleanup) {
			Check(rc3);
		}
	}

	void Exec(const string &sql, bool cleanup = false) {
		Start(cleanup);
		auto rc = SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS);
		if (cleanup) {
			return;
		}
		if (rc == SQL_NO_DATA) {
			return;
		}
		Check(rc);
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
		}
	}

	bool RollbackQuiet() {
		Start(true);
		auto rc = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
		return SQL_SUCCEEDED(rc);
	}

	vector<vector<string>> Rows(const string &sql, bool allow_null = true) {
		Start(false);
		Check(SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols < 1 || cols > 64) {
			RejectDDL("XX000", current_stage, "unexpected column count in Db2 query");
		}
		vector<vector<string>> rows;
		idx_t total_bytes = 0;
		SQLRETURN rc;
		while ((rc = SQLFetch(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			if (rows.size() >= 8192) {
				RejectDDL("54000", current_stage, "Db2 metadata row bound exceeded");
			}
			vector<string> row;
			for (SQLUSMALLINT col = 1; col <= cols; col++) {
				char buf[8192] = {0};
				SQLLEN ind = 0;
				auto get_rc = SQLGetData(stmt.get(), col, SQL_C_CHAR, buf, sizeof(buf), &ind);
				if (get_rc != SQL_SUCCESS) {
					Check(get_rc);
				}
				if (ind == SQL_NULL_DATA) {
					if (!allow_null) {
						RejectDDL("XX000", current_stage, "unexpected NULL Db2 metadata value");
					}
					row.emplace_back("");
				} else if (ind < 0 || static_cast<idx_t>(ind) >= sizeof(buf)) {
					RejectDDL("54000", current_stage, "Db2 metadata cell bound exceeded");
				} else {
					row.emplace_back(buf, static_cast<size_t>(ind));
				}
				total_bytes += row.back().size();
				if (total_bytes > 1024 * 1024) {
					RejectDDL("54000", current_stage, "Db2 metadata document bound exceeded");
				}
			}
			rows.push_back(std::move(row));
		}
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
		}
		return rows;
	}

	string Scalar(const string &sql) {
		auto r = Rows(sql, true);
		if (r.size() != 1 || r[0].size() != 1) {
			RejectDDL("XX000", current_stage, "expected scalar Db2 result");
		}
		return r[0][0];
	}

private:
	ClientContext &ctx;
	SQLHDBC dbc;
	ODBCStatementPtr stmt;
	std::mutex mu;
	std::condition_variable cv;
	std::thread watcher;
	uint32_t timeout_sec = 15;
	bool stopped = false, cleanup_mode = false, cancel_sent = true;
	std::chrono::steady_clock::time_point statement_deadline = std::chrono::steady_clock::now();
	std::atomic<bool> cancelled {false};
};

struct Db2ServerFacts {
	string version;
	string database_name;
	string instance_name;
	string host_name;
	string schema_id;
	string session_user;
	string current_schema;
	bool read_only = false;
};

static Db2ServerFacts QueryDb2Facts(Db2DDLSession &s, SQLHDBC dbc, const string &schema) {
	SQLCHAR dbms[128] = {0};
	SQLSMALLINT dbms_len = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(dbc, SQL_DBMS_NAME, dbms, sizeof(dbms), &dbms_len)) ||
	    string(reinterpret_cast<char *>(dbms)).find("DB2/") != 0) {
		RejectDDL("0A000", "source_identity", "connected datasource is not IBM Db2");
	}
	SQLCHAR ver[256] = {0};
	SQLSMALLINT ver_len = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(dbc, SQL_DBMS_VER, ver, sizeof(ver), &ver_len)) || ver_len <= 0 ||
	    ver_len >= static_cast<SQLSMALLINT>(sizeof(ver))) {
		RejectDDL("0A000", "source_identity", "Db2 version identity unavailable");
	}
	auto id_rows = s.Rows("SELECT RTRIM(CURRENT SERVER), RTRIM(CURRENT USER), RTRIM(CURRENT SCHEMA) "
	                      "FROM SYSIBM.SYSDUMMY1");
	if (id_rows.size() != 1 || id_rows[0].size() != 3) {
		RejectDDL("0A000", "source_identity", "Db2 database identity query failed");
	}
	auto inst_rows = s.Rows("SELECT RTRIM(INST_NAME), RTRIM(SERVICE_LEVEL) FROM SYSIBMADM.ENV_INST_INFO");
	if (inst_rows.size() != 1 || inst_rows[0].size() != 2) {
		RejectDDL("0A000", "source_identity", "Db2 instance identity query failed");
	}
	auto sys_rows = s.Rows("SELECT RTRIM(HOST_NAME) FROM SYSIBMADM.ENV_SYS_INFO");
	if (sys_rows.size() != 1 || sys_rows[0].size() != 1) {
		RejectDDL("0A000", "source_identity", "Db2 host identity query failed");
	}
	auto hadr_rows = s.Rows("SELECT COALESCE(VALUE,'') FROM SYSIBMADM.DBCFG WHERE NAME='hadr_db_role'");
	bool read_only = false;
	if (!hadr_rows.empty()) {
		auto role = StringUtil::Upper(Trimmed(hadr_rows[0][0]));
		if (role == "STANDBY") {
			read_only = true;
		}
	}
	auto sch_rows = s.Rows("SELECT RTRIM(SCHEMANAME), RTRIM(CHAR(CREATE_TIME)) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME=" +
	                       Literal(schema));

	Db2ServerFacts f;
	f.database_name = Trimmed(id_rows[0][0]);
	f.session_user = Trimmed(id_rows[0][1]);
	f.current_schema = Trimmed(id_rows[0][2]);
	f.instance_name = Trimmed(inst_rows[0][0]);
	f.version = Trimmed(inst_rows[0][1]);
	if (f.version.empty()) {
		f.version = Trimmed(string(reinterpret_cast<char *>(ver), static_cast<size_t>(ver_len)));
	}
	f.host_name = Trimmed(sys_rows[0][0]);
	f.read_only = read_only;
	if (sch_rows.size() == 1) {
		f.schema_id = Trimmed(sch_rows[0][0]) + ":" + Trimmed(sch_rows[0][1]);
	}
	if (f.version.empty() || f.database_name.empty() || f.instance_name.empty() || f.host_name.empty()) {
		RejectDDL("0A000", "source_identity", "Db2 database/instance identity unavailable");
	}
	return f;
}

// ---------- Observation table functions ----------

struct Db2DDLMetaData : TableFunctionData {
	ODBCCatalog &catalog;
	string schema;
	string kind;
	string name;
	bool describe = false;
	Db2DDLMetaData(ODBCCatalog &c, bool d) : catalog(c), describe(d) {
	}
};

struct Db2DDLState : GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> DDLInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<Db2DDLState>();
}

static ODBCCatalog &ResolveDb2Catalog(ClientContext &ctx, const string &db_name) {
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, db_name);
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog") {
		throw BinderException("DDL catalog is unavailable");
	}
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::DB2) {
		throw BinderException("DDL catalog is unavailable");
	}
	return catalog;
}

static unique_ptr<FunctionData> DDLMetadataBind(ClientContext &ctx, TableFunctionBindInput &in,
                                                vector<LogicalType> &types, vector<string> &names) {
	for (auto &v : in.inputs) {
		if (v.IsNull()) {
			RejectLocal();
		}
	}
	auto db_name = TextVal(in.inputs[0]);
	auto &catalog = ResolveDb2Catalog(ctx, db_name);
	bool describe = in.inputs.size() == 4;
	auto d = make_uniq<Db2DDLMetaData>(catalog, describe);
	d->schema = TextVal(in.inputs[1]);
	if (!ValidName(d->schema)) {
		RejectLocal();
	}
	if (describe) {
		d->kind = TextVal(in.inputs[2]);
		d->name = TextVal(in.inputs[3]);
		if ((d->kind != "table" && d->kind != "index") || !ValidName(d->name)) {
			RejectLocal();
		}
	}
	names = {"sqlstate", "stage", "document"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(d);
}

static void DDLMetadataScan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<Db2DDLState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &d = in.bind_data->Cast<Db2DDLMetaData>();
	string sqlstate, stage = "source_identity", document;

	shared_ptr<ODBCConnection> connection;
	unique_ptr<Db2DDLSession> session;
	try {
		if (ctx.IsInterrupted()) {
			RejectDDL("57014", "source_identity", "interrupted");
		}
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::DB2), d.catalog.connection_string, 5);
		session = make_uniq<Db2DDLSession>(ctx, connection->GetConn(), 10);
		auto &s = *session;
		auto facts = QueryDb2Facts(s, connection->GetConn(), d.schema);
		bool ns_present = !facts.schema_id.empty();
		const string db_identity = facts.database_name + ":" + facts.instance_name;
		const string srv_identity = facts.host_name + ":" + facts.instance_name;

		if (!d.describe) {
			document = string("{") + "\"contract_version\":1,\"product\":\"db2\",\"source_version\":" +
			           JSON(facts.version) + ",\"database\":" + JSON(facts.database_name) +
			           ",\"database_identity\":" + JSON(db_identity) + ",\"server_identity\":" + JSON(srv_identity) +
			           ",\"schema\":" + JSON(d.schema) + ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
			           ",\"source_read_only\":" + (facts.read_only ? "true" : "false") +
			           ",\"session_identity\":{\"user\":" + JSON(facts.session_user) +
			           ",\"current_schema\":" + JSON(facts.current_schema) +
			           "},\"visibility\":\"CATALOG_READ\",\"executor_available\":false" +
			           ",\"identity_protection\":\"UNVERIFIED\"}";
		} else {
			string presence = "ABSENT";
			string object_identity, observed_kind, owning_table, table_identity;
			bool has_reorg_flag = false;
			bool reorg_pending = false;
			string cols_json = "[]", indexes_json = "[]", constraints_json = "[]";
			if (ns_present && d.kind == "table") {
				auto objs = s.Rows(
				    "SELECT RTRIM(CHAR(t.TBSPACEID)),RTRIM(CHAR(t.TABLEID)),RTRIM(CHAR(t.CREATE_TIME)),t.TYPE,"
				    "COALESCE(a.REORG_PENDING,'N') FROM SYSCAT.TABLES t "
				    "LEFT JOIN SYSIBMADM.ADMINTABINFO a ON a.TABSCHEMA=t.TABSCHEMA AND a.TABNAME=t.TABNAME "
				    "WHERE t.TABSCHEMA=" +
				    Literal(d.schema) + " AND t.TABNAME=" + Literal(d.name) + " AND t.TYPE IN ('T','V')");
				if (objs.size() == 1) {
					presence = "PRESENT";
					object_identity = objs[0][0] + ":" + objs[0][1] + ":" + objs[0][2];
					observed_kind = objs[0][3] == "V" ? "v" : "r";
					if (observed_kind == "r") {
						has_reorg_flag = true;
						reorg_pending = Trimmed(objs[0][4]) == "Y";
					}
					auto col_rows = s.Rows(
					    "SELECT RTRIM(CHAR(c.COLNO + 1)),c.COLNAME,RTRIM(c.TYPENAME),"
					    "RTRIM(CHAR(c.LENGTH)),COALESCE(RTRIM(CHAR(c.SCALE)),'0'),c.NULLS,"
					    "CASE WHEN c.DEFAULT IS NOT NULL THEN '1' ELSE '0' END "
					    "FROM SYSCAT.COLUMNS c WHERE c.TABSCHEMA=" +
					    Literal(d.schema) + " AND c.TABNAME=" + Literal(d.name) + " ORDER BY c.COLNO");
					cols_json = "[";
					for (idx_t idx = 0; idx < col_rows.size() && idx < 256; idx++) {
						if (idx > 0) {
							cols_json += ",";
						}
						auto &r = col_rows[idx];
						cols_json += "{\"ordinal\":" + r[0] + ",\"name\":" + JSON(r[1]) + ",\"type\":" + JSON(r[2]) +
						             ",\"length\":" + (r[3].empty() ? "0" : r[3]) + ",\"precision\":null,\"scale\":" +
						             (r[4].empty() ? "0" : r[4]) +
						             ",\"nullable\":" + (r[5] == "Y" ? "true" : "false") +
						             ",\"has_default\":" + (r[6] == "1" ? "true" : "false") + "}";
					}
					cols_json += "]";

					auto idx_rows = s.Rows(
					    "SELECT i.INDNAME,RTRIM(i.INDEXTYPE),i.UNIQUERULE "
					    "FROM SYSCAT.INDEXES i WHERE i.TABSCHEMA=" +
					    Literal(d.schema) + " AND i.TABNAME=" + Literal(d.name) + " ORDER BY i.INDNAME");
					indexes_json = "[";
					for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
						if (idx > 0) {
							indexes_json += ",";
						}
						auto &r = idx_rows[idx];
						bool uniq = r[2] == "U" || r[2] == "P";
						indexes_json += "{\"table\":" + JSON(d.name) + ",\"name\":" + JSON(r[0]) +
						                ",\"type\":" + JSON(r[1]) + ",\"unique\":" + (uniq ? "true" : "false") + "}";
					}
					indexes_json += "]";

					auto con_rows = s.Rows(
					    "SELECT c.CONSTNAME,c.TYPE,c.ENFORCED,c.CHECKEXISTINGDATA "
					    "FROM SYSCAT.TABCONST c WHERE c.TABSCHEMA=" +
					    Literal(d.schema) + " AND c.TABNAME=" + Literal(d.name) +
					    " AND c.TYPE IN ('P','U','F','K') ORDER BY c.CONSTNAME");
					constraints_json = "[";
					for (idx_t idx = 0; idx < con_rows.size() && idx < 256; idx++) {
						if (idx > 0) {
							constraints_json += ",";
						}
						auto &r = con_rows[idx];
						constraints_json += "{\"name\":" + JSON(r[0]) + ",\"type\":" + JSON(r[1]) +
						                    ",\"enforced\":" + JSON(r[2]) +
						                    ",\"check_existing_data\":" + JSON(r[3]) + "}";
					}
					constraints_json += "]";
				}
			} else if (ns_present && d.kind == "index") {
				auto idx_rows = s.Rows(
				    "SELECT RTRIM(CHAR(i.TBSPACEID)),RTRIM(CHAR(i.INDEX_OBJECTID)),RTRIM(CHAR(i.IID)),"
				    "i.TABNAME,i.UNIQUERULE,RTRIM(CHAR(t.TBSPACEID)),RTRIM(CHAR(t.TABLEID)),"
				    "RTRIM(CHAR(t.CREATE_TIME)) FROM SYSCAT.INDEXES i "
				    "JOIN SYSCAT.TABLES t ON t.TABSCHEMA=i.TABSCHEMA AND t.TABNAME=i.TABNAME "
				    "WHERE i.INDSCHEMA=" +
				    Literal(d.schema) + " AND i.INDNAME=" + Literal(d.name) + " AND i.TABSCHEMA=" + Literal(d.schema) +
				    " ORDER BY i.TABNAME");
				indexes_json = "[";
				for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
					if (idx > 0) {
						indexes_json += ",";
					}
					auto &r = idx_rows[idx];
					bool uniq = r[4] == "U" || r[4] == "P";
					indexes_json += "{\"table\":" + JSON(r[3]) + ",\"name\":" + JSON(d.name) +
					                ",\"unique\":" + (uniq ? "true" : "false") + "}";
				}
				indexes_json += "]";
				if (idx_rows.size() == 1) {
					presence = "PRESENT";
					object_identity = idx_rows[0][0] + ":" + idx_rows[0][1] + ":" + idx_rows[0][2];
					owning_table = idx_rows[0][3];
					table_identity = idx_rows[0][5] + ":" + idx_rows[0][6] + ":" + idx_rows[0][7];
					observed_kind = "i";
				} else if (idx_rows.size() > 1) {
					presence = "UNKNOWN";
				}
			}
			document = string("{") + "\"contract_version\":1,\"product\":\"db2\",\"source_version\":" +
			           JSON(facts.version) + ",\"database\":" + JSON(facts.database_name) +
			           ",\"database_identity\":" + JSON(db_identity) + ",\"server_identity\":" + JSON(srv_identity) +
			           ",\"schema\":" + JSON(d.schema) + ",\"name\":" + JSON(d.name) + ",\"kind\":" + JSON(d.kind) +
			           ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
			           ",\"presence\":" + JSON(presence) + ",\"object_identity\":" + JSON(object_identity);
			if (!observed_kind.empty()) {
				document += ",\"observed_kind\":" + JSON(observed_kind);
			}
			if (!owning_table.empty()) {
				document += ",\"owning_table\":" + JSON(owning_table);
			}
			if (!table_identity.empty()) {
				document += ",\"table_identity\":" + JSON(table_identity);
			}
			if (has_reorg_flag) {
				document += ",\"reorg_pending\":" + string(reorg_pending ? "true" : "false");
			}
			document += ",\"metadata_complete\":false,\"identity_protection\":\"UNVERIFIED\",\"columns\":" + cols_json +
			            ",\"indexes\":" + indexes_json + ",\"constraints\":" + constraints_json + "}";
		}
		stage.clear();
	} catch (const GuardedODBCConnectionFailure &failure) {
		sqlstate = ValidSQLState(failure.diagnostic.state, "08006");
		stage = "source_identity";
		document.clear();
	} catch (const DDLRejectException &rej) {
		sqlstate = ValidSQLState(rej.sqlstate, "HY000");
		stage = rej.stage;
		document.clear();
	} catch (...) {
		sqlstate = ctx.IsInterrupted() ? "57014" : "08006";
		document.clear();
	}
	session.reset();
	if (connection) {
		connection->Close();
	}
	out.SetCardinality(1);
	out.SetValue(0, 0, Value(sqlstate));
	out.SetValue(1, 0, Value(stage));
	out.SetValue(2, 0, Value(document));
}

// ---------- Guarded execution table function ----------

struct DDLColumnDef {
	string name;
	string type;
	int32_t length = 0;
	int32_t precision = 0;
	int32_t scale = 0;
	bool not_null = false;
	string default_kind;
	string default_value;
	bool primary_key = false;
};

struct Db2DDLIntent {
	string database;
	string schema;
	string operation;
	string action;
	string name;
	string index_name;
	string owner;
	string new_name;
	string column_name;
	string default_kind;
	string default_value;
	string constraint_name;
	string constraint_kind;
	string check_column;
	string check_operator;
	string check_value;
	string reference_schema;
	string reference_table;
	string identity_mode;
	vector<string> constraint_keys;
	vector<string> reference_keys;
	vector<DDLColumnDef> columns;
	vector<string> keys;
	bool conditional = false;
	string primary_key_name;
	vector<string> primary_key_keys;
};

static string FormatDb2Default(const string &kind, const string &value) {
	if (kind == "null") {
		if (!value.empty()) {
			RejectDDL("0A000", "source_definition", "invalid null default");
		}
		return "NULL";
	}
	if (kind == "integer") {
		if (!IsSignedInt64Literal(value)) {
			RejectDDL("0A000", "source_definition", "invalid integer default");
		}
		return value;
	}
	if (kind == "string") {
		return Literal(value);
	}
	if (kind == "boolean") {
		if (value == "true") {
			return "TRUE";
		}
		if (value == "false") {
			return "FALSE";
		}
		RejectDDL("0A000", "source_definition", "invalid boolean default");
	}
	RejectDDL("0A000", "source_definition", "unsupported default kind");
}

static string MapColumnTypeDb2(const DDLColumnDef &col) {
	if (col.type == "numeric") {
		if (col.length != 0 || col.precision < 1 || col.precision > 31 || col.scale < 0 || col.scale > col.precision) {
			RejectDDL("0A000", "source_definition", "unsupported Db2 numeric bounds");
		}
		return "DECIMAL(" + std::to_string(col.precision) + "," + std::to_string(col.scale) + ")";
	}
	if (col.type == "time") {
		if (col.length != 0 || col.scale != 0 || col.precision != 0) {
			RejectDDL("0A000", "source_definition", "Db2 TIME only supports whole-second precision time(0)");
		}
		return "TIME";
	}
	if (col.type == "timestamp") {
		if (col.length != 0 || col.precision < 0 || col.precision > 6 || col.scale != 0) {
			RejectDDL("0A000", "source_definition", "unsupported Db2 TIMESTAMP precision");
		}
		return "TIMESTAMP(" + std::to_string(col.precision) + ")";
	}
	if (col.precision != 0 || col.scale != 0) {
		RejectLocal();
	}
	if (col.type == "varchar") {
		if (col.length < 1 || col.length > 32672) {
			RejectDDL("0A000", "source_definition", "unsupported Db2 VARCHAR length");
		}
		return "VARCHAR(" + std::to_string(col.length) + ")";
	}
	if (col.length != 0) {
		RejectLocal();
	}
	if (col.type == "boolean") {
		return "BOOLEAN";
	}
	if (col.type == "smallint") {
		return "SMALLINT";
	}
	if (col.type == "integer") {
		return "INTEGER";
	}
	if (col.type == "bigint") {
		return "BIGINT";
	}
	if (col.type == "real") {
		return "REAL";
	}
	if (col.type == "double") {
		return "DOUBLE";
	}
	if (col.type == "text") {
		return "CLOB(1M)";
	}
	if (col.type == "bytea") {
		return "BLOB(1M)";
	}
	if (col.type == "date") {
		return "DATE";
	}
	RejectDDL("0A000", "source_definition", "unsupported Db2 column type");
}

static vector<string> ReadStringList(const Value &v, idx_t max_len) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::LIST) {
		RejectLocal();
	}
	vector<string> out;
	const auto &items = ListValue::GetChildren(v);
	if (items.size() > max_len) {
		RejectLocal();
	}
	for (const auto &item : items) {
		auto s = TextVal(item);
		if (!ValidName(s) || std::find(out.begin(), out.end(), s) != out.end()) {
			RejectLocal();
		}
		out.push_back(std::move(s));
	}
	return out;
}

static Db2DDLIntent ParseUnifiedIntent(const vector<Value> &inputs) {
	if (inputs.size() != 5) {
		RejectLocal();
	}
	Db2DDLIntent d;
	d.database = TextVal(inputs[1]);
	d.schema = TextVal(inputs[2]);
	d.operation = TextVal(inputs[3]);
	if (!ValidName(d.database) || !ValidName(d.schema) || !ValidUUID(d.operation)) {
		RejectLocal();
	}
	const auto &sval = inputs[4];
	if (sval.IsNull() || sval.type().id() != LogicalTypeId::STRUCT) {
		RejectLocal();
	}
	const auto &types = StructType::GetChildTypes(sval.type());
	const auto &fields = StructValue::GetChildren(sval);
	const vector<string> expected = {
	    "contract_version", "target_contract",  "action",          "name",             "index_name",
	    "owner",            "new_name",         "column_name",     "default_kind",     "default_value",
	    "constraint_name",  "constraint_kind",  "check_column",    "check_operator",   "check_value",
	    "reference_schema", "reference_table",  "identity_mode",   "constraint_keys",  "reference_keys",
	    "columns",          "keys",             "conditional",     "primary_key_name", "primary_key_keys",
	};
	if (types.size() != expected.size()) {
		RejectLocal();
	}
	for (idx_t i = 0; i < expected.size(); i++) {
		if (types[i].first != expected[i] || fields[i].IsNull()) {
			RejectLocal();
		}
	}
	if (IntVal(fields[0]) != 1 || TextVal(fields[1]) != "EXECUTION_NAME") {
		RejectLocal();
	}
	d.action = TextVal(fields[2]);
	d.name = TextVal(fields[3]);
	d.index_name = TextVal(fields[4]);
	d.owner = TextVal(fields[5]);
	d.new_name = TextVal(fields[6]);
	d.column_name = TextVal(fields[7]);
	d.default_kind = TextVal(fields[8]);
	d.default_value = TextVal(fields[9]);
	d.constraint_name = TextVal(fields[10]);
	d.constraint_kind = TextVal(fields[11]);
	d.check_column = TextVal(fields[12]);
	d.check_operator = TextVal(fields[13]);
	d.check_value = TextVal(fields[14]);
	d.reference_schema = TextVal(fields[15]);
	d.reference_table = TextVal(fields[16]);
	d.identity_mode = TextVal(fields[17]);

	if (!ValidName(d.name)) {
		RejectLocal();
	}
	for (const auto *opt : {&d.index_name, &d.owner, &d.new_name, &d.column_name, &d.constraint_name,
	                        &d.check_column, &d.reference_schema, &d.reference_table}) {
		if (!opt->empty() && !ValidName(*opt)) {
			RejectLocal();
		}
	}
	if (d.default_value.size() > 4096) {
		RejectLocal();
	}
	d.constraint_keys = ReadStringList(fields[18], 32);
	d.reference_keys = ReadStringList(fields[19], 32);
	d.keys = ReadStringList(fields[21], 32);
	d.conditional = BoolVal(fields[22]);
	d.primary_key_name = TextVal(fields[23]);
	if (!d.primary_key_name.empty() && !ValidName(d.primary_key_name)) {
		RejectLocal();
	}
	d.primary_key_keys = ReadStringList(fields[24], 32);

	if (d.conditional && d.action != "create_table" && d.action != "drop_table" && d.action != "add_column" &&
	    d.action != "drop_column" && d.action != "create_index" && d.action != "create_unique_index" &&
	    d.action != "drop_index") {
		RejectLocal();
	}
	if ((!d.primary_key_keys.empty() || !d.primary_key_name.empty()) && d.action != "create_table") {
		RejectLocal();
	}

	if (fields[20].type().id() != LogicalTypeId::LIST) {
		RejectLocal();
	}
	const auto &cols = ListValue::GetChildren(fields[20]);
	if (cols.size() > 256) {
		RejectLocal();
	}
	vector<string> seen_cols;
	idx_t inline_pk_count = 0;
	for (const auto &col : cols) {
		if (col.IsNull() || col.type().id() != LogicalTypeId::STRUCT) {
			RejectLocal();
		}
		const auto &ct = StructType::GetChildTypes(col.type());
		const auto &cv = StructValue::GetChildren(col);
		if (ct.size() != 9 || ct[0].first != "name" || ct[1].first != "type" || ct[2].first != "length" ||
		    ct[3].first != "precision" || ct[4].first != "scale" || ct[5].first != "not_null" ||
		    ct[6].first != "default_kind" || ct[7].first != "default_value" || ct[8].first != "primary_key") {
			RejectLocal();
		}
		DDLColumnDef def;
		def.name = TextVal(cv[0]);
		def.type = TextVal(cv[1]);
		def.length = IntVal(cv[2]);
		def.precision = IntVal(cv[3]);
		def.scale = IntVal(cv[4]);
		def.not_null = BoolVal(cv[5]);
		def.default_kind = TextVal(cv[6]);
		def.default_value = TextVal(cv[7]);
		def.primary_key = BoolVal(cv[8]);
		if (!ValidName(def.name) || std::find(seen_cols.begin(), seen_cols.end(), def.name) != seen_cols.end()) {
			RejectLocal();
		}
		if (def.default_value.size() > 4096) {
			RejectLocal();
		}
		if (def.default_kind.empty() || def.default_kind == "null") {
			if (!def.default_value.empty()) {
				RejectLocal();
			}
		} else if (def.default_kind == "integer") {
			if (!IsSignedInt64Literal(def.default_value)) {
				RejectLocal();
			}
		} else if (def.default_kind == "boolean") {
			if (def.default_value != "true" && def.default_value != "false") {
				RejectLocal();
			}
		} else if (def.default_kind != "string") {
			RejectLocal();
		}
		if (d.action == "alter_column_type" && (def.not_null || !def.default_kind.empty() || def.primary_key)) {
			RejectLocal();
		}
		if (d.action == "add_column" && def.primary_key) {
			RejectLocal();
		}
		if ((def.not_null || def.primary_key) && def.default_kind == "null") {
			RejectLocal();
		}
		if (def.primary_key) {
			inline_pk_count++;
		}
		seen_cols.push_back(def.name);
		d.columns.push_back(std::move(def));
	}
	if (inline_pk_count > 1 || (inline_pk_count > 0 && !d.primary_key_keys.empty())) {
		RejectLocal();
	}
	for (const auto &k : d.primary_key_keys) {
		auto it = std::find_if(d.columns.begin(), d.columns.end(), [&](const DDLColumnDef &c) { return c.name == k; });
		if (it == d.columns.end() || it->default_kind == "null") {
			RejectLocal();
		}
	}
	if (!d.primary_key_name.empty() && inline_pk_count == 0 && d.primary_key_keys.empty()) {
		RejectLocal();
	}
	return d;
}

struct Db2DDLExecData : TableFunctionData {
	ODBCCatalog &catalog;
	Db2DDLIntent intent;
	bool diagnostics = false;
	Db2DDLExecData(ODBCCatalog &c, Db2DDLIntent d) : catalog(c), intent(std::move(d)) {
	}
};

static unique_ptr<FunctionData> DDLExecBind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                            vector<string> &names) {
	if (in.inputs.size() != 5) {
		RejectLocal();
	}
	auto db_name = TextVal(in.inputs[0]);
	auto intent = ParseUnifiedIntent(in.inputs);
	auto &catalog = ResolveDb2Catalog(ctx, db_name);
	auto data = make_uniq<Db2DDLExecData>(catalog, std::move(intent));
	auto diag = in.named_parameters.find("source_diagnostics");
	if (diag != in.named_parameters.end() && !diag->second.IsNull()) {
		data->diagnostics = diag->second.GetValue<bool>();
	}
	names = {"operation_id", "status", "sqlstate", "stage", "warning_count"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::BIGINT};
	if (data->diagnostics) {
		names.push_back("source_error");
		types.push_back(LogicalType::VARCHAR);
	}
	return std::move(data);
}

static void DDLExecScan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<Db2DDLState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &d = in.bind_data->Cast<Db2DDLExecData>();
	const auto &i = d.intent;

	string status = "CONFIRMED_NOT_EXECUTED";
	string error = "0A000";
	string stage = "source_identity";
	int64_t warnings = 0;
	string source_error;
	bool sent = false;
	bool commit_started = false;
	bool is_truncate = false;

	shared_ptr<ODBCConnection> connection;
	unique_ptr<Db2DDLSession> session;
	try {
		if (ctx.IsInterrupted()) {
			RejectDDL("57014", "source_identity", "operation cancelled");
		}
		if (ODBCDialectUtil::IsInternalSchema(ODBCDialect::DB2, i.schema)) {
			RejectDDL("42501", "source_identity", "system Db2 schema cannot be modified");
		}
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::DB2), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE,
		                                     reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0))) {
			RejectDDL("25006", "source_identity", "failed to configure read-write connection mode");
		}
		if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT,
		                                     reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0))) {
			RejectDDL("08006", "source_identity", "failed to configure session commit boundary");
		}
		SQLULEN autocommit_mode = SQL_AUTOCOMMIT_ON;
		if (!SQL_SUCCEEDED(SQLGetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, &autocommit_mode, sizeof(autocommit_mode),
		                                     nullptr)) ||
		    autocommit_mode != SQL_AUTOCOMMIT_OFF) {
			RejectDDL("08006", "source_identity", "failed to verify manual commit mode");
		}
		session = make_uniq<Db2DDLSession>(ctx, dbc, 15);
		auto &s = *session;

		auto facts = QueryDb2Facts(s, dbc, i.schema);
		if (facts.database_name != i.database) {
			RejectDDL("0A000", "source_identity", "Db2 connected database mismatch");
		}
		if (facts.read_only) {
			RejectDDL("25006", "source_identity", "Db2 source is read-only standby");
		}
		if (facts.schema_id.empty()) {
			RejectDDL("3F000", "source_definition", "target schema does not exist");
		}

		// Configure short fail-fast lock timeout across UOWs.
		s.Exec("SET CURRENT LOCK TIMEOUT 2");
		s.Exec("COMMIT");

		stage = "source_definition";
		s.current_stage = "source_definition";

		const string target = Identifier(i.schema) + "." + Identifier(i.name);
		auto table_row = [&](const string &tbl) {
			return s.Rows("SELECT t.TYPE,t.STATUS,COALESCE(t.TABLEORG,''),COALESCE(a.REORG_PENDING,'N'),"
			              "COALESCE(a.READ_ACCESS_ONLY,'N') FROM SYSCAT.TABLES t "
			              "LEFT JOIN SYSIBMADM.ADMINTABINFO a ON a.TABSCHEMA=t.TABSCHEMA AND a.TABNAME=t.TABNAME "
			              "WHERE t.TABSCHEMA=" +
			              Literal(i.schema) + " AND t.TABNAME=" + Literal(tbl));
		};
		auto check_table_accessible = [&](const string &tbl) {
			auto rows = table_row(tbl);
			if (rows.empty()) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			const auto &r = rows[0];
			if (r[0] != "T") {
				RejectDDL("42809", "source_definition", "target object is not a base table");
			}
			if (Trimmed(r[3]) == "Y") {
				RejectDDL("0A000", "source_definition", "Db2 target table is in REORG PENDING state");
			}
			if (Trimmed(r[4]) == "Y") {
				RejectDDL("25006", "source_definition", "Db2 target table is read-access-only");
			}
			if (r[1] != "N" || Trimmed(r[2]) != "R") {
				RejectDDL("0A000", "source_definition", "Db2 target table state or organization is unsupported");
			}
		};
		auto relation_exists = [&](const string &obj) {
			return s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TABLES WHERE TABSCHEMA=" + Literal(i.schema) +
			                " AND TABNAME=" + Literal(obj)) != "0";
		};
		auto index_exists = [&](const string &idx) {
			return s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.INDEXES WHERE INDSCHEMA=" + Literal(i.schema) +
			                " AND INDNAME=" + Literal(idx)) != "0";
		};
		auto col_info = [&](const string &tbl, const string &col) {
			return s.Rows("SELECT NULLS,RTRIM(TYPENAME),IDENTITY,COALESCE(RTRIM(GENERATED),''),"
			              "CASE WHEN DEFAULT IS NOT NULL THEN '1' ELSE '0' END "
			              "FROM SYSCAT.COLUMNS WHERE TABSCHEMA=" +
			              Literal(i.schema) + " AND TABNAME=" + Literal(tbl) + " AND COLNAME=" + Literal(col));
		};
		auto has_referencing_fk = [&](const string &tbl, bool exclude_self) {
			string sql = "SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.REFERENCES WHERE REFTABSCHEMA=" +
			             Literal(i.schema) + " AND REFTABNAME=" + Literal(tbl);
			if (exclude_self) {
				sql += " AND NOT (TABSCHEMA=" + Literal(i.schema) + " AND TABNAME=" + Literal(tbl) + ")";
			}
			return s.Scalar(sql) != "0";
		};

		string native_sql;
		bool no_op_commit = false;
		bool source_noop = false;
		vector<string> lock_tables;
		auto add_lock_table = [&](const string &tbl) {
			if (!tbl.empty() && std::find(lock_tables.begin(), lock_tables.end(), tbl) == lock_tables.end()) {
				lock_tables.push_back(tbl);
			}
		};
		auto check_col_dependencies = [&](const string &tbl_name, const string &col_name) {
			if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.INDEXCOLUSE ic "
			             "JOIN SYSCAT.INDEXES ix ON ix.INDSCHEMA=ic.INDSCHEMA AND ix.INDNAME=ic.INDNAME "
			             "WHERE ix.TABSCHEMA=" +
			             Literal(i.schema) + " AND ix.TABNAME=" + Literal(tbl_name) +
			             " AND ic.COLNAME=" + Literal(col_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify column referenced by an index");
			}
			if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.KEYCOLUSE WHERE TABSCHEMA=" + Literal(i.schema) +
			             " AND TABNAME=" + Literal(tbl_name) + " AND COLNAME=" + Literal(col_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify column referenced by a key constraint");
			}
			if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.COLCHECKS WHERE TABSCHEMA=" + Literal(i.schema) +
			             " AND TABNAME=" + Literal(tbl_name) + " AND COLNAME=" + Literal(col_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify column referenced by a check constraint");
			}
			if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TABDEP WHERE BSCHEMA=" + Literal(i.schema) +
			             " AND BNAME=" + Literal(tbl_name) + " AND BTYPE='T' AND DTYPE IN ('V','S')") != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify column of table referenced by a view");
			}
		};

		if (i.action == "drop_column") {
			RejectDDL("0A000", "source_definition", "Db2 DROP COLUMN triggers REORG PENDING and is unsupported");
		} else if (i.action == "set_not_null" || i.action == "drop_not_null") {
			RejectDDL("0A000", "source_definition",
			          "Db2 SET/DROP NOT NULL triggers REORG PENDING and is unsupported");
		} else if (i.action == "create_table") {
			if (i.columns.empty() || !i.keys.empty() || !i.index_name.empty() || !i.owner.empty() ||
			    !i.new_name.empty() || !i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "invalid create_table fields");
			}
			auto existing_tbl = table_row(i.name);
			if (!existing_tbl.empty()) {
				if (i.conditional && existing_tbl[0][0] == "T") {
					source_noop = true;
				} else {
					RejectDDL("42P07", "source_definition", "target table already exists");
				}
			}
			if (!source_noop) {
				vector<string> eff_pk = i.primary_key_keys;
				if (eff_pk.empty()) {
					for (const auto &c : i.columns) {
						if (c.primary_key) {
							eff_pk.push_back(c.name);
							break;
						}
					}
				}
				if (!i.primary_key_name.empty() && index_exists(i.primary_key_name)) {
					RejectDDL("42710", "source_definition", "backing index name already exists in schema");
				}
				vector<string> defs;
				for (const auto &c : i.columns) {
					bool eff_nn = c.not_null || c.primary_key ||
					              std::find(i.primary_key_keys.begin(), i.primary_key_keys.end(), c.name) !=
					                  i.primary_key_keys.end();
					string col_def = Identifier(c.name) + " " + MapColumnTypeDb2(c);
					if (eff_nn) {
						col_def += " NOT NULL";
					}
					if (!c.default_kind.empty()) {
						col_def += " DEFAULT " + FormatDb2Default(c.default_kind, c.default_value);
					}
					defs.push_back(std::move(col_def));
				}
				if (!eff_pk.empty()) {
					vector<string> pk_quoted;
					pk_quoted.reserve(eff_pk.size());
					for (const auto &k : eff_pk) {
						pk_quoted.push_back(Identifier(k));
					}
					string pk_clause;
					if (!i.primary_key_name.empty()) {
						pk_clause = "CONSTRAINT " + Identifier(i.primary_key_name) + " ";
					}
					pk_clause += "PRIMARY KEY (" + StringUtil::Join(pk_quoted, ",") + ")";
					defs.push_back(std::move(pk_clause));
				}
				native_sql = "CREATE TABLE " + target + " (" + StringUtil::Join(defs, ",") + ")";
			}
		} else if (i.action == "drop_table") {
			if (table_row(i.name).empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42P01", "source_definition", "target table does not exist");
				}
			} else {
				check_table_accessible(i.name);
				if (has_referencing_fk(i.name, true)) {
					RejectDDL("2BP01", "source_definition", "cannot drop table with referencing foreign keys");
				}
				native_sql = "DROP TABLE " + target;
				add_lock_table(i.name);
			}
		} else if (i.action == "rename_table") {
			if (i.new_name.empty() || i.new_name == i.name) {
				RejectDDL("0A000", "source_definition", "invalid rename_table name");
			}
			check_table_accessible(i.name);
			if (relation_exists(i.new_name)) {
				RejectDDL("42P07", "source_definition", "destination table already exists");
			}
			if (has_referencing_fk(i.name, false)) {
				RejectDDL("2BP01", "source_definition", "cannot rename table referenced by foreign keys");
			}
			native_sql = "RENAME TABLE " + target + " TO " + Identifier(i.new_name);
			add_lock_table(i.name);
		} else if (i.action == "add_column") {
			if (i.columns.size() != 1 || i.columns[0].primary_key) {
				RejectDDL("0A000", "source_definition", "invalid add_column payload");
			}
			check_table_accessible(i.name);
			const auto &c = i.columns[0];
			if (!col_info(i.name, c.name).empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42701", "source_definition", "column already exists");
				}
			} else {
				if (c.not_null && (c.default_kind.empty() || c.default_kind == "null")) {
					if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM (SELECT 1 FROM " + target +
					             " FETCH FIRST 1 ROW ONLY) t") != "0") {
						RejectDDL("23502", "source_definition",
						          "cannot add NOT NULL column without default to non-empty table");
					}
					RejectDDL("0A000", "source_definition", "Db2 ADD COLUMN NOT NULL requires an explicit DEFAULT");
				}
				string col_def = Identifier(c.name) + " " + MapColumnTypeDb2(c);
				if (c.not_null) {
					col_def += " NOT NULL";
				}
				if (!c.default_kind.empty()) {
					col_def += " DEFAULT " + FormatDb2Default(c.default_kind, c.default_value);
				}
				native_sql = "ALTER TABLE " + target + " ADD COLUMN " + col_def;
				add_lock_table(i.name);
			}
		} else if (i.action == "alter_column_type") {
			if (i.column_name.empty() || i.columns.size() != 1 || i.columns[0].name != i.column_name) {
				RejectDDL("0A000", "source_definition", "invalid alter_column_type payload");
			}
			check_table_accessible(i.name);
			auto col_rows = s.Rows("SELECT NULLS,RTRIM(TYPENAME),RTRIM(CHAR(LENGTH)),IDENTITY,COALESCE(RTRIM(GENERATED),'') "
			                       "FROM SYSCAT.COLUMNS WHERE TABSCHEMA=" +
			                       Literal(i.schema) + " AND TABNAME=" + Literal(i.name) +
			                       " AND COLNAME=" + Literal(i.column_name));
			if (col_rows.empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			const auto &r = col_rows[0];
			if (r[3] == "Y" || !r[4].empty()) {
				RejectDDL("0A000", "source_definition", "cannot alter identity or generated column");
			}
			const string cur_type = StringUtil::Upper(Trimmed(r[1]));
			const int cur_len = std::stoi(r[2]);
			const auto &target_col = i.columns[0];
			if (cur_type != "VARCHAR" || target_col.type != "varchar" || target_col.precision != 0 ||
			    target_col.scale != 0 || target_col.length <= cur_len || target_col.length > 32672) {
				RejectDDL("0A000", "source_definition",
				          "Db2 ALTER COLUMN TYPE only supports REORG-free VARCHAR length widening");
			}
			check_col_dependencies(i.name, i.column_name);
			native_sql = "ALTER TABLE " + target + " ALTER COLUMN " + Identifier(i.column_name) +
			             " SET DATA TYPE VARCHAR(" + std::to_string(target_col.length) + ")";
			add_lock_table(i.name);
		} else if (i.action == "rename_column") {
			if (i.column_name.empty() || i.new_name.empty() || i.new_name == i.column_name) {
				RejectDDL("0A000", "source_definition", "invalid rename_column payload");
			}
			check_table_accessible(i.name);
			if (col_info(i.name, i.column_name).empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			if (!col_info(i.name, i.new_name).empty()) {
				RejectDDL("42701", "source_definition", "destination column already exists");
			}
			native_sql = "ALTER TABLE " + target + " RENAME COLUMN " + Identifier(i.column_name) + " TO " +
			             Identifier(i.new_name);
			add_lock_table(i.name);
		} else if (i.action == "set_default" || i.action == "drop_default") {
			if (i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "column name required");
			}
			check_table_accessible(i.name);
			auto cinfo = col_info(i.name, i.column_name);
			if (cinfo.empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			if (cinfo[0][2] == "Y" || !cinfo[0][3].empty()) {
				RejectDDL("0A000", "source_definition", "cannot alter default on identity or generated column");
			}
			if (i.action == "drop_default") {
				if (cinfo[0][4] == "0") {
					no_op_commit = true;
				} else {
					native_sql =
					    "ALTER TABLE " + target + " ALTER COLUMN " + Identifier(i.column_name) + " DROP DEFAULT";
					add_lock_table(i.name);
				}
			} else {
				native_sql = "ALTER TABLE " + target + " ALTER COLUMN " + Identifier(i.column_name) +
				             " SET DEFAULT " + FormatDb2Default(i.default_kind, i.default_value);
				add_lock_table(i.name);
			}
		} else if (i.action == "add_constraint") {
			if (i.constraint_name.empty() || i.constraint_kind.empty()) {
				RejectDDL("0A000", "source_definition", "invalid add_constraint payload");
			}
			check_table_accessible(i.name);
			if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TABCONST WHERE TABSCHEMA=" + Literal(i.schema) +
			             " AND TABNAME=" + Literal(i.name) + " AND CONSTNAME=" + Literal(i.constraint_name)) != "0") {
				RejectDDL("42710", "source_definition", "constraint name already exists");
			}
			auto quote_list = [](const vector<string> &items) {
				vector<string> quoted;
				quoted.reserve(items.size());
				for (const auto &it : items) {
					quoted.push_back(Identifier(it));
				}
				return StringUtil::Join(quoted, ",");
			};
			if (i.constraint_kind == "primary_key" || i.constraint_kind == "unique") {
				if (i.constraint_keys.empty()) {
					RejectDDL("0A000", "source_definition", "constraint keys required");
				}
				if (i.constraint_kind == "primary_key" &&
				    s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TABCONST WHERE TABSCHEMA=" + Literal(i.schema) +
				             " AND TABNAME=" + Literal(i.name) + " AND TYPE='P'") != "0") {
					RejectDDL("42P16", "source_definition", "multiple primary keys for table are not allowed");
				}
				if (index_exists(i.constraint_name)) {
					RejectDDL("42710", "source_definition", "backing index name already exists in schema");
				}
				for (const auto &k : i.constraint_keys) {
					auto cinfo = col_info(i.name, k);
					if (cinfo.empty()) {
						RejectDDL("42703", "source_definition", "constraint column does not exist");
					}
					if (cinfo[0][0] != "N") {
						RejectDDL("42831", "source_definition",
						          "Db2 PRIMARY KEY/UNIQUE requires NOT NULL columns");
					}
				}
				native_sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Identifier(i.constraint_name) +
				             (i.constraint_kind == "primary_key" ? " PRIMARY KEY (" : " UNIQUE (") +
				             quote_list(i.constraint_keys) + ")";
				add_lock_table(i.name);
			} else if (i.constraint_kind == "foreign_key") {
				if (i.constraint_keys.empty() || i.reference_table.empty() ||
				    i.reference_keys.size() != i.constraint_keys.size() || i.reference_schema != i.schema) {
					RejectDDL("0A000", "source_definition", "invalid foreign key definition");
				}
				for (const auto &k : i.constraint_keys) {
					if (col_info(i.name, k).empty()) {
						RejectDDL("42703", "source_definition", "foreign key column does not exist");
					}
				}
				check_table_accessible(i.reference_table);
				for (const auto &rk : i.reference_keys) {
					if (col_info(i.reference_table, rk).empty()) {
						RejectDDL("42703", "source_definition", "referenced key column does not exist");
					}
				}
				native_sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Identifier(i.constraint_name) +
				             " FOREIGN KEY (" + quote_list(i.constraint_keys) + ") REFERENCES " +
				             Identifier(i.reference_schema) + "." + Identifier(i.reference_table) + " (" +
				             quote_list(i.reference_keys) + ")";
				add_lock_table(i.name);
				add_lock_table(i.reference_table);
			} else if (i.constraint_kind == "check") {
				if (i.check_column.empty() || i.check_value.empty() || i.check_value == "-") {
					RejectDDL("0A000", "source_definition", "invalid check constraint");
				}
				if (i.check_operator != "=" && i.check_operator != "<>" && i.check_operator != "<" &&
				    i.check_operator != ">" && i.check_operator != "<=" && i.check_operator != ">=") {
					RejectDDL("0A000", "source_definition", "unsupported check operator");
				}
				for (idx_t idx = 0; idx < i.check_value.size(); idx++) {
					char c = i.check_value[idx];
					if (!((c >= '0' && c <= '9') || (idx == 0 && c == '-'))) {
						RejectDDL("0A000", "source_definition", "invalid integer check literal");
					}
				}
				if (col_info(i.name, i.check_column).empty()) {
					RejectDDL("42703", "source_definition", "check column does not exist");
				}
				native_sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Identifier(i.constraint_name) +
				             " CHECK (" + Identifier(i.check_column) + " " + i.check_operator + " " +
				             i.check_value + ")";
				add_lock_table(i.name);
			} else {
				RejectDDL("0A000", "source_definition", "unsupported constraint kind");
			}
		} else if (i.action == "drop_constraint") {
			if (i.constraint_name.empty()) {
				RejectDDL("0A000", "source_definition", "constraint name required");
			}
			check_table_accessible(i.name);
			auto crows = s.Rows("SELECT TYPE FROM SYSCAT.TABCONST WHERE TABSCHEMA=" + Literal(i.schema) +
			                    " AND TABNAME=" + Literal(i.name) + " AND CONSTNAME=" + Literal(i.constraint_name));
			if (crows.empty()) {
				RejectDDL("42704", "source_definition", "constraint does not exist");
			}
			if ((crows[0][0] == "P" || crows[0][0] == "U") &&
			    s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.REFERENCES WHERE REFTABSCHEMA=" +
			             Literal(i.schema) + " AND REFTABNAME=" + Literal(i.name) +
			             " AND REFKEYNAME=" + Literal(i.constraint_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot drop constraint with referencing foreign keys");
			}
			native_sql = "ALTER TABLE " + target + " DROP CONSTRAINT " + Identifier(i.constraint_name);
			add_lock_table(i.name);
		} else if (i.action == "create_index" || i.action == "create_unique_index") {
			if (i.index_name.empty() || i.keys.empty()) {
				RejectDDL("0A000", "source_definition", "index name and keys required");
			}
			check_table_accessible(i.name);
			auto idx_rows = s.Rows("SELECT TABSCHEMA,TABNAME FROM SYSCAT.INDEXES WHERE INDSCHEMA=" + Literal(i.schema) +
			                       " AND INDNAME=" + Literal(i.index_name));
			if (!idx_rows.empty()) {
				if (i.conditional && idx_rows.size() == 1 && Trimmed(idx_rows[0][0]) == i.schema &&
				    idx_rows[0][1] == i.name) {
					source_noop = true;
				} else {
					RejectDDL("42P07", "source_definition", "index name already exists in schema");
				}
			}
			if (!source_noop) {
				vector<string> cols;
				for (const auto &k : i.keys) {
					if (col_info(i.name, k).empty()) {
						RejectDDL("42703", "source_definition", "index column does not exist");
					}
					cols.push_back(Identifier(k));
				}
				native_sql = string(i.action == "create_unique_index" ? "CREATE UNIQUE INDEX " : "CREATE INDEX ") +
				             Identifier(i.schema) + "." + Identifier(i.index_name) + " ON " + target + " (" +
				             StringUtil::Join(cols, ",") + ")";
				add_lock_table(i.name);
			}
		} else if (i.action == "drop_index") {
			auto idx_rows = s.Rows(
			    "SELECT TABNAME,UNIQUERULE FROM SYSCAT.INDEXES WHERE INDSCHEMA=" + Literal(i.schema) +
			    " AND INDNAME=" + Literal(i.name) + " AND TABSCHEMA=" + Literal(i.schema));
			if (idx_rows.empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42704", "source_definition", "index does not exist");
				}
			} else if (!i.owner.empty() && idx_rows[0][0] != i.owner) {
				RejectDDL("42704", "source_definition", "index owner mismatch");
			} else {
				if (idx_rows[0][1] == "P" ||
				    s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.CONSTDEP WHERE TABSCHEMA=" + Literal(i.schema) +
				             " AND TABNAME=" + Literal(idx_rows[0][0]) + " AND BSCHEMA=" + Literal(i.schema) +
				             " AND BNAME=" + Literal(i.name) + " AND BTYPE='I'") != "0") {
					RejectDDL("2BP01", "source_definition", "cannot drop constraint backing index directly");
				}
				check_table_accessible(idx_rows[0][0]);
				native_sql = "DROP INDEX " + target;
				add_lock_table(idx_rows[0][0]);
			}
		} else if (i.action == "rename_index") {
			if (i.new_name.empty() || i.new_name == i.name) {
				RejectDDL("0A000", "source_definition", "invalid rename_index payload");
			}
			auto idx_rows = s.Rows("SELECT TABNAME FROM SYSCAT.INDEXES WHERE INDSCHEMA=" + Literal(i.schema) +
			                       " AND INDNAME=" + Literal(i.name) + " AND TABSCHEMA=" + Literal(i.schema));
			if (idx_rows.empty()) {
				RejectDDL("42704", "source_definition", "index does not exist");
			}
			if (!i.owner.empty() && idx_rows[0][0] != i.owner) {
				RejectDDL("42704", "source_definition", "index owner mismatch");
			}
			if (index_exists(i.new_name)) {
				RejectDDL("42P07", "source_definition", "destination index name already exists in schema");
			}
			check_table_accessible(idx_rows[0][0]);
			native_sql = "RENAME INDEX " + target + " TO " + Identifier(i.new_name);
			add_lock_table(idx_rows[0][0]);
		} else if (i.action == "truncate") {
			if (i.identity_mode == "restart") {
				RejectDDL("0A000", "source_definition", "Db2 TRUNCATE RESTART IDENTITY is unsupported");
			}
			check_table_accessible(i.name);
			if (has_referencing_fk(i.name, false)) {
				RejectDDL("2BP01", "source_definition", "cannot truncate table with referencing foreign keys");
			}
			if (s.Scalar("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TRIGGERS WHERE TABSCHEMA=" + Literal(i.schema) +
			             " AND TABNAME=" + Literal(i.name) + " AND TRIGEVENT='D'") != "0") {
				RejectDDL("0A000", "source_definition", "cannot truncate table with DELETE triggers");
			}
			is_truncate = true;
			native_sql = "TRUNCATE TABLE " + target + " IMMEDIATE";
		} else {
			RejectDDL("0A000", "source_definition", "unsupported Db2 DDL action");
		}

		if (is_truncate) {
			// Db2 requires TRUNCATE TABLE ... IMMEDIATE to be the first statement in its UOW.
			s.Exec("COMMIT");
		} else if (!no_op_commit && !source_noop && !lock_tables.empty()) {
			std::sort(lock_tables.begin(), lock_tables.end());
			stage = "source_lock";
			s.current_stage = "source_lock";
			for (const auto &tbl : lock_tables) {
				if (ctx.IsInterrupted()) {
					RejectDDL("57014", "source_lock", "operation cancelled");
				}
				s.Exec("LOCK TABLE " + Identifier(i.schema) + "." + Identifier(tbl) + " IN EXCLUSIVE MODE");
			}
		}

		stage = "source_execute";
		s.current_stage = "source_execute";
		if (!no_op_commit && !source_noop) {
			if (ctx.IsInterrupted()) {
				RejectDDL("57014", "source_execute", "operation cancelled");
			}
			s.last_warning_count = 0;
			sent = true;
			s.Exec(native_sql);
		}
		stage = "source_commit";
		s.current_stage = "source_commit";
		commit_started = true;
		status = "UNKNOWN";
		s.Exec("COMMIT");
		status = "COMMITTED";
		if (source_noop) {
			warnings = 0;
			error.clear();
			stage = "source_noop";
		} else {
			warnings = s.last_warning_count;
			error.clear();
			stage = warnings > 0 ? "source_warning" : "";
		}
	} catch (const GuardedODBCLocalRejection &) {
		error = status == "COMMITTED" ? "XX000" : "0A000";
		stage = "source_identity";
	} catch (const GuardedODBCConnectionFailure &failure) {
		auto src = failure.diagnostic;
		src.product = "db2";
		source_error = src.JSON();
		error = ValidSQLState(src.state, "08006");
		stage = "source_identity";
	} catch (const DDLRejectException &rej) {
		if (source_error.empty() && session) {
			source_error = session->source.JSON();
		}
		error = ValidSQLState(rej.sqlstate, "HY000");
		stage = rej.stage;
		if (sent && status != "COMMITTED") {
			if (session && IsDb2LockTimeout(session->last_native, session->last_odbc_state.c_str(),
			                                session->last_message)) {
				status = "CONFIRMED_NOT_EXECUTED";
				stage = "source_lock";
				error = "55P03";
			} else if (is_truncate) {
				if (session && !commit_started &&
				    IsDb2TruncateConfirmedNotExecuted(session->last_native, session->last_odbc_state.c_str(),
				                                     session->last_message)) {
					status = "CONFIRMED_NOT_EXECUTED";
					stage = "source_execute";
				} else {
					status = "UNKNOWN";
				}
			} else if (!commit_started && session && session->RollbackQuiet()) {
				status = "CONFIRMED_ROLLED_BACK";
			} else {
				status = "UNKNOWN";
			}
		}
	} catch (...) {
		if (source_error.empty() && session) {
			source_error = session->source.JSON();
		}
		error = session && !session->error.empty() ? session->error : (ctx.IsInterrupted() ? "57014" : "08006");
		if (sent && status != "COMMITTED") {
			if (!is_truncate && !commit_started && session && session->RollbackQuiet()) {
				status = "CONFIRMED_ROLLED_BACK";
			} else {
				status = "UNKNOWN";
			}
		}
	}

	if (status != "COMMITTED" && session) {
		session->RollbackQuiet();
	}
	if (status == "CONFIRMED_ROLLED_BACK" && (stage == "source_lock" || stage == "source_definition")) {
		stage = "source_execute";
	}
	if (status != "COMMITTED") {
		warnings = 0;
		if (stage == "source_warning" || stage == "source_verify") {
			stage = "source_execute";
		}
		if (status != "CONFIRMED_NOT_EXECUTED" && (stage == "source_lock" || stage == "source_definition")) {
			stage = "source_execute";
		}
		error = ValidSQLState(error, "HY000");
	}

	session.reset();
	if (connection) {
		connection->Close();
	}

	out.SetCardinality(1);
	out.SetValue(0, 0, Value(i.operation));
	out.SetValue(1, 0, Value(status));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value::BIGINT(warnings));
	if (d.diagnostics) {
		out.SetValue(5, 0, Value(source_error));
	}
}
} // namespace

void RegisterDb2GuardedDDL(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("db2_ddl_capabilities_v1", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      DDLMetadataScan, DDLMetadataBind, DDLInit));
	loader.RegisterFunction(TableFunction(
	    "db2_ddl_describe_v1",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DDLMetadataScan,
	    DDLMetadataBind, DDLInit));
	TableFunction exec("db2_guarded_ddl_v1",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                    LogicalType::ANY},
	                   DDLExecScan, DDLExecBind, DDLInit);
	exec.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec);
}
} // namespace duckdb
