//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hana_guarded_ddl.cpp
//
// Guarded SAP HANA capability observation, catalog description, and
// implicit-commit-safe core-subset DDL execution on an isolated session.
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
                                   const string &msg = "guarded HANA DDL rejected") {
	throw DDLRejectException(sqlstate, stage, msg);
}

[[noreturn]] static void RejectLocal() {
	throw GuardedODBCLocalRejection("guarded HANA DDL input invalid");
}

static bool IsHanaSystemSchema(const string &schema) {
	auto normalized = StringUtil::Lower(schema);
	StringUtil::Trim(normalized);
	// SYSTEM is the default user schema on standalone HANA tenants; only block metadata schemas here.
	return normalized == "information_schema" || normalized == "sys" || StringUtil::StartsWith(normalized, "_sys_");
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

static bool IsHanaLockTimeout(SQLINTEGER native, const string &message) {
	if (native == 131 || native == 146) {
		return true;
	}
	auto lower = StringUtil::Lower(message);
	return StringUtil::Contains(lower, "lock wait time") || StringUtil::Contains(lower, "lock-wait time") ||
	       StringUtil::Contains(lower, "nowait");
}

static string MapHanaDDLSQLState(SQLINTEGER native, const char *odbc_state, const string &message) {
	if (IsHanaLockTimeout(native, message)) {
		return "55P03";
	}
	if (native == 133) {
		return "40P01";
	}
	if (native == 7) {
		auto lower = StringUtil::Lower(message);
		if (StringUtil::Contains(lower, "null value exists")) {
			return "23502";
		}
		if (StringUtil::Contains(lower, "foreign key") || StringUtil::Contains(lower, "referenced table")) {
			return "2BP01";
		}
		return "0A000";
	}
	switch (native) {
	case 288:
	case 289:
	case 290:
		return "42P07";
	case 259:
		return "42P01";
	case 260:
		return "42703";
	case 261:
	case 397:
		return "42704";
	case 308:
		return "42701";
	case 301:
		return "23505";
	case 287:
		return "23502";
	case 461:
	case 462:
		return "23503";
	case 456:
	case 457:
		return "23514";
	case 417:
	case 419:
		return "2BP01";
	case 258:
		return "42501";
	default:
		break;
	}
	if (odbc_state) {
		return ValidSQLState(odbc_state, "HY000");
	}
	return "HY000";
}

// Deterministic HANA errors guaranteed to fail before altering persistent definition.
static bool IsHanaConfirmedNotExecuted(SQLINTEGER native, const string &message) {
	if (IsHanaLockTimeout(native, message)) {
		return true;
	}
	switch (native) {
	case 7:
	case 133:
	case 257:
	case 258:
	case 259:
	case 260:
	case 261:
	case 267:
	case 287:
	case 288:
	case 289:
	case 290:
	case 301:
	case 308:
	case 318:
	case 397:
	case 417:
	case 419:
	case 456:
	case 457:
	case 461:
	case 462:
		return true;
	default:
		return false;
	}
}

// Never configure SQL_ATTR_QUERY_TIMEOUT: unixODBC / vendor drivers may install
// process-wide SIGALRM handlers. Cancel via a session-scoped SQLCancelHandle watcher.
class HanaDDLSession {
public:
	HanaDDLSession(ClientContext &ctx_p, SQLHDBC dbc, uint32_t stmt_timeout_sec = 15)
	    : ctx(ctx_p), stmt(nullptr, ODBCStatementDelete), timeout_sec(stmt_timeout_sec) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS) {
			RejectDDL("08006", "source_identity", "failed to allocate HANA statement");
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
	~HanaDDLSession() {
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
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "hana");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLCHAR msg[1024] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, msg, sizeof(msg), &len);
		last_native = native;
		last_message = !source.message.empty() ? source.message : string(reinterpret_cast<char *>(msg));
		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
		} else {
			error = MapHanaDDLSQLState(native, reinterpret_cast<const char *>(state), last_message);
		}
		RejectDDL(error, current_stage, last_message.empty() ? "HANA DDL error" : last_message);
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

	vector<vector<string>> Rows(const string &sql, bool allow_null = true) {
		Start(false);
		Check(SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS));
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols < 1 || cols > 64) {
			RejectDDL("XX000", current_stage, "unexpected column count in HANA query");
		}
		vector<vector<string>> rows;
		idx_t total_bytes = 0;
		SQLRETURN rc;
		while ((rc = SQLFetch(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			if (rows.size() >= 8192) {
				RejectDDL("54000", current_stage, "HANA metadata row bound exceeded");
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
						RejectDDL("XX000", current_stage, "unexpected NULL HANA metadata value");
					}
					row.emplace_back("");
				} else if (ind < 0 || static_cast<idx_t>(ind) >= sizeof(buf)) {
					RejectDDL("54000", current_stage, "HANA metadata cell bound exceeded");
				} else {
					row.emplace_back(buf, static_cast<size_t>(ind));
				}
				total_bytes += row.back().size();
				if (total_bytes > 1024 * 1024) {
					RejectDDL("54000", current_stage, "HANA metadata document bound exceeded");
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
			RejectDDL("XX000", current_stage, "expected scalar HANA result");
		}
		return r[0][0];
	}

private:
	ClientContext &ctx;
	ODBCStatementPtr stmt;
	std::mutex mu;
	std::condition_variable cv;
	std::thread watcher;
	uint32_t timeout_sec = 15;
	bool stopped = false, cleanup_mode = false, cancel_sent = true;
	std::chrono::steady_clock::time_point statement_deadline = std::chrono::steady_clock::now();
	std::atomic<bool> cancelled {false};
};

struct HanaServerFacts {
	string version;
	string system_id;
	string database_name;
	string host_name;
	string schema_id;
	string session_user;
	string current_schema;
};

static HanaServerFacts QueryHanaFacts(HanaDDLSession &s, SQLHDBC dbc, const string &schema) {
	SQLCHAR dbms[128] = {0};
	SQLSMALLINT dbms_len = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(dbc, SQL_DBMS_NAME, dbms, sizeof(dbms), &dbms_len)) ||
	    string(reinterpret_cast<char *>(dbms)) != "HDB") {
		RejectDDL("0A000", "source_identity", "connected datasource is not SAP HANA");
	}
	SQLCHAR ver[256] = {0};
	SQLSMALLINT ver_len = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(dbc, SQL_DBMS_VER, ver, sizeof(ver), &ver_len)) || ver_len <= 0 ||
	    ver_len >= static_cast<SQLSMALLINT>(sizeof(ver))) {
		RejectDDL("0A000", "source_identity", "HANA version identity unavailable");
	}
	auto db_rows = s.Rows("SELECT SYSTEM_ID, DATABASE_NAME, HOST, VERSION FROM SYS.M_DATABASE");
	if (db_rows.size() != 1 || db_rows[0].size() != 4) {
		RejectDDL("0A000", "source_identity", "HANA database identity query failed");
	}
	auto ctx_rows = s.Rows("SELECT CURRENT_USER, CURRENT_SCHEMA FROM DUMMY");
	if (ctx_rows.size() != 1 || ctx_rows[0].size() != 2) {
		RejectDDL("0A000", "source_identity", "HANA session identity query failed");
	}
	auto sch_rows = s.Rows("SELECT SCHEMA_NAME, TO_VARCHAR(CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7') "
	                       "FROM SYS.SCHEMAS WHERE SCHEMA_NAME=" +
	                       Literal(schema));

	HanaServerFacts f;
	f.system_id = Trimmed(db_rows[0][0]);
	f.database_name = Trimmed(db_rows[0][1]);
	f.host_name = Trimmed(db_rows[0][2]);
	f.version = Trimmed(db_rows[0][3]);
	if (f.version.empty()) {
		f.version = Trimmed(string(reinterpret_cast<char *>(ver), static_cast<size_t>(ver_len)));
	}
	f.session_user = Trimmed(ctx_rows[0][0]);
	f.current_schema = Trimmed(ctx_rows[0][1]);
	if (sch_rows.size() == 1) {
		f.schema_id = Trimmed(sch_rows[0][0]) + ":" + Trimmed(sch_rows[0][1]);
	}
	if (f.version.empty() || f.database_name.empty() || f.system_id.empty() || f.host_name.empty()) {
		RejectDDL("0A000", "source_identity", "HANA database/host identity unavailable");
	}
	return f;
}

// ---------- Observation table functions ----------

struct HanaDDLMetaData : TableFunctionData {
	ODBCCatalog &catalog;
	string schema;
	string kind;
	string name;
	bool describe = false;
	HanaDDLMetaData(ODBCCatalog &c, bool d) : catalog(c), describe(d) {
	}
};

struct HanaDDLState : GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> DDLInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<HanaDDLState>();
}

static ODBCCatalog &ResolveHanaCatalog(ClientContext &ctx, const string &db_name) {
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, db_name);
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog") {
		throw BinderException("DDL catalog is unavailable");
	}
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::HANA) {
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
	auto &catalog = ResolveHanaCatalog(ctx, db_name);
	bool describe = in.inputs.size() == 4;
	auto d = make_uniq<HanaDDLMetaData>(catalog, describe);
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
	auto &state = in.global_state->Cast<HanaDDLState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &d = in.bind_data->Cast<HanaDDLMetaData>();
	string sqlstate, stage = "source_identity", document;

	shared_ptr<ODBCConnection> connection;
	unique_ptr<HanaDDLSession> session;
	try {
		if (ctx.IsInterrupted()) {
			RejectDDL("57014", "source_identity", "interrupted");
		}
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::HANA), d.catalog.connection_string, 5);
		session = make_uniq<HanaDDLSession>(ctx, connection->GetConn(), 10);
		auto &s = *session;
		auto facts = QueryHanaFacts(s, connection->GetConn(), d.schema);
		bool ns_present = !facts.schema_id.empty();
		const string db_identity = facts.system_id + ":" + facts.database_name;
		const string srv_identity = facts.host_name + ":" + facts.system_id;

		if (!d.describe) {
			document = string("{") + "\"contract_version\":1,\"product\":\"hana\",\"source_version\":" +
			           JSON(facts.version) + ",\"database\":" + JSON(facts.database_name) +
			           ",\"database_identity\":" + JSON(db_identity) + ",\"server_identity\":" + JSON(srv_identity) +
			           ",\"schema\":" + JSON(d.schema) + ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") + ",\"source_read_only\":false" +
			           ",\"session_identity\":{\"user\":" + JSON(facts.session_user) +
			           ",\"current_schema\":" + JSON(facts.current_schema) +
			           "},\"visibility\":\"CATALOG_READ\",\"executor_available\":false" +
			           ",\"identity_protection\":\"UNVERIFIED\"}";
		} else {
			string presence = "ABSENT";
			string object_identity, observed_kind, owning_table, table_identity;
			string cols_json = "[]", indexes_json = "[]", constraints_json = "[]";
			if (ns_present && d.kind == "table") {
				auto tbls = s.Rows("SELECT TO_VARCHAR(TABLE_OID),TABLE_TYPE,"
				                   "TO_VARCHAR(CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7') "
				                   "FROM SYS.TABLES WHERE SCHEMA_NAME=" +
				                   Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.name));
				if (tbls.size() == 1) {
					presence = "PRESENT";
					object_identity = tbls[0][0] + ":" + tbls[0][2];
					observed_kind = "r";
					auto col_rows = s.Rows(
					    "SELECT TO_VARCHAR(POSITION),COLUMN_NAME,DATA_TYPE_NAME,"
					    "COALESCE(TO_VARCHAR(LENGTH),'0'),COALESCE(TO_VARCHAR(SCALE),'0'),IS_NULLABLE,"
					    "CASE WHEN DEFAULT_VALUE IS NOT NULL THEN '1' ELSE '0' END "
					    "FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=" +
					    Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.name) + " ORDER BY POSITION");
					cols_json = "[";
					for (idx_t idx = 0; idx < col_rows.size() && idx < 256; idx++) {
						if (idx > 0) {
							cols_json += ",";
						}
						auto &r = col_rows[idx];
						cols_json += "{\"ordinal\":" + r[0] + ",\"name\":" + JSON(r[1]) + ",\"type\":" + JSON(r[2]) +
						             ",\"length\":" + r[3] + ",\"scale\":" + r[4] +
						             ",\"nullable\":" + (r[5] == "TRUE" ? "true" : "false") +
						             ",\"has_default\":" + (r[6] == "1" ? "true" : "false") + "}";
					}
					cols_json += "]";

					auto idx_rows = s.Rows("SELECT INDEX_NAME,INDEX_TYPE,COALESCE(CONSTRAINT,'') "
					                       "FROM SYS.INDEXES WHERE SCHEMA_NAME=" +
					                       Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.name) +
					                       " ORDER BY INDEX_NAME");
					indexes_json = "[";
					for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
						if (idx > 0) {
							indexes_json += ",";
						}
						auto &r = idx_rows[idx];
						bool uniq = r[2] == "PRIMARY KEY" || r[2] == "UNIQUE" || r[2] == "NOT NULL UNIQUE" ||
						            StringUtil::EndsWith(r[1], " UNIQUE");
						indexes_json += "{\"table\":" + JSON(d.name) + ",\"name\":" + JSON(r[0]) +
						                ",\"type\":" + JSON(r[1]) + ",\"unique\":" + (uniq ? "true" : "false") + "}";
					}
					indexes_json += "]";

					auto con_rows = s.Rows(
					    "SELECT CONSTRAINT_NAME, CASE WHEN MAX(IS_PRIMARY_KEY)='TRUE' THEN 'P' "
					    "WHEN MAX(IS_UNIQUE_KEY)='TRUE' THEN 'U' ELSE 'C' END AS KIND "
					    "FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" +
					    Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.name) +
					    " GROUP BY CONSTRAINT_NAME "
					    "UNION ALL SELECT CONSTRAINT_NAME,'F' FROM SYS.REFERENTIAL_CONSTRAINTS "
					    "WHERE SCHEMA_NAME=" +
					    Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.name) + " ORDER BY 1");
					constraints_json = "[";
					for (idx_t idx = 0; idx < con_rows.size() && idx < 256; idx++) {
						if (idx > 0) {
							constraints_json += ",";
						}
						auto &r = con_rows[idx];
						constraints_json += "{\"name\":" + JSON(r[0]) + ",\"type\":" + JSON(r[1]) + "}";
					}
					constraints_json += "]";
				} else {
					auto views = s.Rows("SELECT TO_VARCHAR(VIEW_OID) FROM SYS.VIEWS WHERE SCHEMA_NAME=" +
					                    Literal(d.schema) + " AND VIEW_NAME=" + Literal(d.name));
					if (views.size() == 1) {
						presence = "PRESENT";
						object_identity = views[0][0];
						observed_kind = "v";
					}
				}
			} else if (ns_present && d.kind == "index") {
				auto idx_rows = s.Rows(
				    "SELECT TO_VARCHAR(i.INDEX_OID),i.TABLE_NAME,TO_VARCHAR(t.TABLE_OID),"
				    "TO_VARCHAR(t.CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7'),i.INDEX_TYPE,COALESCE(i.CONSTRAINT,'') "
				    "FROM SYS.INDEXES i JOIN SYS.TABLES t ON t.SCHEMA_NAME=i.SCHEMA_NAME AND t.TABLE_NAME=i.TABLE_NAME "
				    "WHERE i.SCHEMA_NAME=" +
				    Literal(d.schema) + " AND i.INDEX_NAME=" + Literal(d.name) + " ORDER BY i.TABLE_NAME");
				indexes_json = "[";
				for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
					if (idx > 0) {
						indexes_json += ",";
					}
					auto &r = idx_rows[idx];
					bool uniq = r[5] == "PRIMARY KEY" || r[5] == "UNIQUE" || r[5] == "NOT NULL UNIQUE" ||
					            StringUtil::EndsWith(r[4], " UNIQUE");
					indexes_json += "{\"table\":" + JSON(r[1]) + ",\"name\":" + JSON(d.name) +
					                ",\"unique\":" + (uniq ? "true" : "false") + "}";
				}
				indexes_json += "]";
				if (idx_rows.size() == 1) {
					presence = "PRESENT";
					object_identity = idx_rows[0][0];
					owning_table = idx_rows[0][1];
					table_identity = idx_rows[0][2] + ":" + idx_rows[0][3];
					observed_kind = "i";
				} else if (idx_rows.size() > 1) {
					presence = "UNKNOWN";
				}
			}
			document = string("{") + "\"contract_version\":1,\"product\":\"hana\",\"source_version\":" +
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

struct HanaDDLIntent {
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

static string FormatHanaDefault(const string &kind, const string &value) {
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

static bool IsSignedDecimalLiteral(const string &s) {
	if (s.empty() || s == "-" || s == "+" || s == ".") {
		return false;
	}
	idx_t i = 0;
	if (s[0] == '-' || s[0] == '+') {
		i = 1;
		if (i >= s.size()) {
			return false;
		}
	}
	bool seen_digit = false;
	bool seen_dot = false;
	for (; i < s.size(); i++) {
		char c = s[i];
		if (c >= '0' && c <= '9') {
			seen_digit = true;
		} else if (c == '.' && !seen_dot) {
			seen_dot = true;
		} else {
			return false;
		}
	}
	return seen_digit;
}

static bool HanaCheckExprMentionsColumn(const string &expr, const string &col) {
	for (idx_t idx = 0; idx < expr.size();) {
		char ch = expr[idx];
		if (ch == '\'') {
			idx++;
			while (idx < expr.size()) {
				if (expr[idx] == '\'') {
					if (idx + 1 < expr.size() && expr[idx + 1] == '\'') {
						idx += 2;
						continue;
					}
					idx++;
					break;
				}
				idx++;
			}
		} else if (ch == '"') {
			idx++;
			string ident;
			while (idx < expr.size()) {
				if (expr[idx] == '"') {
					if (idx + 1 < expr.size() && expr[idx + 1] == '"') {
						ident.push_back('"');
						idx += 2;
						continue;
					}
					idx++;
					break;
				}
				ident.push_back(expr[idx]);
				idx++;
			}
			if (ident == col) {
				return true;
			}
		} else if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_') {
			idx_t start = idx;
			while (idx < expr.size() && ((expr[idx] >= 'A' && expr[idx] <= 'Z') || (expr[idx] >= 'a' && expr[idx] <= 'z') ||
			                             (expr[idx] >= '0' && expr[idx] <= '9') || expr[idx] == '_' ||
			                             expr[idx] == '$' || expr[idx] == '#')) {
				idx++;
			}
			string tok = expr.substr(start, idx - start);
			if (tok == col || StringUtil::Upper(tok) == StringUtil::Upper(col)) {
				return true;
			}
		} else {
			idx++;
		}
	}
	return false;
}

static string MapColumnTypeHana(const DDLColumnDef &col) {
	if (col.type == "numeric") {
		if (col.length != 0 || col.precision < 1 || col.precision > 38 || col.scale < 0 || col.scale > col.precision) {
			RejectDDL("0A000", "source_definition", "unsupported HANA numeric bounds");
		}
		return "DECIMAL(" + std::to_string(col.precision) + "," + std::to_string(col.scale) + ")";
	}
	if (col.type == "time") {
		if (col.length != 0 || col.scale != 0 || col.precision != 0) {
			RejectDDL("0A000", "source_definition", "HANA TIME only supports whole-second precision time(0)");
		}
		return "TIME";
	}
	if (col.type == "timestamp") {
		if (col.length != 0 || col.precision < 0 || col.precision > 6 || col.scale != 0) {
			RejectDDL("0A000", "source_definition", "unsupported HANA TIMESTAMP precision");
		}
		return "TIMESTAMP";
	}
	if (col.precision != 0 || col.scale != 0) {
		RejectLocal();
	}
	if (col.type == "varchar") {
		if (col.length < 1 || col.length > 5000) {
			RejectDDL("0A000", "source_definition", "unsupported HANA NVARCHAR length");
		}
		return "NVARCHAR(" + std::to_string(col.length) + ")";
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
		return "NCLOB";
	}
	if (col.type == "bytea") {
		return "BLOB";
	}
	if (col.type == "date") {
		return "DATE";
	}
	RejectDDL("0A000", "source_definition", "unsupported HANA column type");
}

static string ReconstructHanaColumnType(const string &dtype, const string &len_str, const string &scale_str) {
	if (dtype == "BOOLEAN" || dtype == "TINYINT" || dtype == "SMALLINT" || dtype == "INTEGER" || dtype == "BIGINT" ||
	    dtype == "REAL" || dtype == "DOUBLE" || dtype == "CLOB" || dtype == "NCLOB" || dtype == "BLOB" ||
	    dtype == "DATE" || dtype == "TIME" || dtype == "SECONDDATE" || dtype == "TIMESTAMP") {
		return dtype;
	}
	if (dtype == "DECIMAL") {
		if (len_str.empty() || scale_str.empty() || scale_str == "-1") {
			RejectDDL("0A000", "source_definition", "unconstrained HANA DECIMAL alter is unsupported");
		}
		return "DECIMAL(" + len_str + "," + scale_str + ")";
	}
	if (dtype == "VARCHAR" || dtype == "NVARCHAR" || dtype == "CHAR" || dtype == "NCHAR" || dtype == "VARBINARY" ||
	    dtype == "BINARY") {
		if (len_str.empty()) {
			RejectDDL("0A000", "source_definition", "invalid HANA length type for alter");
		}
		return dtype + "(" + len_str + ")";
	}
	RejectDDL("0A000", "source_definition", "unsupported HANA column type for alter");
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

static HanaDDLIntent ParseUnifiedIntent(const vector<Value> &inputs) {
	if (inputs.size() != 5) {
		RejectLocal();
	}
	HanaDDLIntent d;
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

struct HanaDDLExecData : TableFunctionData {
	ODBCCatalog &catalog;
	HanaDDLIntent intent;
	bool diagnostics = false;
	HanaDDLExecData(ODBCCatalog &c, HanaDDLIntent d) : catalog(c), intent(std::move(d)) {
	}
};

static unique_ptr<FunctionData> DDLExecBind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                            vector<string> &names) {
	if (in.inputs.size() != 5) {
		RejectLocal();
	}
	auto db_name = TextVal(in.inputs[0]);
	auto intent = ParseUnifiedIntent(in.inputs);
	auto &catalog = ResolveHanaCatalog(ctx, db_name);
	auto data = make_uniq<HanaDDLExecData>(catalog, std::move(intent));
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
	auto &state = in.global_state->Cast<HanaDDLState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &d = in.bind_data->Cast<HanaDDLExecData>();
	const auto &i = d.intent;

	string status = "CONFIRMED_NOT_EXECUTED";
	string error = "0A000";
	string stage = "source_identity";
	int64_t warnings = 0;
	string source_error;
	bool sent = false;

	shared_ptr<ODBCConnection> connection;
	unique_ptr<HanaDDLSession> session;
	try {
		if (ctx.IsInterrupted()) {
			RejectDDL("57014", "source_identity", "operation cancelled");
		}
		if (IsHanaSystemSchema(i.schema)) {
			RejectDDL("42501", "source_identity", "system HANA schema cannot be modified");
		}
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::HANA), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE,
		                                     reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0))) {
			RejectDDL("25006", "source_identity", "failed to configure read-write connection mode");
		}
		if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT,
		                                     reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0))) {
			RejectDDL("08006", "source_identity", "failed to configure session commit boundary");
		}
		session = make_uniq<HanaDDLSession>(ctx, dbc, 15);
		auto &s = *session;

		QueryHanaFacts(s, dbc, i.schema);
		auto sch_cnt = s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.SCHEMAS WHERE SCHEMA_NAME=" + Literal(i.schema));
		if (sch_cnt == "0") {
			RejectDDL("3F000", "source_definition", "target schema does not exist");
		}

		// Fail-fast on lock contention instead of waiting indefinitely.
		s.Exec("SET TRANSACTION LOCK WAIT TIMEOUT 2000");

		stage = "source_definition";
		s.current_stage = "source_definition";

		const string target = Identifier(i.schema) + "." + Identifier(i.name);
		auto table_row = [&](const string &tbl) {
			return s.Rows("SELECT TABLE_TYPE,IS_TEMPORARY,IS_USER_DEFINED_TYPE FROM SYS.TABLES "
			              "WHERE SCHEMA_NAME=" +
			              Literal(i.schema) + " AND TABLE_NAME=" + Literal(tbl));
		};
		auto view_exists = [&](const string &obj) {
			return s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.VIEWS WHERE SCHEMA_NAME=" + Literal(i.schema) +
			                " AND VIEW_NAME=" + Literal(obj)) != "0";
		};
		auto check_base_table = [&](const string &tbl) {
			auto rows = table_row(tbl);
			if (rows.empty()) {
				if (view_exists(tbl)) {
					RejectDDL("42809", "source_definition", "target object is a view, not a table");
				}
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			const auto &r = rows[0];
			if ((r[0] != "COLUMN" && r[0] != "ROW") || r[1] != "FALSE" || r[2] != "FALSE") {
				RejectDDL("0A000", "source_definition", "unsupported HANA table kind");
			}
			return r[0];
		};
		auto relation_exists = [&](const string &obj) {
			return !table_row(obj).empty() || view_exists(obj);
		};
		auto index_exists = [&](const string &idx) {
			return s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.INDEXES WHERE SCHEMA_NAME=" + Literal(i.schema) +
			                " AND INDEX_NAME=" + Literal(idx)) != "0";
		};
		auto col_row = [&](const string &tbl, const string &col) {
			return s.Rows("SELECT DATA_TYPE_NAME,TO_VARCHAR(LENGTH),COALESCE(TO_VARCHAR(SCALE),'-1'),"
			              "IS_NULLABLE,CASE WHEN DEFAULT_VALUE IS NOT NULL THEN '1' ELSE '0' END,"
			              "COALESCE(GENERATION_TYPE,''),COALESCE(GENERATED_ALWAYS_AS,''),COALESCE(DEFAULT_VALUE,'') "
			              "FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=" +
			              Literal(i.schema) + " AND TABLE_NAME=" + Literal(tbl) + " AND COLUMN_NAME=" + Literal(col));
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
		auto check_col_dependencies = [&](const string &tbl_name, const string &col_name,
		                                  const vector<string> &col_meta) {
			auto gen_upper = StringUtil::Upper(Trimmed(col_meta[5]));
			if ((!gen_upper.empty() && gen_upper != "NONE") || !Trimmed(col_meta[6]).empty()) {
				RejectDDL("0A000", "source_definition", "cannot modify or drop identity or generated column");
			}
			if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" + Literal(i.schema) +
			             " AND TABLE_NAME=" + Literal(tbl_name) + " AND COLUMN_NAME=" + Literal(col_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify or drop column referenced by a constraint");
			}
			auto tbl_checks = s.Rows("SELECT COALESCE(CHECK_CONDITION,'') FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" +
			                         Literal(i.schema) + " AND TABLE_NAME=" + Literal(tbl_name) +
			                         " AND IS_PRIMARY_KEY='FALSE' AND IS_UNIQUE_KEY='FALSE' AND COLUMN_NAME IS NULL");
			for (const auto &chk : tbl_checks) {
				if (HanaCheckExprMentionsColumn(chk[0], col_name)) {
					RejectDDL("2BP01", "source_definition",
					          "cannot modify or drop column referenced by a check constraint");
				}
			}
			if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE (SCHEMA_NAME=" +
			             Literal(i.schema) + " AND TABLE_NAME=" + Literal(tbl_name) +
			             " AND COLUMN_NAME=" + Literal(col_name) +
			             ") OR (REFERENCED_SCHEMA_NAME=" + Literal(i.schema) +
			             " AND REFERENCED_TABLE_NAME=" + Literal(tbl_name) +
			             " AND REFERENCED_COLUMN_NAME=" + Literal(col_name) + ")") != "0") {
				RejectDDL("2BP01", "source_definition",
				          "cannot modify or drop column referenced by a foreign key constraint");
			}
			if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.INDEX_COLUMNS WHERE SCHEMA_NAME=" + Literal(i.schema) +
			             " AND TABLE_NAME=" + Literal(tbl_name) + " AND COLUMN_NAME=" + Literal(col_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify or drop column referenced by an index");
			}
			if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.OBJECT_DEPENDENCIES WHERE BASE_SCHEMA_NAME=" +
			             Literal(i.schema) + " AND BASE_OBJECT_NAME=" + Literal(tbl_name) +
			             " AND BASE_OBJECT_TYPE='TABLE' AND DEPENDENT_OBJECT_TYPE IN ('VIEW','CALCULATION_VIEW')") !=
			    "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify or drop column of table referenced by a view");
			}
		};

		if (i.action == "create_table") {
			if (i.columns.empty() || !i.keys.empty() || !i.index_name.empty() || !i.owner.empty() ||
			    !i.new_name.empty() || !i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "invalid create_table fields");
			}
			if (!table_row(i.name).empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42P07", "source_definition", "target table already exists");
				}
			} else if (view_exists(i.name)) {
				RejectDDL("42P07", "source_definition", "target table already exists");
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
				vector<string> defs;
				for (const auto &c : i.columns) {
					bool eff_nn = c.not_null || c.primary_key ||
					              std::find(i.primary_key_keys.begin(), i.primary_key_keys.end(), c.name) !=
					                  i.primary_key_keys.end();
					string col_def = Identifier(c.name) + " " + MapColumnTypeHana(c);
					if (!c.default_kind.empty()) {
						col_def += " DEFAULT " + FormatHanaDefault(c.default_kind, c.default_value);
					}
					col_def += eff_nn ? " NOT NULL" : " NULL";
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
				// Decision D3: SAP HANA CREATE TABLE defaults to COLUMN TABLE.
				native_sql = "CREATE COLUMN TABLE " + target + " (" + StringUtil::Join(defs, ",") + ")";
			}
		} else if (i.action == "drop_table") {
			if (table_row(i.name).empty() && !view_exists(i.name)) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42P01", "source_definition", "target table does not exist");
				}
			} else {
				check_base_table(i.name);
				if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE REFERENCED_SCHEMA_NAME=" +
				             Literal(i.schema) + " AND REFERENCED_TABLE_NAME=" + Literal(i.name) +
				             " AND NOT (SCHEMA_NAME=" + Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name) + ")") !=
				    "0") {
					RejectDDL("2BP01", "source_definition", "cannot drop table with referencing foreign keys");
				}
				native_sql = "DROP TABLE " + target;
				add_lock_table(i.name);
			}
		} else if (i.action == "rename_table") {
			if (i.new_name.empty() || i.new_name == i.name) {
				RejectDDL("0A000", "source_definition", "invalid rename_table name");
			}
			check_base_table(i.name);
			if (relation_exists(i.new_name)) {
				RejectDDL("42P07", "source_definition", "destination table already exists");
			}
			native_sql = "RENAME TABLE " + target + " TO " + Identifier(i.schema) + "." + Identifier(i.new_name);
			add_lock_table(i.name);
		} else if (i.action == "add_column") {
			if (i.columns.size() != 1 || i.columns[0].primary_key) {
				RejectDDL("0A000", "source_definition", "invalid add_column payload");
			}
			check_base_table(i.name);
			const auto &c = i.columns[0];
			if (!col_row(i.name, c.name).empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42701", "source_definition", "column already exists");
				}
			} else {
				if (c.not_null && (c.default_kind.empty() || c.default_kind == "null")) {
					if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM (SELECT 1 FROM " + target + " LIMIT 1)") != "0") {
						RejectDDL("23502", "source_definition",
						          "cannot add NOT NULL column without default to non-empty table");
					}
				}
				string col_def = Identifier(c.name) + " " + MapColumnTypeHana(c);
				if (!c.default_kind.empty()) {
					col_def += " DEFAULT " + FormatHanaDefault(c.default_kind, c.default_value);
				}
				col_def += c.not_null ? " NOT NULL" : " NULL";
				native_sql = "ALTER TABLE " + target + " ADD (" + col_def + ")";
				add_lock_table(i.name);
			}
		} else if (i.action == "drop_column") {
			if (i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "column name required");
			}
			check_base_table(i.name);
			auto cr = col_row(i.name, i.column_name);
			if (cr.empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42703", "source_definition", "column does not exist");
				}
			} else {
				if (std::stoi(s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=" +
				                       Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name))) <= 1) {
					RejectDDL("42P16", "source_definition", "cannot drop the only column of a table");
				}
				check_col_dependencies(i.name, i.column_name, cr[0]);
				native_sql = "ALTER TABLE " + target + " DROP (" + Identifier(i.column_name) + ")";
				add_lock_table(i.name);
			}
		} else if (i.action == "alter_column_type") {
			if (i.column_name.empty() || i.columns.size() != 1 || i.columns[0].name != i.column_name) {
				RejectDDL("0A000", "source_definition", "invalid alter_column_type payload");
			}
			const string ttype = check_base_table(i.name);
			if (ttype != "COLUMN") {
				RejectDDL("0A000", "source_definition", "HANA ALTER COLUMN TYPE requires COLUMN table");
			}
			auto cr = col_row(i.name, i.column_name);
			if (cr.empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			const auto &c = cr[0];
			check_col_dependencies(i.name, i.column_name, c);
			const string dtype = StringUtil::Upper(Trimmed(c[0]));
			const int cur_len = std::stoi(c[1]);
			const string scale_str = Trimmed(c[2]);
			const auto &target_col = i.columns[0];
			string new_type;

			if (dtype == "SMALLINT") {
				if (target_col.type == "integer" && target_col.length == 0 && target_col.precision == 0 &&
				    target_col.scale == 0) {
					new_type = "INTEGER";
				} else if (target_col.type == "bigint" && target_col.length == 0 && target_col.precision == 0 &&
				           target_col.scale == 0) {
					new_type = "BIGINT";
				}
			} else if (dtype == "INTEGER") {
				if (target_col.type == "bigint" && target_col.length == 0 && target_col.precision == 0 &&
				    target_col.scale == 0) {
					new_type = "BIGINT";
				}
			} else if (dtype == "REAL") {
				if (target_col.type == "double" && target_col.length == 0 && target_col.precision == 0 &&
				    target_col.scale == 0) {
					new_type = "DOUBLE";
				}
			} else if (dtype == "DECIMAL" && scale_str != "-1") {
				int s1 = std::stoi(scale_str);
				if (target_col.type == "numeric" && target_col.length == 0 && s1 == target_col.scale &&
				    target_col.precision > cur_len && target_col.precision <= 38) {
					new_type = "DECIMAL(" + std::to_string(target_col.precision) + "," +
					           std::to_string(target_col.scale) + ")";
				}
			} else if (dtype == "NVARCHAR") {
				if (target_col.type == "varchar" && target_col.precision == 0 && target_col.scale == 0 &&
				    target_col.length > cur_len && target_col.length <= 5000) {
					new_type = "NVARCHAR(" + std::to_string(target_col.length) + ")";
				}
			}
			if (new_type.empty()) {
				RejectDDL("0A000", "source_definition", "only lossless type widening is supported");
			}
			const string null_kw = c[3] == "FALSE" ? "NOT NULL" : "NULL";
			string alter_col_clause = Identifier(i.column_name) + " " + new_type + " " + null_kw;
			if (c[4] == "1") {
				if (dtype == "NVARCHAR" || dtype == "VARCHAR" || dtype == "CHAR" || dtype == "NCHAR") {
					alter_col_clause += " DEFAULT " + Literal(c[7]);
				} else {
					string def_trim = Trimmed(c[7]);
					bool ok = (dtype == "SMALLINT" || dtype == "INTEGER") ? IsSignedInt64Literal(def_trim)
					                                                      : IsSignedDecimalLiteral(def_trim);
					if (!ok) {
						RejectDDL("0A000", "source_definition",
						          "cannot preserve non-literal default during HANA alter column type");
					}
					alter_col_clause += " DEFAULT " + def_trim;
				}
			}
			native_sql = "ALTER TABLE " + target + " ALTER (" + alter_col_clause + ")";
			add_lock_table(i.name);
		} else if (i.action == "rename_column") {
			if (i.column_name.empty() || i.new_name.empty() || i.new_name == i.column_name) {
				RejectDDL("0A000", "source_definition", "invalid rename_column payload");
			}
			check_base_table(i.name);
			if (col_row(i.name, i.column_name).empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			if (!col_row(i.name, i.new_name).empty()) {
				RejectDDL("42701", "source_definition", "destination column already exists");
			}
			native_sql = "RENAME COLUMN " + target + "." + Identifier(i.column_name) + " TO " + Identifier(i.new_name);
			add_lock_table(i.name);
		} else if (i.action == "set_default" || i.action == "drop_default" || i.action == "set_not_null" ||
		           i.action == "drop_not_null") {
			if (i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "column name required");
			}
			check_base_table(i.name);
			auto cr = col_row(i.name, i.column_name);
			if (cr.empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			const auto &c = cr[0];
			auto gen_upper = StringUtil::Upper(Trimmed(c[5]));
			if ((!gen_upper.empty() && gen_upper != "NONE") || !Trimmed(c[6]).empty()) {
				RejectDDL("0A000", "source_definition", "cannot alter identity or generated column");
			}
			const string exact_type = ReconstructHanaColumnType(c[0], c[1], c[2]);
			const string null_kw = c[3] == "FALSE" ? "NOT NULL" : "NULL";
			if (i.action == "drop_default") {
				if (c[4] == "0") {
					no_op_commit = true;
				} else {
					native_sql = "ALTER TABLE " + target + " ALTER (" + Identifier(i.column_name) + " " + exact_type +
					             " " + null_kw + " DEFAULT NULL)";
					add_lock_table(i.name);
				}
			} else if (i.action == "set_default") {
				string def_expr = FormatHanaDefault(i.default_kind, i.default_value);
				native_sql = "ALTER TABLE " + target + " ALTER (" + Identifier(i.column_name) + " " + exact_type +
				             " " + null_kw + " DEFAULT " + def_expr + ")";
				add_lock_table(i.name);
			} else if (i.action == "set_not_null") {
				if (c[3] == "FALSE") {
					no_op_commit = true;
				} else {
					if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM " + target + " WHERE " + Identifier(i.column_name) +
					             " IS NULL") != "0") {
						RejectDDL("23502", "source_definition", "column contains NULL values");
					}
					native_sql = "ALTER TABLE " + target + " ALTER (" + Identifier(i.column_name) + " " + exact_type +
					             " NOT NULL)";
					add_lock_table(i.name);
				}
			} else {
				if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" + Literal(i.schema) +
				             " AND TABLE_NAME=" + Literal(i.name) + " AND COLUMN_NAME=" + Literal(i.column_name) +
				             " AND IS_PRIMARY_KEY='TRUE'") != "0") {
					RejectDDL("42P16", "source_definition", "cannot drop NOT NULL on primary key column");
				}
				if (c[3] == "TRUE") {
					no_op_commit = true;
				} else {
					native_sql = "ALTER TABLE " + target + " ALTER (" + Identifier(i.column_name) + " " + exact_type +
					             " NULL)";
					add_lock_table(i.name);
				}
			}
		} else if (i.action == "add_constraint") {
			if (i.constraint_name.empty() || i.constraint_kind.empty()) {
				RejectDDL("0A000", "source_definition", "invalid add_constraint payload");
			}
			const string ttype = check_base_table(i.name);
			if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" + Literal(i.schema) +
			             " AND TABLE_NAME=" + Literal(i.name) + " AND CONSTRAINT_NAME=" +
			             Literal(i.constraint_name)) != "0" ||
			    s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE SCHEMA_NAME=" +
			             Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name) +
			             " AND CONSTRAINT_NAME=" + Literal(i.constraint_name)) != "0") {
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
				    s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" + Literal(i.schema) +
				             " AND TABLE_NAME=" + Literal(i.name) + " AND IS_PRIMARY_KEY='TRUE'") != "0") {
					RejectDDL("42P16", "source_definition", "multiple primary keys for table are not allowed");
				}
				for (const auto &k : i.constraint_keys) {
					if (col_row(i.name, k).empty()) {
						RejectDDL("42703", "source_definition", "constraint column does not exist");
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
				if (ttype != "COLUMN") {
					RejectDDL("0A000", "source_definition", "HANA foreign keys require COLUMN tables");
				}
				for (const auto &k : i.constraint_keys) {
					if (col_row(i.name, k).empty()) {
						RejectDDL("42703", "source_definition", "foreign key column does not exist");
					}
				}
				if (check_base_table(i.reference_table) != "COLUMN") {
					RejectDDL("0A000", "source_definition", "HANA foreign key referenced table must be COLUMN table");
				}
				for (const auto &rk : i.reference_keys) {
					if (col_row(i.reference_table, rk).empty()) {
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
				if (col_row(i.name, i.check_column).empty()) {
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
			check_base_table(i.name);
			auto c_rows = s.Rows("SELECT IS_PRIMARY_KEY, IS_UNIQUE_KEY FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" +
			                     Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name) +
			                     " AND CONSTRAINT_NAME=" + Literal(i.constraint_name));
			auto fk_cnt = s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE SCHEMA_NAME=" +
			                       Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name) +
			                       " AND CONSTRAINT_NAME=" + Literal(i.constraint_name));
			if (c_rows.empty() && fk_cnt == "0") {
				RejectDDL("42704", "source_definition", "constraint does not exist");
			}
			if (!c_rows.empty() && (c_rows[0][0] == "TRUE" || c_rows[0][1] == "TRUE") &&
			    s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE REFERENCED_SCHEMA_NAME=" +
			             Literal(i.schema) + " AND REFERENCED_TABLE_NAME=" + Literal(i.name) +
			             " AND REFERENCED_CONSTRAINT_NAME=" + Literal(i.constraint_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot drop constraint with referencing foreign keys");
			}
			native_sql = "ALTER TABLE " + target + " DROP CONSTRAINT " + Identifier(i.constraint_name);
			add_lock_table(i.name);
		} else if (i.action == "create_index" || i.action == "create_unique_index") {
			if (i.index_name.empty() || i.keys.empty()) {
				RejectDDL("0A000", "source_definition", "index name and keys required");
			}
			check_base_table(i.name);
			auto idx_rows = s.Rows("SELECT TABLE_NAME FROM SYS.INDEXES WHERE SCHEMA_NAME=" + Literal(i.schema) +
			                       " AND INDEX_NAME=" + Literal(i.index_name));
			if (!idx_rows.empty()) {
				if (i.conditional && idx_rows.size() == 1 && idx_rows[0][0] == i.name) {
					source_noop = true;
				} else {
					RejectDDL("42P07", "source_definition", "index name already exists in schema");
				}
			}
			if (!source_noop) {
				vector<string> cols;
				for (const auto &k : i.keys) {
					auto cr = col_row(i.name, k);
					if (cr.empty()) {
						RejectDDL("42703", "source_definition", "index column does not exist");
					}
					if (cr[0][0] == "CLOB" || cr[0][0] == "NCLOB" || cr[0][0] == "BLOB") {
						RejectDDL("0A000", "source_definition", "cannot index LOB column on HANA");
					}
					cols.push_back(Identifier(k));
				}
				native_sql = string(i.action == "create_unique_index" ? "CREATE UNIQUE INDEX " : "CREATE INDEX ") +
				             Identifier(i.schema) + "." + Identifier(i.index_name) + " ON " + target + " (" +
				             StringUtil::Join(cols, ",") + ")";
				add_lock_table(i.name);
			}
		} else if (i.action == "drop_index") {
			auto idx_rows = s.Rows("SELECT TABLE_NAME, COALESCE(CONSTRAINT,'') FROM SYS.INDEXES WHERE SCHEMA_NAME=" +
			                       Literal(i.schema) + " AND INDEX_NAME=" + Literal(i.name));
			if (idx_rows.empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42704", "source_definition", "index does not exist");
				}
			} else if (!i.owner.empty() && idx_rows[0][0] != i.owner) {
				RejectDDL("42704", "source_definition", "index owner mismatch");
			} else {
				if (idx_rows[0][1] == "PRIMARY KEY") {
					RejectDDL("2BP01", "source_definition", "cannot drop constraint backing index directly");
				}
				if ((idx_rows[0][1] == "UNIQUE" || idx_rows[0][1] == "NOT NULL UNIQUE") &&
				    s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE REFERENCED_SCHEMA_NAME=" +
				             Literal(i.schema) + " AND REFERENCED_TABLE_NAME=" + Literal(idx_rows[0][0]) +
				             " AND REFERENCED_CONSTRAINT_NAME=" + Literal(i.name)) != "0") {
					RejectDDL("2BP01", "source_definition",
					          "cannot drop constraint backing index with referencing foreign keys");
				}
				native_sql = "DROP INDEX " + target;
				add_lock_table(idx_rows[0][0]);
			}
		} else if (i.action == "rename_index") {
			if (i.new_name.empty() || i.new_name == i.name) {
				RejectDDL("0A000", "source_definition", "invalid rename_index payload");
			}
			auto idx_rows = s.Rows("SELECT TABLE_NAME FROM SYS.INDEXES WHERE SCHEMA_NAME=" + Literal(i.schema) +
			                       " AND INDEX_NAME=" + Literal(i.name));
			if (idx_rows.empty()) {
				RejectDDL("42704", "source_definition", "index does not exist");
			}
			if (!i.owner.empty() && idx_rows[0][0] != i.owner) {
				RejectDDL("42704", "source_definition", "index owner mismatch");
			}
			if (index_exists(i.new_name)) {
				RejectDDL("42P07", "source_definition", "destination index name already exists in schema");
			}
			// HANA RENAME INDEX requires an unqualified destination index name.
			native_sql = "RENAME INDEX " + target + " TO " + Identifier(i.new_name);
			add_lock_table(idx_rows[0][0]);
		} else if (i.action == "truncate") {
			if (i.identity_mode == "restart") {
				RejectDDL("0A000", "source_definition", "HANA TRUNCATE RESTART IDENTITY is unsupported");
			}
			check_base_table(i.name);
			if (s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE (REFERENCED_SCHEMA_NAME=" +
			             Literal(i.schema) + " AND REFERENCED_TABLE_NAME=" + Literal(i.name) +
			             ") OR (SCHEMA_NAME=" + Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name) + ")") != "0") {
				RejectDDL("2BP01", "source_definition", "cannot truncate table with foreign key constraints");
			}
			native_sql = "TRUNCATE TABLE " + target;
			add_lock_table(i.name);
		} else {
			RejectDDL("0A000", "source_definition", "unsupported HANA DDL action");
		}

		if (!no_op_commit && !source_noop && !lock_tables.empty()) {
			std::sort(lock_tables.begin(), lock_tables.end());
			stage = "source_lock";
			s.current_stage = "source_lock";
			for (const auto &tbl : lock_tables) {
				if (ctx.IsInterrupted()) {
					RejectDDL("57014", "source_lock", "operation cancelled");
				}
				s.Exec("LOCK TABLE " + Identifier(i.schema) + "." + Identifier(tbl) + " IN EXCLUSIVE MODE NOWAIT");
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
		src.product = "hana";
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
			if (session && IsHanaConfirmedNotExecuted(session->last_native, session->last_message)) {
				status = "CONFIRMED_NOT_EXECUTED";
				if (IsHanaLockTimeout(session->last_native, session->last_message)) {
					stage = "source_lock";
				}
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
			status = "UNKNOWN";
		}
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

	if (session) {
		try {
			session->Exec("ROLLBACK", true);
		} catch (...) {
		}
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

void RegisterHanaGuardedDDL(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("hana_ddl_capabilities_v1", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      DDLMetadataScan, DDLMetadataBind, DDLInit));
	loader.RegisterFunction(TableFunction(
	    "hana_ddl_describe_v1",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DDLMetadataScan,
	    DDLMetadataBind, DDLInit));
	TableFunction exec("hana_guarded_ddl_v1",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                    LogicalType::ANY},
	                   DDLExecScan, DDLExecBind, DDLInit);
	exec.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec);
}
} // namespace duckdb
