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
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_set>
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
static bool IsSafeDamengDefault(const string &raw) {
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
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "dameng");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLCHAR message[1] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, message, sizeof(message), &len);

		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
		} else {
			switch (native) {
			case -6602:
				error = "23505";
				break;
			case -6609:
				error = "23502";
				break;
			case -6605:
				error = "23514";
				break;
			case -6606:
			case -6607:
				error = "23503";
				break;
			case -6102:
			case -6108:
			case -6111:
			case -6116:
			case -6119:
				error = "22003";
				break;
			case -6169:
			case -6170:
				error = "22001";
				break;
			case -6112:
			case -6114:
			case -6118:
			case -6121:
				error = "22007";
				break;
			case -6105:
				error = "22P02";
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
		vector<string> texts;
		vector<string> blobs;
		vector<SQLLEN> lengths;
		char empty_blob_sentinel = 0;
		explicit BoundParams(idx_t n)
		    : values(n, 0), bits(n, 0), floats(n, 0.0f), doubles(n, 0.0), texts(n), blobs(n), lengths(n, 0) {
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
					b.texts[i] = v.GetValue<string>();
					if (b.texts[i].size() > 1048576 || b.texts[i].find('\0') != string::npos)
						Reject();
					b.lengths[i] = static_cast<SQLLEN>(b.texts[i].size());
					SQLSMALLINT sql_type = b.texts[i].size() > 8000 ? SQL_LONGVARCHAR : SQL_VARCHAR;
					SQLULEN col_size = MaxValue<SQLULEN>(1, b.texts[i].size());
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, sql_type, col_size, 0,
					                       const_cast<char *>(b.texts[i].c_str()), b.texts[i].size() + 1,
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
					SQLSMALLINT sql_type = b.blobs[i].size() > 8000 ? SQL_LONGVARBINARY : SQL_VARBINARY;
					SQLULEN col_size = MaxValue<SQLULEN>(1, b.blobs[i].size());
					SQLPOINTER ptr = b.blobs[i].empty()
					                     ? static_cast<SQLPOINTER>(&b.empty_blob_sentinel)
					                     : static_cast<SQLPOINTER>(const_cast<char *>(b.blobs[i].data()));
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, sql_type, col_size, 0, ptr,
					                       b.blobs[i].size(), &b.lengths[i]));
				}
				continue;
			case LogicalTypeId::TINYINT:
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
		if (count != static_cast<SQLSMALLINT>(params.size() + 1) || count > 2100)
			Reject();
		BoundParams b(params.size());
		BindPreparedInputParams(params, b);
		char out_buf[128] = {0};
		SQLLEN out_ind = 0;
		Check(SQLBindParameter(stmt.get(), static_cast<SQLUSMALLINT>(params.size() + 1), SQL_PARAM_OUTPUT, SQL_C_CHAR,
		                       SQL_VARCHAR, 128, 0, out_buf, sizeof(out_buf), &out_ind));

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
		if (cols != 0)
			Reject();
		SQLLEN first, second;
		std::memset(&first, 0xa5, sizeof(first));
		std::memset(&second, 0x5a, sizeof(second));
		Check(SQLRowCount(stmt.get(), &first));
		Check(SQLRowCount(stmt.get(), &second));
		int64_t affected = -1;
		if (!DecodeGuardedODBCRowCount(first, second, affected) || affected != 1)
			Reject();
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		if (out_ind == SQL_NULL_DATA) {
			returned_rows = "[[null]]";
			return 1;
		}
		if (out_ind <= 0 || out_ind >= static_cast<SQLLEN>(sizeof(out_buf)))
			Reject();
		string val(out_buf, static_cast<size_t>(out_ind));
		while (!val.empty() && val.back() == '\0')
			val.pop_back();
		StringUtil::Trim(val);
		idx_t pos = (!val.empty() && (val[0] == '+' || val[0] == '-')) ? 1 : 0;
		if (pos >= val.size() || val.size() - pos > 20 ||
		    val.find_first_not_of("0123456789", pos) != string::npos)
			Reject();
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
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, name);
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != ODBCDialect::DAMENG)
		Reject();
	return catalog;
}
static unique_ptr<FunctionData> BindInternal(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                             vector<string> &names, bool is_returning) {
	for (auto &v : in.inputs)
		if (v.IsNull())
			Reject();
	auto d = make_uniq<Data>(CatalogFor(ctx, in.inputs[0].GetValue<string>()), in.inputs.size() == 6);
	d->returning = is_returning;
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
                                     vector<string> &names) {
	return BindInternal(ctx, in, types, names, false);
}
static unique_ptr<FunctionData> BindReturning(ClientContext &ctx, TableFunctionBindInput &in,
                                              vector<LogicalType> &types, vector<string> &names) {
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
static string NormalizedDamengType(const string &raw_type) {
	if (StringUtil::StartsWith(raw_type, "TIME(") && StringUtil::EndsWith(raw_type, ")"))
		return "TIME";
	if (StringUtil::StartsWith(raw_type, "TIMESTAMP(")) {
		if (StringUtil::EndsWith(raw_type, ") WITH TIME ZONE"))
			return "TIMESTAMP WITH TIME ZONE";
		if (StringUtil::EndsWith(raw_type, ") WITH LOCAL TIME ZONE"))
			return "TIMESTAMP WITH LOCAL TIME ZONE";
		if (StringUtil::EndsWith(raw_type, ")"))
			return "TIMESTAMP";
	}
	if (StringUtil::StartsWith(raw_type, "DATETIME(")) {
		if (StringUtil::EndsWith(raw_type, ") WITH TIME ZONE"))
			return "DATETIME WITH TIME ZONE";
		if (StringUtil::EndsWith(raw_type, ") WITH LOCAL TIME ZONE"))
			return "DATETIME WITH LOCAL TIME ZONE";
		if (StringUtil::EndsWith(raw_type, ")"))
			return "DATETIME";
	}
	return raw_type;
}
static bool IsValidUint(const string &v) {
	return !v.empty() && v.find_first_not_of("0123456789") == string::npos;
}
static string Metadata(Session &s, const Data &d, string &stage, std::unordered_set<string> *column_names = nullptr) {
	const auto filter = "OWNER=" + Literal(d.schema) + " AND TABLE_NAME=" + Literal(d.table);
	stage = "source_metadata";
	auto identity = s.Rows("SELECT NAME,USER FROM V$DATABASE");
	if (identity.size() != 1 || identity[0].size() != 2 || identity[0][0].empty() ||
	    (identity[0][1] != d.schema && identity[0][1] != "SYSDBA"))
		Reject();
	auto modes = s.Rows("SELECT PARA_NAME,PARA_VALUE FROM V$DM_INI WHERE PARA_NAME='COMPATIBLE_MODE'");
	if (modes.size() != 1 || modes[0][1] != "0")
		Reject();
	auto unicode = s.Scalar("SELECT UNICODE()");
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
	auto cols = s.Rows("SELECT COLUMN_NAME,DATA_TYPE,COALESCE(DATA_TYPE_OWNER,'_'),COALESCE(DATA_LENGTH,0),"
	                   "COALESCE(DATA_SCALE,-1),NULLABLE,COLUMN_ID,COALESCE(DEFAULT_LENGTH,0),COALESCE(DATA_DEFAULT,'_'),"
	                   "COALESCE(HIDDEN_COLUMN,'_'),COALESCE(VIRTUAL_COLUMN,'_'),COALESCE(DATA_PRECISION,-1),"
	                   "COALESCE(CHAR_LENGTH,0),COALESCE(CHAR_USED,'_'),"
	                   "CASE WHEN DATA_DEFAULT IS NULL THEN '0' ELSE '1' END FROM DBA_TAB_COLS WHERE " +
	                   filter + " ORDER BY COLUMN_ID");
	auto raw = s.Rows("SELECT NAME,COLID,TYPE$,COALESCE(LENGTH$,0),COALESCE(SCALE,-1),NULLABLE$,COALESCE(DEFVAL,'_'),"
	                  "INFO1,INFO2 FROM SYS.SYSCOLUMNS WHERE ID=" +
	                  object[0][0] + " ORDER BY COLID");
	if (cols.empty() || cols.size() > 1024 || cols.size() != raw.size())
		Reject();
	for (idx_t i = 0; i < cols.size(); i++) {
		auto &c = cols[i];
		auto &r = raw[i];
		if (!SimpleName(c[0]) || c[1].empty() || (c[5] != "Y" && c[5] != "N") || c[6] != std::to_string(i + 1) ||
		    c[9] != "NO" || c[10] != "NO" || (c[14] != "0" && c[14] != "1") || r[0] != c[0] ||
		    r[1] != std::to_string(i) || r[5] != c[5] || r[7] != "0" || (r[8] != "0" && r[8] != "1"))
			Reject();
		bool is_identity = (r[8] == "1");
		bool has_default = (c[14] == "1");
		if (is_identity && has_default)
			Reject();
	}
	stage = "source_constraints";
	auto constraints = s.Rows("SELECT CONSTRAINT_NAME,CONSTRAINT_TYPE,STATUS,DEFERRABLE,DEFERRED,VALIDATED,"
	                          "COALESCE(SEARCH_CONDITION,'_') FROM DBA_CONSTRAINTS WHERE " +
	                          filter + " ORDER BY CONSTRAINT_NAME");
	std::unordered_set<string> pk_names;
	for (auto &c : constraints) {
		if ((c[1] != "P" && c[1] != "U") || c[2] != "ENABLED" || c[3] != "NOT DEFERRABLE" || c[4] != "IMMEDIATE" ||
		    c[5] != "VALIDATED" || c[6] != "_")
			Reject();
		if (c[1] == "P")
			pk_names.insert(c[0]);
	}
	auto constraint_cols = s.Rows("SELECT CONSTRAINT_NAME,COLUMN_NAME,POSITION FROM DBA_CONS_COLUMNS WHERE " + filter +
	                              " ORDER BY CONSTRAINT_NAME,POSITION");
	std::unordered_set<string> single_pk_cols;
	for (const auto &pk_name : pk_names) {
		vector<string> pk_cols;
		for (const auto &cc : constraint_cols) {
			if (cc[0] == pk_name)
				pk_cols.push_back(cc[1]);
		}
		if (pk_cols.size() == 1)
			single_pk_cols.insert(pk_cols[0]);
	}
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
	string revision = JSONRows(identity) + JSONRows(modes) + JSON(unicode) + JSONRows(table) + JSONRows(object) +
	                  JSONRows(cols) + JSONRows(raw) + JSONRows(constraints) + JSONRows(constraint_cols) +
	                  JSONRows(indexes) + JSONRows(index_cols) + JSONRows(index_objects);
	string result = "{\"database\":" + JSON(identity[0][0]) + ",\"schema\":" + JSON(d.schema) +
	                ",\"table\":" + JSON(d.table) +
	                ",\"object_id\":" + JSON(object[0][0] + ":" + object[0][1] + ":" + object[0][5]) +
	                ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	bool first = true;
	string charset = unicode == "1" ? "UTF-8" : "NON_UTF8";
	for (idx_t i = 0; i < cols.size(); i++) {
		auto &c = cols[i];
		auto &r = raw[i];
		if (column_names)
			column_names->insert(c[0]);
		if (!first)
			result += ",";
		first = false;
		auto dtype = c[2] == "_" ? NormalizedDamengType(c[1]) : (c[2] + "." + c[1]);
		result += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(dtype) +
		          ",\"nullable\":" + (c[5] == "Y" ? "true" : "false");
		if (r[8] == "1")
			result += ",\"is_identity\":true,\"identity_always\":true";
		if (c[14] == "1") {
			result += ",\"has_default\":true";
			if (IsSafeDamengDefault(c[8]))
				result += ",\"default_safe\":true";
		}
		if (single_pk_cols.count(c[0]))
			result += ",\"single_column_primary_key\":true";
		if ((dtype == "DECIMAL" || dtype == "NUMERIC" || dtype == "DEC" || dtype == "NUMBER") && IsValidUint(c[11]) &&
		    IsValidUint(c[4])) {
			result += ",\"precision\":" + c[11] + ",\"scale\":" + c[4];
		} else if ((dtype == "TIME" || dtype == "DATETIME" || dtype == "TIMESTAMP" ||
		            dtype == "TIMESTAMP WITH TIME ZONE" || dtype == "DATETIME WITH TIME ZONE") &&
		           IsValidUint(c[4])) {
			result += ",\"scale\":" + c[4];
		} else if (dtype == "BLOB" && IsValidUint(c[4]) && c[4] != "0") {
			// Dameng JSON columns share DATA_TYPE='BLOB' with positive DATA_SCALE; expose scale so Go treats JSON as opaque.
			result += ",\"scale\":" + c[4];
		}
		if ((dtype == "CHAR" || dtype == "VARCHAR" || dtype == "VARCHAR2" || dtype == "TEXT" || dtype == "CLOB" ||
		     dtype == "BINARY" || dtype == "VARBINARY" || dtype == "BLOB" || dtype == "FLOAT" || dtype == "REAL" ||
		     dtype == "DOUBLE" || dtype == "DOUBLE PRECISION") &&
		    IsValidUint(c[3])) {
			result += ",\"max_length\":" + c[3];
		}
		if (dtype == "CHAR" || dtype == "VARCHAR" || dtype == "VARCHAR2" || dtype == "TEXT" || dtype == "CLOB") {
			result += ",\"charset\":" + JSON(charset);
			if ((dtype == "CHAR" || dtype == "VARCHAR" || dtype == "VARCHAR2") && c[13] != "_" && IsValidUint(c[12])) {
				result += ",\"char_length\":" + c[12] + ",\"char_used\":" + JSON(c[13]);
			}
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
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, ODBCDialect::DAMENG), d.catalog.connection_string, 5);
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
	out.SetCardinality(1);
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
}
} // namespace
void RegisterDamengGuardedCRUD(ExtensionLoader &loader) {
	TableFunction metadata("dameng_crud_metadata_v1", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(metadata);
	TableFunction exec("dameng_guarded_mutation_v1", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.named_parameters["params"] = LogicalType::ANY;
	exec.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec);
	TableFunction exec_returning("dameng_guarded_insert_returning_v1", vector<LogicalType>(6, LogicalType::VARCHAR),
	                             Scan, BindReturning, Init);
	exec_returning.named_parameters["params"] = LogicalType::ANY;
	exec_returning.named_parameters["returning_name"] = LogicalType::VARCHAR;
	exec_returning.named_parameters["returning_oid"] = LogicalType::BIGINT;
	exec_returning.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec_returning);
}
} // namespace duckdb
