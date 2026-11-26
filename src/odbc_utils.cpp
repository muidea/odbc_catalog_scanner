#include "guarded_odbc_source_error.hpp"
#include "odbc_temporal_utils.hpp"
#include "odbc_utils.hpp"
#include "odbc_handle_guard.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <set>
#include <tuple>

#ifndef _WIN32
#include <unistd.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_transaction.hpp"

namespace duckdb {

#ifndef _WIN32
static std::mutex odbc_ini_mutex;
#endif

static bool ParseValue(const string &dsn, idx_t &pos, string &result) {
	// skip leading spaces
	while (pos < dsn.size() && StringUtil::CharacterIsSpace(dsn[pos])) {
		pos++;
	}
	if (pos >= dsn.size()) {
		return false;
	}
	// check if we are parsing a quoted value or not
	if (dsn[pos] == '"') {
		pos++;
		// scan until we find another quote
		bool found_quote = false;
		for (; pos < dsn.size(); pos++) {
			if (dsn[pos] == '"') {
				found_quote = true;
				pos++;
				break;
			}
			if (dsn[pos] == '\\') {
				// backslash escapes the backslash or double-quote
				if (pos + 1 >= dsn.size()) {
					throw InvalidInputException("Invalid dsn \"%s\" - backslash at end of dsn", dsn);
				}
				if (dsn[pos + 1] != '\\' && dsn[pos + 1] != '"') {
					throw InvalidInputException("Invalid dsn \"%s\" - backslash can only escape \\ or \"", dsn);
				}
				result += dsn[pos + 1];
				pos++;
			} else {
				result += dsn[pos];
			}
		}
		if (!found_quote) {
			throw InvalidInputException("Invalid dsn \"%s\" - unterminated quote", dsn);
		}
	} else {
		// unquoted value, continue until space, equality sign or end of string
		for (; pos < dsn.size(); pos++) {
			if (dsn[pos] == '=') {
				break;
			}
			if (StringUtil::CharacterIsSpace(dsn[pos])) {
				break;
			}
			result += dsn[pos];
		}
	}
	return true;
}

static bool ParseBoolValue(const string &value) {
	return value != "0" && !StringUtil::CIEquals(value, "false");
}

bool ReadOptionFromEnv(const char *env, string &result) {
	auto res = std::getenv(env);
	if (!res) {
		return false;
	}
	result = res;
	return true;
}

uint32_t ParsePort(const string &value) {
	constexpr const static int PORT_MIN = 0;
	constexpr const static int PORT_MAX = 65535;
	int port_val = std::stoi(value);
	if (port_val < PORT_MIN || port_val > PORT_MAX) {
		throw InvalidInputException("Invalid port %d - port must be between %d and %d", port_val, PORT_MIN, PORT_MAX);
	}
	return uint32_t(port_val);
}

string EscapeODBCConnectionValue(const string &value) {
	if (value.empty()) {
		return value;
	}
	if (value.find_first_of(";{}") == string::npos) {
		return value;
	}
	string result = "{";
	for (auto c : value) {
		if (c == '}') {
			result += "}}";
		} else {
			result += c;
		}
	}
	result += "}";
	return result;
}

#ifndef _WIN32
struct TemporaryODBCIniGuard {
	explicit TemporaryODBCIniGuard(const char *path_p) : path(path_p) {
	}
	~TemporaryODBCIniGuard() {
		if (path) {
			std::remove(path);
		}
	}
	TemporaryODBCIniGuard(const TemporaryODBCIniGuard &) = delete;
	TemporaryODBCIniGuard &operator=(const TemporaryODBCIniGuard &) = delete;
	const char *path;
};

struct ODBCIniEnvironmentGuard {
	explicit ODBCIniEnvironmentGuard(const string &path) {
		auto previous = std::getenv("ODBCINI");
		had_previous = previous != nullptr;
		if (previous) {
			previous_value = previous;
		}
		if (setenv("ODBCINI", path.c_str(), 1) != 0) {
			throw IOException("Failed to set temporary ODBC configuration");
		}
	}
	~ODBCIniEnvironmentGuard() {
		if (had_previous) {
			setenv("ODBCINI", previous_value.c_str(), 1);
		} else {
			unsetenv("ODBCINI");
		}
	}
	ODBCIniEnvironmentGuard(const ODBCIniEnvironmentGuard &) = delete;
	ODBCIniEnvironmentGuard &operator=(const ODBCIniEnvironmentGuard &) = delete;
	bool had_previous;
	string previous_value;
};

bool HasExtraParam(const vector<string> &extra_params, const string &name) {
	auto lower_name = StringUtil::Lower(name);
	for (auto &extra : extra_params) {
		auto separator = extra.find('=');
		auto key = separator == string::npos ? extra : extra.substr(0, separator);
		if (StringUtil::Lower(key) == lower_name) {
			return true;
		}
	}
	return false;
}

void WriteODBCIniValue(std::ofstream &file, const string &key, const string &value) {
	if (value.find_first_of("\r\n") != string::npos) {
		throw InvalidInputException("Invalid ODBC connection value for \"%s\" - newlines are not supported", key);
	}
	file << key << "=" << value << "\n";
}

string CreateTemporaryODBCIni(const ODBCConnectionParameters &config, string &dsn_name) {
	char path_template[] = "/tmp/duckdb_odbc_XXXXXX";
	auto fd = mkstemp(path_template);
	if (fd < 0) {
		throw IOException("Failed to create temporary ODBC ini file");
	}
	TemporaryODBCIniGuard cleanup(path_template);
	close(fd);

	string path = path_template;
	auto slash = path.find_last_of('/');
	dsn_name = slash == string::npos ? path : path.substr(slash + 1);

	std::ofstream file(path);
	if (!file.is_open()) {
		std::remove(path.c_str());
		throw IOException("Failed to open temporary ODBC ini file \"%s\"", path);
	}

	file << "[" << dsn_name << "]\n";
	WriteODBCIniValue(file, "Driver", config.driver);
	WriteODBCIniValue(file, "Server", config.server);
	if (config.port != 0) {
		WriteODBCIniValue(file, "Port", std::to_string(config.port));
	}
	if (!config.database.empty()) {
		WriteODBCIniValue(file, "Database", config.database);
	}
	for (auto &extra : config.extra_params) {
		auto separator = extra.find('=');
		if (separator == string::npos) {
			continue;
		}
		WriteODBCIniValue(file, extra.substr(0, separator), extra.substr(separator + 1));
	}
	if (!HasExtraParam(config.extra_params, "ClientCharset")) {
		WriteODBCIniValue(file, "ClientCharset", "UTF-8");
	}
	if (!HasExtraParam(config.extra_params, "TDS_Version")) {
		WriteODBCIniValue(file, "TDS_Version", "8.0");
	}

	file.close();
	if (!file.good()) {
		std::remove(path.c_str());
		throw IOException("Failed to write temporary ODBC ini file \"%s\"", path);
	}
	cleanup.path = nullptr;
	return path;
}

bool ShouldUseSQLConnectPasswordFallback(const ODBCConnectionParameters &config) {
	return !config.driver.empty() && !config.server.empty() && !config.uid.empty() && !config.pwd.empty() &&
	       config.pwd.find('}') != string::npos;
}

SQLRETURN SQLConnectWithTemporaryDSN(SQLHDBC dbc, const ODBCConnectionParameters &config) {
	string dsn_name;
	auto odbc_ini_path = CreateTemporaryODBCIni(config, dsn_name);

	TemporaryODBCIniGuard cleanup(odbc_ini_path.c_str());
	lock_guard<std::mutex> lock(odbc_ini_mutex);
	ODBCIniEnvironmentGuard environment(odbc_ini_path);
	return SQLConnect(dbc, (SQLCHAR *)dsn_name.c_str(), SQL_NTS, (SQLCHAR *)config.uid.c_str(), SQL_NTS,
	                  (SQLCHAR *)config.pwd.c_str(), SQL_NTS);
}
#endif

std::tuple<ODBCConnectionParameters, unordered_set<string>> ODBCUtils::ParseConnectionParameters(const string &dsn) {
	ODBCConnectionParameters result;

	unordered_set<string> set_options;
	// parse options from ODBC connection string format: "key1=value1;key2=value2;"
	idx_t pos = 0;
	while (pos < dsn.size()) {
		string key;
		string value;

		// Parse key
		while (pos < dsn.size() && dsn[pos] != '=' && dsn[pos] != ';') {
			key += dsn[pos];
			pos++;
		}

		if (pos >= dsn.size() || dsn[pos] != '=') {
			throw InvalidInputException(
			    "Invalid ODBC catalog connection string - expected key=value pairs separated by "
			    "semicolons");
		}
		pos++; // skip '='

		// Parse value
		if (pos < dsn.size() && dsn[pos] == '{') {
			pos++; // skip '{'
			bool found_closing_brace = false;
			while (pos < dsn.size()) {
				if (dsn[pos] == '}') {
					if (pos + 1 < dsn.size() && dsn[pos + 1] == '}') {
						value += '}';
						pos += 2;
						continue;
					}
					pos++; // skip closing '}'
					found_closing_brace = true;
					break;
				}
				value += dsn[pos];
				pos++;
			}
			if (!found_closing_brace) {
				throw InvalidInputException("Invalid ODBC catalog connection string - unterminated braced value");
			}
			if (pos < dsn.size() && dsn[pos] != ';') {
				throw InvalidInputException(
				    "Invalid ODBC catalog connection string - expected semicolon after braced value");
			}
		} else {
			while (pos < dsn.size() && dsn[pos] != ';') {
				value += dsn[pos];
				pos++;
			}
		}

		if (pos < dsn.size() && dsn[pos] == ';') {
			pos++; // skip ';'
		}

		auto original_key = key;
		key = StringUtil::Lower(key);

		if (set_options.find(key) != set_options.end()) {
			throw InvalidInputException("Duplicate '%s' parameter in connection string. Each parameter "
			                            "should only be specified once.",
			                            key);
		}

		// Handle standard ODBC connection parameters
		if (key == "driver") {
			set_options.insert("driver");
			result.driver = value;
		} else if (key == "server") {
			set_options.insert("server");
			result.server = value;
		} else if (key == "database") {
			set_options.insert("database");
			result.database = value;
		} else if (key == "uid") {
			set_options.insert("uid");
			result.uid = value;
		} else if (key == "pwd") {
			set_options.insert("pwd");
			result.pwd = value;
		} else if (key == "port") {
			set_options.insert("port");
			result.port = ParsePort(value);
		} else {
			// Preserve driver-specific parameters (like TDS_Version) verbatim
			result.extra_params.push_back(original_key + "=" + value);
			continue;
		}
	}

	// For ODBC, connection parameters are specified in the connection string
	return std::make_tuple(result, set_options);
}

string ODBCUtils::BuildConnectionString(const string &dsn) {
	ODBCConnectionParameters config;
	unordered_set<string> unused_params;
	std::tie(config, unused_params) = ParseConnectionParameters(dsn);

	string connection_string;
	if (!config.driver.empty()) {
		connection_string += "DRIVER=" + EscapeODBCConnectionValue(config.driver) + ";";
	}
	if (!config.server.empty()) {
		connection_string += "SERVER=" + EscapeODBCConnectionValue(config.server) + ";";
	}
	if (!config.uid.empty()) {
		connection_string += "UID=" + EscapeODBCConnectionValue(config.uid) + ";";
	}
	if (!config.pwd.empty()) {
		connection_string += "PWD=" + EscapeODBCConnectionValue(config.pwd) + ";";
	}
	if (!config.database.empty()) {
		connection_string += "DATABASE=" + EscapeODBCConnectionValue(config.database) + ";";
	}
	if (config.port != 0) {
		connection_string += "PORT=" + std::to_string(config.port) + ";";
	}

	// Append driver-specific / unrecognized parameters verbatim
	for (auto &extra : config.extra_params) {
		auto separator = extra.find('=');
		if (separator == string::npos) {
			connection_string += extra + ";";
			continue;
		}
		connection_string +=
		    extra.substr(0, separator + 1) + EscapeODBCConnectionValue(extra.substr(separator + 1)) + ";";
	}

#ifndef _WIN32
	auto lower_driver = StringUtil::Lower(config.driver);
	if (StringUtil::Contains(lower_driver, "freetds")) {
		if (StringUtil::Lower(connection_string).find("clientcharset") == string::npos) {
			connection_string += "ClientCharset=UTF-8;";
		}
		if (StringUtil::Lower(connection_string).find("tds_version") == string::npos) {
			connection_string += "TDS_Version=8.0;";
		}
	}
#endif

	return connection_string;
}

SQLHDBC ODBCUtils::Connect(const string &dsn, SQLHENV &out_env, uint32_t login_timeout) {
	ODBCConnectGuard handles;
	auto &env = handles.environment;
	auto &dbc = handles.connection;
	SQLRETURN ret;

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (!SQL_SUCCEEDED(ret)) {
		throw IOException("Failed to allocate ODBC environment handle");
	}

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
	if (!SQL_SUCCEEDED(ret)) {
		throw IOException("Failed to set ODBC version");
	}

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	if (!SQL_SUCCEEDED(ret)) {
		throw IOException("Failed to allocate ODBC connection handle");
	}

	if (login_timeout &&
	    SQLSetConnectAttr(dbc, SQL_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(login_timeout)),
	                      0) != SQL_SUCCESS) {
		throw IOException("ODBC driver does not support the guarded login timeout");
	}
	ret = SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_ONLY), 0);
	if (!SQL_SUCCEEDED(ret)) {
		throw IOException("ODBC driver does not support read-only connections");
	}

	ODBCConnectionParameters config;
	unordered_set<string> unused_params;
	std::tie(config, unused_params) = ParseConnectionParameters(dsn);

#ifndef _WIN32
	if (ShouldUseSQLConnectPasswordFallback(config)) {
		ret = SQLConnectWithTemporaryDSN(dbc, config);
	} else
#endif
	{
		string connection_string = BuildConnectionString(dsn);

		SQLCHAR outstr[1024];
		SQLSMALLINT outstrlen;
		ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)connection_string.c_str(), SQL_NTS, outstr, sizeof(outstr),
		                       &outstrlen, SQL_DRIVER_NOPROMPT);
	}

	if (!SQL_SUCCEEDED(ret)) {
		auto source = ReadGuardedODBCSourceError(SQL_HANDLE_DBC, dbc, "");
		if (!source.message.empty())
			throw GuardedODBCConnectionFailure(std::move(source));
		throw IOException("ODBC connection failed without a source diagnostic");
	}

	return handles.Release(out_env);
}

static string ReadCatalogString(SQLHSTMT statement, SQLUSMALLINT column) {
	vector<char> buffer(256);
	string result;
	SQLLEN indicator = 0;
	while (true) {
		auto status = SQLGetData(statement, column, SQL_C_CHAR, buffer.data(), buffer.size(), &indicator);
		if (status == SQL_NO_DATA || indicator == SQL_NULL_DATA) {
			break;
		}
		if (!SQL_SUCCEEDED(status)) {
			throw IOException("Failed to read ODBC catalog metadata");
		}
		result.append(buffer.data());
		if (status == SQL_SUCCESS) {
			break;
		}
	}
	result.erase(std::remove(result.begin(), result.end(), '\0'), result.end());
	StringUtil::Trim(result);
	return result;
}

string ODBCUtils::CatalogPattern(SQLHDBC connection, const string &identifier) {
	SQLCHAR escape[16] = {0};
	SQLSMALLINT length = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(connection, SQL_SEARCH_PATTERN_ESCAPE, escape, sizeof(escape), &length)) ||
	    length < 0 || length >= static_cast<SQLSMALLINT>(sizeof(escape))) {
		throw IOException("ODBC catalog cannot determine identifier pattern escaping");
	}
	string result;
	for (auto c : identifier) {
		if (c == '%' || c == '_' || (length == 1 && c == static_cast<char>(escape[0]))) {
			if (length != 1)
				throw IOException("ODBC catalog cannot match identifiers containing pattern characters");
			result += static_cast<char>(escape[0]);
		}
		result += c;
	}
	return result;
}

vector<ODBCCatalogObject> ODBCUtils::GetCatalogObjects(SQLHDBC connection, const string &schema, const string &table) {
	SQLHSTMT raw_statement = SQL_NULL_HSTMT;
	if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection, &raw_statement))) {
		throw IOException("Failed to allocate ODBC catalog statement");
	}
	ODBCStatementPtr statement(raw_statement, ODBCStatementDelete);
	auto schema_pattern = schema.empty() ? string() : CatalogPattern(connection, schema);
	auto table_pattern = table.empty() ? string("%") : CatalogPattern(connection, table);
	auto schema_ptr = schema.empty() ? nullptr : reinterpret_cast<SQLCHAR *>(&schema_pattern[0]);
	auto table_ptr = reinterpret_cast<SQLCHAR *>(&table_pattern[0]);
	auto types = reinterpret_cast<SQLCHAR *>(const_cast<char *>("'TABLE','VIEW'"));
	auto status = SQLTables(raw_statement, nullptr, 0, schema_ptr, schema.empty() ? 0 : SQL_NTS, table_ptr, SQL_NTS,
	                        types, SQL_NTS);
	if (!SQL_SUCCEEDED(status)) {
		throw IOException("ODBC driver failed to enumerate tables and views");
	}

	vector<ODBCCatalogObject> result;
	std::set<std::pair<string, string>> identities;
	while (SQL_SUCCEEDED(status = SQLFetch(raw_statement))) {
		ODBCCatalogObject object;
		object.schema = ReadCatalogString(raw_statement, 2);
		object.name = ReadCatalogString(raw_statement, 3);
		object.type = ReadCatalogString(raw_statement, 4);
		StringUtil::Trim(object.type);
		if ((!schema.empty() && object.schema != schema) || (!table.empty() && object.name != table) ||
		    (object.type != "TABLE" && object.type != "VIEW")) {
			throw IOException("ODBC catalog returned an unexpected table identity");
		}
		if (!object.schema.empty() && !object.name.empty()) {
			if (!identities.emplace(object.schema, object.name).second) {
				throw IOException("ODBC catalog returned ambiguous table identity");
			}
			result.push_back(std::move(object));
		}
	}
	if (status != SQL_NO_DATA) {
		throw IOException("ODBC driver failed while reading table metadata");
	}
	return result;
}

string ODBCUtils::EscapeQuotes(const string &text, char quote) {
	string result;
	for (auto c : text) {
		if (c == quote) {
			result += string(2, quote);
		} else {
			result += c;
		}
	}
	return result;
}

string ODBCUtils::WriteQuoted(const string &text, char quote) {
	return string(1, quote) + EscapeQuotes(text, quote) + string(1, quote);
}

string ODBCUtils::WriteIdentifier(const string &identifier) {
	return WriteQuoted(identifier, '"');
}

string ODBCUtils::WriteLiteral(const string &identifier) {
	return ODBCUtils::WriteQuoted(identifier, '\'');
}

static string TransformBlobToODBC(const string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	string result = "0x";
	for (idx_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	return result;
}

bool ODBCUtils::TryTransformConstant(const Value &val, ODBCDialect dialect, const string &remote_type_name, string &out) {
	if (val.IsNull()) {
		out.clear();
		return false;
	}
	if (val.type().id() == LogicalTypeId::BOOLEAN) {
		if (dialect == ODBCDialect::DAMENG) {
			out = BooleanValue::Get(val) ? "1" : "0";
		} else {
			out = val.ToSQLString();
		}
		return true;
	}
	if (val.type().id() == LogicalTypeId::FLOAT) {
		if (!std::isfinite(FloatValue::Get(val))) {
			out.clear();
			return false;
		}
		out = val.ToSQLString();
		return true;
	}
	if (val.type().id() == LogicalTypeId::DOUBLE) {
		if (!std::isfinite(DoubleValue::Get(val))) {
			out.clear();
			return false;
		}
		out = val.ToSQLString();
		return true;
	}
	if (val.type().IsNumeric()) {
		out = val.ToSQLString();
		return true;
	}
	if (val.type().id() == LogicalTypeId::BLOB) {
		out = TransformBlobToODBC(StringValue::Get(val));
		return true;
	}
	if (val.type().id() == LogicalTypeId::DATE) {
		date_t dd = DateValue::Get(val);
		if (!dd.IsFinite()) {
			out.clear();
			return false;
		}
		int32_t year, month, day;
		Date::Convert(dd, year, month, day);
		if (!IsODBCYearInRange(year)) {
			out.clear();
			return false;
		}
		out = "DATE " + WriteLiteral(FormatODBCDateParts(year, month, day));
		return true;
	}
	if (val.type().id() == LogicalTypeId::TIME) {
		dtime_t dt = TimeValue::Get(val);
		int32_t hour, minute, second, micros;
		Time::Convert(dt, hour, minute, second, micros);
		if (!IsODBCTimeInRange(hour, minute, second, micros)) {
			out.clear();
			return false;
		}
		if ((dialect == ODBCDialect::DB2 || dialect == ODBCDialect::HANA) && micros != 0) {
			out.clear();
			return false;
		}
		out = "TIME " + WriteLiteral(FormatODBCTimeParts(hour, minute, second, micros));
		return true;
	}
	if (val.type().id() == LogicalTypeId::TIMESTAMP) {
		timestamp_t ts = TimestampValue::Get(val);
		if (!ts.IsFinite()) {
			out.clear();
			return false;
		}
		date_t dd;
		dtime_t dt;
		Timestamp::Convert(ts, dd, dt);
		int32_t year, month, day, hour, minute, second, micros;
		Date::Convert(dd, year, month, day);
		if (!IsODBCYearInRange(year)) {
			out.clear();
			return false;
		}
		Time::Convert(dt, hour, minute, second, micros);
		if (NormalizeODBCTypeName(remote_type_name) == "seconddate" && micros != 0) {
			out.clear();
			return false;
		}
		out = "TIMESTAMP " +
		      WriteLiteral(FormatODBCDateParts(year, month, day) + " " + FormatODBCTimeParts(hour, minute, second, micros));
		return true;
	}
	if (val.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
		if (dialect != ODBCDialect::DAMENG) {
			out.clear();
			return false;
		}
		timestamp_t ts(val.GetValueUnsafe<int64_t>());
		if (!ts.IsFinite()) {
			out.clear();
			return false;
		}
		date_t dd;
		dtime_t dt;
		Timestamp::Convert(ts, dd, dt);
		int32_t year, month, day;
		Date::Convert(dd, year, month, day);
		if (!IsODBCYearInRange(year)) {
			out.clear();
			return false;
		}
		out = "TIMESTAMP " + WriteLiteral(FormatODBCTimestampTZUTC(timestamp_tz_t(val.GetValueUnsafe<int64_t>()), false));
		return true;
	}
	out = val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
	return true;
}

string ODBCUtils::TransformConstant(const Value &val) {
	return TransformConstant(val, ODBCDialect::DB2);
}

string ODBCUtils::TransformConstant(const Value &val, ODBCDialect dialect) {
	string out;
	if (!TryTransformConstant(val, dialect, "", out)) {
		throw InvalidInputException("Unsupported constant value for ODBC filter pushdown: %s", val.ToString());
	}
	return out;
}

} // namespace duckdb
