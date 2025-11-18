#include "odbc_utils.hpp"

#include <tuple>

#include <sql.h>
#include <sqlext.h>

#include "duckdb/common/string_util.hpp"

#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_transaction.hpp"

namespace duckdb {

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
	constexpr const static int PORT_MAX = 65353;
	int port_val = std::stoi(value);
	if (port_val < PORT_MIN || port_val > PORT_MAX) {
		throw InvalidInputException("Invalid port %d - port must be between %d and %d", port_val, PORT_MIN, PORT_MAX);
	}
	return uint32_t(port_val);
}

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
			throw InvalidInputException("Invalid connection string \"%s\" - expected key=value pairs separated by semicolons", dsn);
		}
		pos++; // skip '='
		
		// Parse value
		while (pos < dsn.size() && dsn[pos] != ';') {
			value += dsn[pos];
			pos++;
		}
		
		if (pos < dsn.size() && dsn[pos] == ';') {
			pos++; // skip ';'
		}
		
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
			// Ignore unrecognized parameters (like TDS_Version) as they are driver-specific
			continue;
		}
	}
	
	// For ODBC, connection parameters are specified in the connection string
	return std::make_tuple(result, set_options);
}

SQLHDBC ODBCUtils::Connect(const string &dsn) {
	SQLHENV env;
	SQLHDBC dbc;
	SQLRETURN ret;

	// Allocate environment handle
	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		throw IOException("Failed to allocate ODBC environment handle");
	}

	// Set ODBC version
	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		throw IOException("Failed to set ODBC version");
	}

	// Allocate connection handle
	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		throw IOException("Failed to allocate ODBC connection handle");
	}

	ODBCConnectionParameters config;
	unordered_set<string> unused_params;
	std::tie(config, unused_params) = ParseConnectionParameters(dsn);

	// Build connection string
	string connection_string;
	if (!config.driver.empty()) {
		connection_string += "DRIVER=" + config.driver + ";";
	}
	if (!config.server.empty()) {
		connection_string += "SERVER=" + config.server + ";";
	}
	if (!config.uid.empty()) {
		connection_string += "UID=" + config.uid + ";";
	}
	if (!config.pwd.empty()) {
		connection_string += "PWD=" + config.pwd + ";";
	}
	if (!config.database.empty()) {
		connection_string += "DATABASE=" + config.database + ";";
	}
	if (config.port != 0) {
		connection_string += "PORT=" + std::to_string(config.port) + ";";
	}

	// Add character set for Chinese support
	// For SQL Server with FreeTDS driver, use UTF-8 encoding
	connection_string += "ClientCharset=UTF-8;";
	connection_string += "TDS_Version=8.0;";

	// Connect to the data source
	SQLCHAR outstr[1024];
	SQLSMALLINT outstrlen;
	ret = SQLDriverConnect(dbc, NULL,
		(SQLCHAR*)connection_string.c_str(), SQL_NTS,
		outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLCHAR sqlstate[6];
		SQLINTEGER native_error;
		SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
		SQLSMALLINT text_length;
		
		SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlstate, &native_error,
			message_text, SQL_MAX_MESSAGE_LENGTH, &text_length);
		
		SQLFreeHandle(SQL_HANDLE_DBC, dbc);
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		
		throw IOException("Failed to connect to ODBC database with parameters \"%s\": %s",
			dsn.c_str(), (char*)message_text);
	}

	// Set character set if needed
	// Note: Character set setting in ODBC is driver-specific and may not be needed

	// Free environment handle as it's no longer needed
	SQLFreeHandle(SQL_HANDLE_ENV, env);

	return dbc;
}

string ODBCUtils::EscapeQuotes(const string &text, char quote) {
	string result;
	for (auto c : text) {
		if (c == quote) {
			result += "\\";
			result += quote;
		} else if (c == '\\') {
			result += "\\\\";
		} else {
			result += c;
		}
	}
	return result;
}

string ODBCUtils::WriteQuoted(const string &text, char quote) {
	// 1. Escapes all occurences of 'quote' by escaping them with a backslash
	// 2. Adds quotes around the string
	return string(1, quote) + EscapeQuotes(text, quote) + string(1, quote);
}

string ODBCUtils::WriteIdentifier(const string &identifier) {
	// For SQL Server ODBC, use square brackets for identifiers
	// We need to handle square brackets specially since they are different from quotes
	string result;
	result += '[';
	result += EscapeQuotes(identifier, ']'); // Escape any closing brackets in the identifier
	result += ']';
	return result;
}

string ODBCUtils::WriteLiteral(const string &identifier) {
	return ODBCUtils::WriteQuoted(identifier, '\'');
}

static string TransformBlobToODBC(const string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	string result = "x'";
	for (idx_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	result += "'";
	return result;
}

string ODBCUtils::TransformConstant(const Value &val) {
	if (val.type().IsNumeric() || val.type().id() == LogicalTypeId::BOOLEAN) {
		return val.ToSQLString();
	}
	if (val.type().id() == LogicalTypeId::BLOB) {
		return TransformBlobToODBC(StringValue::Get(val));
	}
	if (val.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
		return val.DefaultCastAs(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
	}
	return val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
}

} // namespace duckdb
