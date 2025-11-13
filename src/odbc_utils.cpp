#include "odbc_utils.hpp"

#include <tuple>

#include "odbc_com.h"

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
	// parse options
	idx_t pos = 0;
	while (pos < dsn.size()) {
		string key;
		string value;
		if (!ParseValue(dsn, pos, key)) {
			break;
		}
		if (pos >= dsn.size() || dsn[pos] != '=') {
			throw InvalidInputException("Invalid dsn \"%s\" - expected key=value pairs separated by spaces", dsn);
		}
		pos++;
		if (!ParseValue(dsn, pos, value)) {
			throw InvalidInputException("Invalid dsn \"%s\" - expected key=value pairs separated by spaces", dsn);
		}
		key = StringUtil::Lower(key);

		if (set_options.find(key) != set_options.end()) {
			throw InvalidInputException("Duplicate '%s' parameter in connection string. Each parameter "
			                            "should only be specified once.",
			                            key);
		}

		// Handle duplicate options (except for aliased options like passwd/password
		// which map to the same option)
		if (key == "host") {
			set_options.insert("host");
			result.host = value;
		} else if (key == "user") {
			set_options.insert("user");
			result.user = value;
		} else if (key == "passwd" || key == "password") {
			set_options.insert("password");
			result.passwd = value;
		} else if (key == "db" || key == "database") {
			set_options.insert("database");
			result.db = value;
		} else if (key == "port") {
			set_options.insert("port");
			result.port = ParsePort(value);
		} else if (key == "socket" || key == "unix_socket") {
			set_options.insert("socket");
			result.unix_socket = value;
		} else if (key == "compress") {
			set_options.insert("compress");
			if (ParseBoolValue(value)) {
				result.client_flag |= CLIENT_COMPRESS;
			} else {
				result.client_flag &= ~CLIENT_COMPRESS;
			}
		} else if (key == "compression") {
			set_options.insert("compress");
			auto val = StringUtil::Lower(value);
			if (val == "required") {
				result.client_flag |= CLIENT_COMPRESS;
			} else if (val == "disabled") {
				result.client_flag &= ~CLIENT_COMPRESS;
			} else if (val == "preferred") {
				// nop
			} else {
				throw InvalidInputException("Invalid dsn - compression mode must be either "
				                            "disabled/required/preferred - got %s",
				                            value);
			}
		} else if (key == "ssl_mode") {
			set_options.insert("ssl_mode");
			auto val = StringUtil::Lower(value);
			if (val == "disabled") {
				result.ssl_mode = SSL_MODE_DISABLED;
			} else if (val == "required") {
				result.ssl_mode = SSL_MODE_REQUIRED;
			} else if (val == "verify_ca") {
				result.ssl_mode = SSL_MODE_VERIFY_CA;
			} else if (val == "verify_identity") {
				result.ssl_mode = SSL_MODE_VERIFY_IDENTITY;
			} else if (val == "preferred") {
				result.ssl_mode = SSL_MODE_PREFERRED;
			} else {
				throw InvalidInputException("Invalid dsn - ssl mode must be either "
				                            "disabled, required, verify_ca, "
				                            "verify_identity or preferred - got %s",
				                            value);
			}
		} else if (key == "ssl_ca") {
			set_options.insert("ssl_ca");
			result.ssl_ca = value;
		} else if (key == "ssl_capath") {
			set_options.insert("ssl_capath");
			result.ssl_ca_path = value;
		} else if (key == "ssl_cert") {
			set_options.insert("ssl_cert");
			result.ssl_cert = value;
		} else if (key == "ssl_cipher") {
			set_options.insert("ssl_cipher");
			result.ssl_cipher = value;
		} else if (key == "ssl_crl") {
			set_options.insert("ssl_crl");
			result.ssl_crl = value;
		} else if (key == "ssl_crlpath") {
			set_options.insert("ssl_crlpath");
			result.ssl_crl_path = value;
		} else if (key == "ssl_key") {
			set_options.insert("ssl_key");
			result.ssl_key = value;
		} else {
			throw InvalidInputException("Unrecognized configuration parameter \"%s\" "
			                            "- expected options are host, "
			                            "user, passwd, db, port, socket",
			                            key);
		}
	}
	// read options that are not set from environment variables
	if (set_options.find("host") == set_options.end()) {
		ReadOptionFromEnv("MYSQL_HOST", result.host);
	}
	if (set_options.find("password") == set_options.end()) {
		ReadOptionFromEnv("MYSQL_PWD", result.passwd);
	}
	if (set_options.find("user") == set_options.end()) {
		ReadOptionFromEnv("MYSQL_USER", result.user);
	}
	if (set_options.find("database") == set_options.end()) {
		ReadOptionFromEnv("MYSQL_DATABASE", result.db);
	}
	if (set_options.find("socket") == set_options.end()) {
		ReadOptionFromEnv("MYSQL_UNIX_PORT", result.unix_socket);
	}
	if (set_options.find("port") == set_options.end()) {
		string port_number;
		if (ReadOptionFromEnv("MYSQL_TCP_PORT", port_number)) {
			result.port = ParsePort(port_number);
		}
	}
	if (set_options.find("compress") == set_options.end()) {
		string compress;
		if (ReadOptionFromEnv("MYSQL_COMPRESS", compress)) {
			if (ParseBoolValue(compress)) {
				result.client_flag |= CLIENT_COMPRESS;
			} else {
				result.client_flag &= ~CLIENT_COMPRESS;
			}
		}
	}
	return std::make_tuple(result, set_options);
}

void SetODBCOption(MYSQL *odbc, enum odbc_option option, const string &value) {
	if (value.empty()) {
		return;
	}
	int rc = odbc_options(odbc, option, value.c_str());
	if (rc != 0) {
		throw InternalException("Failed to set ODBC option");
	}
}

MYSQL *ODBCUtils::Connect(const string &dsn) {
	MYSQL *odbc = odbc_init(NULL);
	if (!odbc) {
		throw IOException("Failure in odbc_init");
	}
	MYSQL *result;

	ODBCConnectionParameters config;
	unordered_set<string> unused;
	std::tie(config, unused) = ParseConnectionParameters(dsn);

	// set SSL options (if any)
	if (config.ssl_mode != SSL_MODE_PREFERRED) {
		odbc_options(odbc, MYSQL_OPT_SSL_MODE, &config.ssl_mode);
	}
	SetODBCOption(odbc, MYSQL_OPT_SSL_CA, config.ssl_ca);
	SetODBCOption(odbc, MYSQL_OPT_SSL_CAPATH, config.ssl_ca_path);
	SetODBCOption(odbc, MYSQL_OPT_SSL_CERT, config.ssl_cert);
	SetODBCOption(odbc, MYSQL_OPT_SSL_CIPHER, config.ssl_cipher);
	SetODBCOption(odbc, MYSQL_OPT_SSL_CRL, config.ssl_crl);
	SetODBCOption(odbc, MYSQL_OPT_SSL_CRLPATH, config.ssl_crl_path);
	SetODBCOption(odbc, MYSQL_OPT_SSL_KEY, config.ssl_key);

	// get connection options
	const char *host = config.host.empty() ? nullptr : config.host.c_str();
	const char *user = config.user.empty() ? nullptr : config.user.c_str();
	const char *passwd = config.passwd.empty() ? nullptr : config.passwd.c_str();
	const char *db = config.db.empty() ? nullptr : config.db.c_str();
	const char *unix_socket = config.unix_socket.empty() ? nullptr : config.unix_socket.c_str();
	result = odbc_real_connect(odbc, host, user, passwd, db, config.port, unix_socket, config.client_flag);
	if (!result) {
		string original_error = odbc_error(odbc);
		string attempted_host = host ? host : "nullptr (default)";

		if (config.host.empty() || config.host == "localhost") {
			result =
			    odbc_real_connect(odbc, "127.0.0.1", user, passwd, db, config.port, unix_socket, config.client_flag);

			if (!result) {
				throw IOException("Failed to connect to ODBC database with parameters "
				                  "\"%s\": %s. First attempted host: %s. "
				                  "Retry with 127.0.0.1 also failed.",
				                  dsn, odbc_error(odbc), attempted_host.c_str());
			}
		} else {
			throw IOException("Failed to connect to ODBC database with parameters "
			                  "\"%s\": %s. Attempted host: %s",
			                  dsn, original_error.c_str(), attempted_host.c_str());
		}
	}
	if (odbc_set_character_set(odbc, "utf8mb4") != 0) {
		throw IOException("Failed to set ODBC character set");
	}
	D_ASSERT(odbc == result);
	return result;
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
	return ODBCUtils::WriteQuoted(identifier, '`');
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
