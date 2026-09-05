#include "odbc_dialect.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

static string ConnectionValue(const string &value) {
	string result = "{";
	for (auto ch : value) {
		result += ch == '}' ? "}}" : string(1, ch);
	}
	return result + "}";
}

static void AppendConnectionOption(string &result, const string &name, const string &value) {
	if (!value.empty()) {
		result += name + "=" + ConnectionValue(value) + ";";
	}
}

ODBCDialect ODBCDialectUtil::Parse(const string &name) {
	auto normalized = StringUtil::Lower(name);
	StringUtil::Trim(normalized);
	if (normalized == "hana") {
		return ODBCDialect::HANA;
	}
	if (normalized == "dameng") {
		return ODBCDialect::DAMENG;
	}
	if (normalized == "db2") {
		return ODBCDialect::DB2;
	}
	throw InvalidInputException("Unsupported ODBC catalog dialect \"%s\"; expected hana, dameng, or db2", name);
}

string ODBCDialectUtil::Name(ODBCDialect dialect) {
	switch (dialect) {
	case ODBCDialect::HANA:
		return "hana";
	case ODBCDialect::DAMENG:
		return "dameng";
	case ODBCDialect::DB2:
		return "db2";
	}
	throw InternalException("Unknown ODBC catalog dialect");
}

string ODBCDialectUtil::BuildConnectionString(ODBCDialect dialect, const ODBCCatalogConnectionOptions &options) {
	if (options.driver.empty() || options.host.empty() || options.user.empty() || options.password.empty() ||
	    options.port == 0 || (dialect != ODBCDialect::HANA && options.database.empty())) {
		throw InvalidInputException("ODBC catalog secret requires DRIVER, HOST, PORT, USER, and PASSWORD; DATABASE is "
		                            "required except for HANA");
	}

	string result;
	AppendConnectionOption(result, "DRIVER", options.driver);
	switch (dialect) {
	case ODBCDialect::HANA:
		AppendConnectionOption(result, "SERVERNODE", options.host + ":" + to_string(options.port));
		// A HANA tenant is selected by its SQL port. Passing the source's
		// database label as DATABASENAME changes the target database and fails
		// for the common SYSTEM user/schema label.
		break;
	case ODBCDialect::DAMENG:
		AppendConnectionOption(result, "SERVER", options.host);
		AppendConnectionOption(result, "TCP_PORT", to_string(options.port));
		AppendConnectionOption(result, "DATABASE", options.database);
		break;
	case ODBCDialect::DB2:
		AppendConnectionOption(result, "HOSTNAME", options.host);
		AppendConnectionOption(result, "PORT", to_string(options.port));
		AppendConnectionOption(result, "DATABASE", options.database);
		AppendConnectionOption(result, "PROTOCOL", "TCPIP");
		break;
	}
	AppendConnectionOption(result, "UID", options.user);
	AppendConnectionOption(result, "PWD", options.password);
	return result;
}

bool ODBCDialectUtil::IsInternalSchema(ODBCDialect dialect, const string &schema) {
	auto normalized = StringUtil::Lower(schema);
	StringUtil::Trim(normalized);
	if (normalized == "information_schema") {
		return true;
	}
	switch (dialect) {
	case ODBCDialect::HANA:
		return normalized == "sys" || normalized == "system" || StringUtil::StartsWith(normalized, "_sys_");
	case ODBCDialect::DAMENG:
		return normalized == "sys" || normalized == "sysauditor" || normalized == "sysdba" || normalized == "sysdbo";
	case ODBCDialect::DB2:
		return StringUtil::StartsWith(normalized, "syscat") || StringUtil::StartsWith(normalized, "sysibm") ||
		       StringUtil::StartsWith(normalized, "sysibmadm") || StringUtil::StartsWith(normalized, "sysproc") ||
		       StringUtil::StartsWith(normalized, "sysstat");
	}
	return false;
}

} // namespace duckdb
