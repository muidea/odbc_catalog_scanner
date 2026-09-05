//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_dialect.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class ODBCDialect : uint8_t { HANA, DAMENG, DB2 };

struct ODBCCatalogConnectionOptions {
	string driver;
	string host;
	string database;
	string user;
	string password;
	uint32_t port = 0;
};

class ODBCDialectUtil {
public:
	static ODBCDialect Parse(const string &name);
	static string Name(ODBCDialect dialect);
	static string BuildConnectionString(ODBCDialect dialect, const ODBCCatalogConnectionOptions &options);
	static bool IsInternalSchema(ODBCDialect dialect, const string &schema);
};

} // namespace duckdb
