//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "odbc_types.hpp"

namespace duckdb {
class ODBCSchemaEntry;
class ODBCTransaction;

struct ODBCConnectionParameters {
	string driver;
	string server;
	string database;
	string uid;
	string pwd;
	uint32_t port = 0;
	vector<string> extra_params;
};

enum class ODBCResultStreaming { UNINITIALIZED, ALLOW_STREAMING, FORCE_MATERIALIZATION };

enum class ODBCConnectorInterface { UNINITIALIZED, BASIC, PREPARED_STATEMENT };

struct ODBCCatalogObject {
	string schema;
	string name;
	string type;
};

class ODBCUtils {
public:
	static std::tuple<ODBCConnectionParameters, unordered_set<string>> ParseConnectionParameters(const string &dsn);
	static string BuildConnectionString(const string &dsn);
	static SQLHDBC Connect(const string &dsn, SQLHENV &out_env, uint32_t login_timeout = 0);
	static string CatalogPattern(SQLHDBC connection, const string &identifier);
	static vector<ODBCCatalogObject> GetCatalogObjects(SQLHDBC connection, const string &schema = string(),
	                                                   const string &table = string());

	static string WriteIdentifier(const string &identifier);
	static string WriteLiteral(const string &identifier);
	static string EscapeQuotes(const string &text, char quote);
	static string WriteQuoted(const string &text, char quote);
	static bool TryTransformConstant(const Value &val, ODBCDialect dialect, const string &remote_type_name, string &out);
	static string TransformConstant(const Value &val);
	static string TransformConstant(const Value &val, ODBCDialect dialect);
};

using ODBCStatementPtr = duckdb::unique_ptr<void, void (*)(SQLHSTMT)>;

inline void ODBCStatementDelete(SQLHSTMT stmt) {
	if (stmt) {
		SQLFreeStmt(stmt, SQL_CLOSE);
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	}
}

using ODBCResultPtr = duckdb::unique_ptr<void, void (*)(SQLHSTMT)>;

inline void ODBCResultDelete(SQLHSTMT stmt) {
	if (stmt) {
		SQLFreeStmt(stmt, SQL_CLOSE);
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	}
}

} // namespace duckdb
