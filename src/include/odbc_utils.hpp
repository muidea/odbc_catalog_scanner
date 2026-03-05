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
};

enum class ODBCResultStreaming { UNINITIALIZED, ALLOW_STREAMING, FORCE_MATERIALIZATION };

enum class ODBCConnectorInterface { UNINITIALIZED, BASIC, PREPARED_STATEMENT };

class ODBCUtils {
public:
	static std::tuple<ODBCConnectionParameters, unordered_set<string>> ParseConnectionParameters(const string &dsn);
	static SQLHDBC Connect(const string &dsn);

	static string WriteIdentifier(const string &identifier);
	static string WriteLiteral(const string &identifier);
	static string EscapeQuotes(const string &text, char quote);
	static string WriteQuoted(const string &text, char quote);
	static string TransformConstant(const Value &val);
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
