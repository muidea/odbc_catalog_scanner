#pragma once

#include <sql.h>
#include <sqlext.h>

namespace duckdb {

struct ODBCConnectGuard {
	SQLHENV environment = SQL_NULL_HENV;
	SQLHDBC connection = SQL_NULL_HDBC;

	ODBCConnectGuard() = default;
	ODBCConnectGuard(const ODBCConnectGuard &) = delete;
	ODBCConnectGuard &operator=(const ODBCConnectGuard &) = delete;
	~ODBCConnectGuard() {
		if (connection) {
			SQLDisconnect(connection);
			SQLFreeHandle(SQL_HANDLE_DBC, connection);
		}
		if (environment) {
			SQLFreeHandle(SQL_HANDLE_ENV, environment);
		}
	}
	SQLHDBC Release(SQLHENV &out_environment) {
		out_environment = environment;
		auto result = connection;
		environment = SQL_NULL_HENV;
		connection = SQL_NULL_HDBC;
		return result;
	}
};

} // namespace duckdb
