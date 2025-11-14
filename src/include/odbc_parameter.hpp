//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_parameter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <sql.h>
#include <sqlext.h>

namespace duckdb {

struct ODBCParameter {
	Value value;
	SQLSMALLINT buffer_type = SQL_UNKNOWN_TYPE;
	bool is_unsigned = false;

	vector<char> bind_buffer;
	SQLLEN bind_length = 0;

	ODBCParameter(const string &query, Value value_p);

	// TODO: Implement ODBC parameter binding
	// This is complex and requires mapping DuckDB types to ODBC SQL types
};

} // namespace duckdb
