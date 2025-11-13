//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_parameter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "odbc.h"

namespace duckdb {

struct ODBCParameter {
	Value value;
	enum_field_types buffer_type = MYSQL_TYPE_INVALID;
	bool is_unsigned = false;

	vector<char> bind_buffer;
	unsigned long bind_length = 0;

	ODBCParameter(const string &query, Value value_p);

	MYSQL_BIND CreateBind();
};

} // namespace duckdb
