//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "odbc.h"

namespace duckdb {

struct ODBCTypeData {
	string type_name;
	string column_type;
	int64_t precision;
	int64_t scale;
};

enum class ODBCTypeAnnotation { STANDARD, CAST_TO_VARCHAR, NUMERIC_AS_DOUBLE, CTID, JSONB, FIXED_LENGTH_CHAR };

struct ODBCType {
	idx_t oid = 0;
	ODBCTypeAnnotation info = ODBCTypeAnnotation::STANDARD;
	vector<ODBCType> children;
};

struct ODBCTypeConfig {
	bool bit1_as_boolean = false;
	bool tinyint1_as_boolean = false;
	bool time_as_time = false;

	ODBCTypeConfig();
	ODBCTypeConfig(ClientContext &context);
};

class ODBCTypes {
public:
	static LogicalType ToODBCType(const ODBCTypeConfig &type_config, const LogicalType &input);
	static LogicalType TypeToLogicalType(const ODBCTypeConfig &type_config, const ODBCTypeData &input);
	static LogicalType FieldToLogicalType(const ODBCTypeConfig &type_config, MYSQL_FIELD *field);
	static string TypeToString(const LogicalType &input);
};

} // namespace duckdb
