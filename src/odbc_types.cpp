#include "odbc_types.hpp"
#include "odbc_temporal_utils.hpp"

namespace duckdb {

static bool GetBoolOption(ClientContext &context, const string &name, bool default_value = false) {
	Value value;
	if (context.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		return BooleanValue::Get(value);
	}
	return default_value;
}

static bool GetBoolOption(DatabaseInstance &db, const string &name, bool default_value = false) {
	Value value;
	if (db.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		return BooleanValue::Get(value);
	}
	return default_value;
}

ODBCTypeConfig::ODBCTypeConfig() = default;

ODBCTypeConfig::ODBCTypeConfig(ClientContext &context)
    : bit1_as_boolean(GetBoolOption(context, "odbc_catalog_bit1_as_boolean")),
      tinyint1_as_boolean(GetBoolOption(context, "odbc_catalog_tinyint1_as_boolean")),
      time_as_time(GetBoolOption(context, "odbc_catalog_time_as_time", true)) {
}

ODBCTypeConfig::ODBCTypeConfig(ClientContext &context, ODBCDialect dialect_p) : ODBCTypeConfig(context) {
	has_dialect = true;
	dialect = dialect_p;
}

ODBCTypeConfig::ODBCTypeConfig(DatabaseInstance &db, ODBCDialect dialect_p)
    : bit1_as_boolean(GetBoolOption(db, "odbc_catalog_bit1_as_boolean")),
      tinyint1_as_boolean(GetBoolOption(db, "odbc_catalog_tinyint1_as_boolean")),
      time_as_time(GetBoolOption(db, "odbc_catalog_time_as_time", true)), has_dialect(true), dialect(dialect_p) {
}

static bool TryTypeToLogicalType(const ODBCTypeConfig &type_config, const ODBCTypeData &type_info,
                                 LogicalType &out_type) {
	auto type_name = NormalizeODBCTypeName(type_info.type_name);
	if (type_name.empty()) {
		return false;
	}

	if (type_name == "bit" || type_name == "boolean") {
		out_type = LogicalType::BOOLEAN;
		return true;
	}
	if (type_name == "tinyint") {
		if (type_config.has_dialect && type_config.dialect == ODBCDialect::DAMENG) {
			out_type = LogicalType::TINYINT;
			return true;
		}
		out_type = LogicalType::UTINYINT;
		return true;
	}
	if (type_name == "smallint") {
		out_type = LogicalType::SMALLINT;
		return true;
	}
	if (type_name == "int" || type_name == "integer") {
		out_type = LogicalType::INTEGER;
		return true;
	}
	if (type_name == "bigint") {
		out_type = LogicalType::BIGINT;
		return true;
	}
	if (type_name == "decimal" || type_name == "numeric" || type_name == "number" || type_name == "dec" ||
	    type_name == "smalldecimal" || type_name == "decfloat" || type_name == "money" || type_name == "smallmoney") {
		auto precision = type_info.precision;
		auto scale = type_info.scale;
		if (type_name == "money") {
			precision = 19;
			scale = 4;
		} else if (type_name == "smallmoney") {
			precision = 10;
			scale = 4;
		}
		if (type_name == "decfloat" || type_name == "smalldecimal") {
			out_type = LogicalType::VARCHAR_COLLATION("odbc_exact_numeric");
			return true;
		}
		if (precision > 0 && precision <= 38 && scale >= 0 && scale <= precision) {
			out_type = LogicalType::DECIMAL(NumericCast<uint8_t>(precision), NumericCast<uint8_t>(scale));
			return true;
		}
		out_type = LogicalType::VARCHAR_COLLATION("odbc_exact_numeric");
		return true;
	}
	if (type_name == "real") {
		out_type = LogicalType::FLOAT;
		return true;
	}
	if (type_name == "float" || type_name == "double" || type_name == "double precision") {
		out_type = LogicalType::DOUBLE;
		return true;
	}
	if (type_name == "date") {
		out_type = LogicalType::DATE;
		return true;
	}
	if (type_name == "time") {
		out_type = type_config.time_as_time ? LogicalType::TIME : LogicalType::VARCHAR;
		return true;
	}
	if (IsODBCTimeWithTimeZoneTypeName(type_name)) {
		out_type = LogicalType::VARCHAR;
		return true;
	}
	if (IsODBCLocalTimeZoneTypeName(type_name)) {
		out_type = LogicalType::TIMESTAMP_TZ;
		return true;
	}
	if (((StringUtil::Contains(type_name, "timestamp") || StringUtil::Contains(type_name, "datetime")) &&
	     StringUtil::Contains(type_name, "with time zone")) ||
	    type_name == "timestamptz" || type_name == "datetimeoffset") {
		out_type = LogicalType::TIMESTAMP_TZ;
		return true;
	}
	if (type_name == "timestamp" || type_name == "datetime" || type_name == "datetime2" ||
	    type_name == "smalldatetime" || type_name == "seconddate" || type_name == "longdate") {
		out_type = LogicalType::TIMESTAMP;
		return true;
	}
	if (type_name == "rowversion" || type_name == "binary" || type_name == "varbinary" || type_name == "image" ||
	    type_name == "longvarbinary" || type_name == "blob") {
		out_type = LogicalType::BLOB;
		return true;
	}
	if (type_name == "char" || type_name == "character" || type_name == "varchar" || type_name == "varchar2" ||
	    type_name == "nvarchar2" || type_name == "text" || type_name == "nchar" || type_name == "nvarchar" ||
	    type_name == "ntext" || type_name == "xml" || type_name == "uniqueidentifier" || type_name == "sql_variant" ||
	    type_name == "clob" || type_name == "nclob" || type_name == "dbclob" || type_name == "graphic" ||
	    type_name == "vargraphic" || type_name == "long vargraphic" || type_name == "alphanum" ||
	    type_name == "shorttext") {
		out_type = LogicalType::VARCHAR;
		return true;
	}
	return false;
}

LogicalType ODBCTypes::TypeToLogicalType(const ODBCTypeConfig &type_config, const ODBCTypeData &type_info) {
	LogicalType out_type;
	if (TryTypeToLogicalType(type_config, type_info, out_type)) {
		return out_type;
	}
	return LogicalType::VARCHAR;
}

LogicalType ODBCTypes::FieldToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN column_size,
                                          SQLSMALLINT decimal_digits, const string &reported_type_name) {
	ODBCTypeData type_info;
	type_info.type_name = StringUtil::Lower(reported_type_name);
	type_info.column_type = type_info.type_name;
	type_info.precision = NumericCast<int64_t>(column_size);
	type_info.scale = decimal_digits;
	LogicalType out_type;
	if (!type_info.type_name.empty() && TryTypeToLogicalType(type_config, type_info, out_type)) {
		return out_type;
	}

	switch (data_type) {
	case SQL_BIT:
		type_info.type_name = "bit";
		break;
	case SQL_TINYINT:
		type_info.type_name = "tinyint";
		break;
	case SQL_SMALLINT:
		type_info.type_name = "smallint";
		break;
	case SQL_INTEGER:
		type_info.type_name = "int";
		break;
	case SQL_BIGINT:
		type_info.type_name = "bigint";
		break;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		type_info.type_name = "decimal";
		break;
	case SQL_REAL:
		type_info.type_name = "real";
		break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		type_info.type_name = "float";
		break;
	case SQL_DATE:
	case SQL_TYPE_DATE:
		type_info.type_name = "date";
		break;
	case SQL_TIME:
	case SQL_TYPE_TIME:
		type_info.type_name = "time";
		break;
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
		type_info.type_name = "datetime2";
		break;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		type_info.type_name = "varbinary";
		break;
	default:
		type_info.type_name = "varchar";
		break;
	}
	type_info.column_type = type_info.type_name;
	return TypeToLogicalType(type_config, type_info);
}

SQLSMALLINT ODBCTypes::GetCType(SQLSMALLINT sql_type) {
	switch (sql_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
		return SQL_C_CHAR;
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		return SQL_C_WCHAR;
	case SQL_TINYINT:
		return SQL_C_UTINYINT;
	case SQL_SMALLINT:
		return SQL_C_SSHORT;
	case SQL_INTEGER:
		return SQL_C_SLONG;
	case SQL_BIGINT:
		return SQL_C_SBIGINT;
	case SQL_REAL:
		return SQL_C_FLOAT;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		return SQL_C_DOUBLE;
	case SQL_BIT:
		return SQL_C_BIT;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return SQL_C_BINARY;
	case SQL_DATE:
	case SQL_TYPE_DATE:
		return SQL_C_TYPE_DATE;
	case SQL_TIME:
	case SQL_TYPE_TIME:
		return SQL_C_TYPE_TIME;
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
		return SQL_C_TYPE_TIMESTAMP;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
	case SQL_GUID:
	default:
		return SQL_C_CHAR;
	}
}

LogicalType ODBCTypes::ODBCToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN column_size,
                                         SQLSMALLINT decimal_digits, const string &type_name) {
	return FieldToLogicalType(type_config, data_type, column_size, decimal_digits, type_name);
}

} // namespace duckdb
