#include "odbc_types.hpp"

namespace duckdb {

static bool GetBoolOption(ClientContext &context, const std::string &opt_name) {
	Value val;
	if (context.TryGetCurrentSetting(opt_name, val)) {
		return BooleanValue::Get(val);
	}
	return false;
}

ODBCTypeConfig::ODBCTypeConfig() {
}

ODBCTypeConfig::ODBCTypeConfig(ClientContext &context)
    : bit1_as_boolean(GetBoolOption(context, "odbc_bit1_as_boolean")),
      tinyint1_as_boolean(GetBoolOption(context, "odbc_tinyint1_as_boolean")),
      time_as_time(GetBoolOption(context, "odbc_time_as_time")) {
}

LogicalType ODBCTypes::TypeToLogicalType(const ODBCTypeConfig &type_config, const ODBCTypeData &type_info) {
	if (type_info.type_name == "tinyint") {
		if (type_info.column_type == "tinyint(1)" && type_config.tinyint1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UTINYINT;
		} else {
			return LogicalType::TINYINT;
		}
	} else if (type_info.type_name == "smallint") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::USMALLINT;
		} else {
			return LogicalType::SMALLINT;
		}
	} else if (type_info.type_name == "int") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UINTEGER;
		} else {
			return LogicalType::INTEGER;
		}
	} else if (type_info.type_name == "bigint") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UBIGINT;
		} else {
			return LogicalType::BIGINT;
		}
	} else if (type_info.type_name == "real") {
		return LogicalType::FLOAT;
	} else if (type_info.type_name == "float") {
		return LogicalType::FLOAT;
	} else if (type_info.type_name == "double") {
		return LogicalType::DOUBLE;
	} else if (type_info.type_name == "date") {
		return LogicalType::DATE;
	} else if (type_info.type_name == "time") {
		// we convert time to VARCHAR by default because TIME in ODBC is more like an
		// interval and can store ranges between -838:00:00 to 838:00:00
		return type_config.time_as_time ? LogicalType::TIME : LogicalType::VARCHAR;
	} else if (type_info.type_name == "timestamp") {
		// in ODBC, "timestamp" columns are timezone aware while "datetime" columns
		// are not
		return LogicalType::TIMESTAMP_TZ;
	} else if (type_info.type_name == "datetime") {
		return LogicalType::TIMESTAMP;
	} else if (type_info.type_name == "decimal") {
		if (type_info.precision > 0 && type_info.precision <= 38) {
			return LogicalType::DECIMAL(type_info.precision, type_info.scale);
		}
		return LogicalType::DOUBLE;
	} else if (type_info.type_name == "bit") {
		if (type_info.column_type == "bit(1)" && type_config.bit1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		return LogicalType::BLOB;
	} else if (type_info.type_name == "varbinary" || type_info.type_name == "binary" ||
	           type_info.type_name == "blob" || type_info.type_name == "longvarbinary") {
		return LogicalType::BLOB;
	} else if (type_info.type_name == "nvarchar" || type_info.type_name == "nchar" ||
	           type_info.type_name == "uniqueidentifier" || type_info.type_name == "interval") {
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "varchar" || type_info.type_name == "char" ||
	           type_info.type_name == "text" || type_info.type_name == "longvarchar") {
		return LogicalType::VARCHAR;
	}
	// fallback for unknown types
	return LogicalType::VARCHAR;
}

LogicalType ODBCTypes::FieldToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN col_size, SQLSMALLINT decimal_digits) {
	ODBCTypeData type_data;
	
	switch (data_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
		type_data.type_name = "varchar";
		break;
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		type_data.type_name = "nvarchar";
		break;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		type_data.type_name = "decimal";
		type_data.precision = (col_size > 0) ? static_cast<int64_t>(col_size) : 10;
		type_data.scale = (decimal_digits > 0) ? static_cast<int64_t>(decimal_digits) : 0;
		break;
	case SQL_SMALLINT:
		type_data.type_name = "smallint";
		break;
	case SQL_INTEGER:
		type_data.type_name = "int";
		break;
	case SQL_REAL:
		type_data.type_name = "real";
		break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		type_data.type_name = "double";
		break;
	case SQL_BIT:
		type_data.type_name = "bit";
		break;
	case SQL_TINYINT:
		type_data.type_name = "tinyint";
		break;
	case SQL_BIGINT:
		type_data.type_name = "bigint";
		break;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		type_data.type_name = "varbinary";
		break;
	case SQL_DATE:
		type_data.type_name = "date";
		break;
	case SQL_TIME:
		type_data.type_name = "time";
		break;
	case SQL_TIMESTAMP:
		type_data.type_name = "timestamp";
		break;
	case SQL_TYPE_DATE:
		type_data.type_name = "date";
		break;
	case SQL_TYPE_TIME:
		type_data.type_name = "time";
		break;
	case SQL_TYPE_TIMESTAMP:
		type_data.type_name = "timestamp";
		break;
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_YEAR:
	case SQL_INTERVAL_YEAR_TO_MONTH:
	case SQL_INTERVAL_DAY:
	case SQL_INTERVAL_HOUR:
	case SQL_INTERVAL_MINUTE:
	case SQL_INTERVAL_SECOND:
	case SQL_INTERVAL_DAY_TO_HOUR:
	case SQL_INTERVAL_DAY_TO_MINUTE:
	case SQL_INTERVAL_DAY_TO_SECOND:
	case SQL_INTERVAL_HOUR_TO_MINUTE:
	case SQL_INTERVAL_HOUR_TO_SECOND:
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		type_data.type_name = "interval";
		break;
	case SQL_GUID:
		type_data.type_name = "uniqueidentifier";
		break;
	default:
		type_data.type_name = "varchar";
		break;
	}
	
	type_data.column_type = type_data.type_name;
	return ODBCTypes::TypeToLogicalType(type_config, type_data);
}

LogicalType ODBCTypes::ToODBCType(const ODBCTypeConfig &type_config, const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::TIME:
		return type_config.time_as_time ? LogicalType::TIME : LogicalType::VARCHAR;
	case LogicalTypeId::LIST:
		throw NotImplementedException("ODBC does not support arrays - unsupported type \"%s\"", input.ToString());
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		throw NotImplementedException("ODBC does not support composite types - unsupported type \"%s\"",
		                              input.ToString());
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

string ODBCTypes::TypeToString(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::VARCHAR:
		return "TEXT";
	case LogicalType::UTINYINT:
		return "TINYINT UNSIGNED";
	case LogicalType::USMALLINT:
		return "SMALLINT UNSIGNED";
	case LogicalType::UINTEGER:
		return "INTEGER UNSIGNED";
	case LogicalType::UBIGINT:
		return "BIGINT UNSIGNED";
	case LogicalType::TIMESTAMP:
		return "DATETIME";
	case LogicalType::TIMESTAMP_TZ:
		return "TIMESTAMP";
	default:
		return input.ToString();
	}
}

SQLSMALLINT ODBCTypes::GetCType(SQLSMALLINT sql_type) {
	switch (sql_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		return SQL_C_CHAR;
	case SQL_TINYINT:
		return SQL_C_STINYINT;
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
		return SQL_C_CHAR; // Handle as string for simplicity
	case SQL_GUID:
		return SQL_C_GUID;
	default:
		return SQL_C_CHAR; // Default to string
	}
}

LogicalType ODBCTypes::ODBCToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN column_size, SQLSMALLINT decimal_digits) {
	return ODBCTypes::FieldToLogicalType(type_config, data_type, column_size, decimal_digits);
}

} // namespace duckdb
