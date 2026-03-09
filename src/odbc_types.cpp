#include "odbc_types.hpp"
#include <cstdio>

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
	auto &name = type_info.type_name;

	if (name == "tinyint") {
		if (type_info.column_type == "tinyint(1)" && type_config.tinyint1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		if (StringUtil::Contains(type_info.column_type, "signed")) {
			return LogicalType::TINYINT;
		}
		return LogicalType::UTINYINT;
	} else if (name == "smallint") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::USMALLINT;
		}
		return LogicalType::SMALLINT;
	} else if (name == "int") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UINTEGER;
		}
		return LogicalType::INTEGER;
	} else if (name == "bigint") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UBIGINT;
		}
		return LogicalType::BIGINT;
	} else if (name == "real") {
		return LogicalType::FLOAT;
	} else if (name == "float") {
		return LogicalType::DOUBLE;
	} else if (name == "double") {
		return LogicalType::DOUBLE;
	} else if (name == "decimal" || name == "numeric") {
		if (type_info.precision > 0 && type_info.precision <= 38) {
			auto scale = (type_info.scale >= 0) ? static_cast<uint8_t>(type_info.scale) : 0;
			return LogicalType::DECIMAL(static_cast<uint8_t>(type_info.precision), scale);
		}
		return LogicalType::DOUBLE;
	} else if (name == "money") {
		return LogicalType::DECIMAL(19, 4);
	} else if (name == "smallmoney") {
		return LogicalType::DECIMAL(10, 4);
	} else if (name == "bit") {
		if (type_info.column_type == "bit(1)" && type_config.bit1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		return LogicalType::BOOLEAN;
	} else if (name == "date") {
		return LogicalType::DATE;
	} else if (name == "time") {
		return type_config.time_as_time ? LogicalType::TIME : LogicalType::VARCHAR;
	} else if (name == "datetime" || name == "datetime2" || name == "smalldatetime") {
		return LogicalType::TIMESTAMP;
	} else if (name == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (name == "datetimeoffset") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (name == "varbinary" || name == "binary" || name == "blob" || name == "longvarbinary" ||
	           name == "image") {
		return LogicalType::BLOB;
	} else if (name == "nvarchar" || name == "nchar" || name == "uniqueidentifier" || name == "interval" ||
	           name == "sysname" || name == "ntext") {
		return LogicalType::VARCHAR;
	} else if (name == "varchar" || name == "char" || name == "text" || name == "longvarchar") {
		return LogicalType::VARCHAR;
	} else if (name == "xml" || name == "hierarchyid" || name == "sql_variant" || name == "geography" ||
	           name == "geometry") {
		return LogicalType::VARCHAR;
	}
	return LogicalType::VARCHAR;
}

LogicalType ODBCTypes::FieldToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN col_size,
                                          SQLSMALLINT decimal_digits) {
	ODBCTypeData type_data;
	type_data.precision = 0;
	type_data.scale = 0;

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
		type_data.precision = (col_size > 0) ? static_cast<int64_t>(col_size) : 18;
		type_data.scale = (decimal_digits >= 0) ? static_cast<int64_t>(decimal_digits) : 0;
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
		type_data.type_name = "float";
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
	case SQL_TYPE_DATE:
		type_data.type_name = "date";
		break;
	case SQL_TIME:
	case SQL_TYPE_TIME:
		type_data.type_name = "time";
		break;
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
		type_data.type_name = "datetime";
		break;
	case SQL_GUID:
		type_data.type_name = "uniqueidentifier";
		break;
	case -154: // SQL_SS_TIME2
		type_data.type_name = "time";
		break;
	case -155: // SQL_SS_TIMESTAMPOFFSET
		type_data.type_name = "datetimeoffset";
		break;
	case -152: // SQL_SS_XML
		type_data.type_name = "xml";
		break;
	case -151: // SQL_SS_UDT
	case -150: // SQL_SS_VARIANT
		type_data.type_name = "sql_variant";
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
	case SQL_TIME:
	case SQL_TYPE_TIME:
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
		return SQL_C_TYPE_TIMESTAMP;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		return SQL_C_CHAR;
	case SQL_GUID:
		return SQL_C_BINARY;
	case -154: // SQL_SS_TIME2
		return SQL_C_CHAR;
	case -155: // SQL_SS_TIMESTAMPOFFSET
		return SQL_C_CHAR;
	case -152: // SQL_SS_XML
	case -151: // SQL_SS_UDT
	case -150: // SQL_SS_VARIANT
		return SQL_C_CHAR;
	default:
		return SQL_C_CHAR;
	}
}

LogicalType ODBCTypes::ODBCToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN column_size, SQLSMALLINT decimal_digits) {
	return ODBCTypes::FieldToLogicalType(type_config, data_type, column_size, decimal_digits);
}

} // namespace duckdb
