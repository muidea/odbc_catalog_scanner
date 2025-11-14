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
	} else if (type_info.type_name == "mediumint" || type_info.type_name == "int") {
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
	} else if (type_info.type_name == "year") {
		return LogicalType::INTEGER;
	} else if (type_info.type_name == "datetime") {
		return LogicalType::TIMESTAMP;
	} else if (type_info.type_name == "decimal") {
		if (type_info.precision > 0 && type_info.precision <= 38) {
			return LogicalType::DECIMAL(type_info.precision, type_info.scale);
		}
		return LogicalType::DOUBLE;
	} else if (type_info.type_name == "json") {
		// FIXME
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "enum") {
		// FIXME: we can actually retrieve the enum values from the column_type
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "set") {
		// FIXME: set is essentially a list of enum
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "bit") {
		if (type_info.column_type == "bit(1)" && type_config.bit1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		return LogicalType::BLOB;
	} else if (type_info.type_name == "blob" || type_info.type_name == "tinyblob" ||
	           type_info.type_name == "mediumblob" || type_info.type_name == "longblob" ||
	           type_info.type_name == "binary" || type_info.type_name == "varbinary" ||
	           type_info.type_name == "geometry" || type_info.type_name == "point" ||
	           type_info.type_name == "linestring" || type_info.type_name == "polygon" ||
	           type_info.type_name == "multipoint" || type_info.type_name == "multilinestring" ||
	           type_info.type_name == "multipolygon" || type_info.type_name == "geomcollection") {
		return LogicalType::BLOB;
	} else if (type_info.type_name == "varchar" || type_info.type_name == "mediumtext" ||
	           type_info.type_name == "longtext" || type_info.type_name == "text" || type_info.type_name == "enum" ||
	           type_info.type_name == "char") {
		return LogicalType::VARCHAR;
	}
	// fallback for unknown types
	return LogicalType::VARCHAR;
}

LogicalType ODBCTypes::FieldToLogicalType(const ODBCTypeConfig &type_config, void *field_info) {
	ODBCTypeData type_data;
	// TODO: Implement proper ODBC field type mapping
	// For now, return VARCHAR for everything
	type_data.type_name = "varchar";
	type_data.column_type = "varchar";
	return LogicalType::VARCHAR;
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

} // namespace duckdb
