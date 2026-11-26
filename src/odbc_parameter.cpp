#include "odbc_parameter.hpp"
#include "odbc_temporal_utils.hpp"
#include "odbc_types.hpp"

#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

template <typename NUM_TYPE>
static void FillNumberBuffer(Value &value, vector<char> &bind_buffer) {
	bind_buffer.resize(sizeof(NUM_TYPE));
	NUM_TYPE val = value.GetValueUnsafe<NUM_TYPE>();
	std::memcpy(bind_buffer.data(), &val, sizeof(NUM_TYPE));
}

static void FillTextBuffer(const string &text, vector<char> &bind_buffer, SQLSMALLINT &c_type, SQLSMALLINT &buffer_type,
                           SQLULEN &column_size, SQLSMALLINT &decimal_digits, SQLLEN &bind_length) {
	c_type = SQL_C_CHAR;
	buffer_type = SQL_VARCHAR;
	column_size = MaxValue<SQLULEN>(text.size(), 1);
	decimal_digits = 0;
	bind_buffer.resize(text.size() + 1);
	if (!text.empty()) {
		std::memcpy(bind_buffer.data(), text.data(), text.size());
	}
	bind_buffer[text.size()] = '\0';
	bind_length = static_cast<SQLLEN>(text.size());
}

static void FillDateBuffer(const Value &value, vector<char> &bind_buffer, SQLSMALLINT &c_type, SQLSMALLINT &buffer_type,
                           SQLULEN &column_size, SQLSMALLINT &decimal_digits, SQLLEN &bind_length) {
	date_t dd = DateValue::Get(value);
	if (!dd.IsFinite()) {
		throw ConversionException("ODBC DATE parameter must be finite: %s", value.ToString());
	}
	int32_t year, month, day;
	Date::Convert(dd, year, month, day);
	if (!IsODBCYearInRange(year)) {
		throw ConversionException("ODBC DATE parameter year out of supported range (0001..9999): %s",
		                          value.ToString());
	}
	SQL_DATE_STRUCT date;
	std::memset(&date, '\0', sizeof(SQL_DATE_STRUCT));
	date.year = static_cast<SQLSMALLINT>(year);
	date.month = static_cast<SQLUSMALLINT>(month);
	date.day = static_cast<SQLUSMALLINT>(day);

	bind_buffer.resize(sizeof(SQL_DATE_STRUCT));
	std::memcpy(bind_buffer.data(), &date, sizeof(SQL_DATE_STRUCT));
	c_type = SQL_C_TYPE_DATE;
	buffer_type = SQL_TYPE_DATE;
	column_size = 10;
	decimal_digits = 0;
	bind_length = sizeof(SQL_DATE_STRUCT);
}

static void FillTimeBuffer(const Value &value, vector<char> &bind_buffer, SQLSMALLINT &c_type, SQLSMALLINT &buffer_type,
                           SQLULEN &column_size, SQLSMALLINT &decimal_digits, SQLLEN &bind_length) {
	dtime_t dt = TimeValue::Get(value);
	int32_t hour, minute, second, micros;
	Time::Convert(dt, hour, minute, second, micros);
	if (!IsODBCTimeInRange(hour, minute, second, micros)) {
		throw ConversionException("ODBC TIME parameter out of range (00:00:00..23:59:59.999999): %s",
		                          value.ToString());
	}
	if (micros > 0) {
		FillTextBuffer(FormatODBCTimeParts(hour, minute, second, micros), bind_buffer, c_type, buffer_type, column_size,
		               decimal_digits, bind_length);
		return;
	}
	SQL_TIME_STRUCT time;
	std::memset(&time, '\0', sizeof(SQL_TIME_STRUCT));
	time.hour = static_cast<SQLUSMALLINT>(hour);
	time.minute = static_cast<SQLUSMALLINT>(minute);
	time.second = static_cast<SQLUSMALLINT>(second);

	bind_buffer.resize(sizeof(SQL_TIME_STRUCT));
	std::memcpy(bind_buffer.data(), &time, sizeof(SQL_TIME_STRUCT));
	c_type = SQL_C_TYPE_TIME;
	buffer_type = SQL_TYPE_TIME;
	column_size = 8;
	decimal_digits = 0;
	bind_length = sizeof(SQL_TIME_STRUCT);
}

static void FillTimestampBuffer(const Value &value, vector<char> &bind_buffer, SQLSMALLINT &c_type,
                                SQLSMALLINT &buffer_type, SQLULEN &column_size, SQLSMALLINT &decimal_digits,
                                SQLLEN &bind_length) {
	timestamp_t ts = TimestampValue::Get(value);
	if (!ts.IsFinite()) {
		throw ConversionException("ODBC TIMESTAMP parameter must be finite: %s", value.ToString());
	}
	date_t dd;
	dtime_t dt;
	Timestamp::Convert(ts, dd, dt);
	int32_t year, month, day, hour, minute, second, micros;
	Date::Convert(dd, year, month, day);
	if (!IsODBCYearInRange(year)) {
		throw ConversionException("ODBC TIMESTAMP parameter year out of supported range (0001..9999): %s",
		                          value.ToString());
	}
	Time::Convert(dt, hour, minute, second, micros);
	SQL_TIMESTAMP_STRUCT timestamp;
	std::memset(&timestamp, '\0', sizeof(SQL_TIMESTAMP_STRUCT));
	timestamp.year = static_cast<SQLSMALLINT>(year);
	timestamp.month = static_cast<SQLUSMALLINT>(month);
	timestamp.day = static_cast<SQLUSMALLINT>(day);
	timestamp.hour = static_cast<SQLUSMALLINT>(hour);
	timestamp.minute = static_cast<SQLUSMALLINT>(minute);
	timestamp.second = static_cast<SQLUSMALLINT>(second);
	timestamp.fraction = static_cast<SQLUINTEGER>(micros) * 1000U;

	bind_buffer.resize(sizeof(SQL_TIMESTAMP_STRUCT));
	std::memcpy(bind_buffer.data(), &timestamp, sizeof(SQL_TIMESTAMP_STRUCT));
	c_type = SQL_C_TYPE_TIMESTAMP;
	buffer_type = SQL_TYPE_TIMESTAMP;
	column_size = 26;
	decimal_digits = 6;
	bind_length = sizeof(SQL_TIMESTAMP_STRUCT);
}

ODBCParameter::ODBCParameter(const string &query, Value value_p) : value(std::move(value_p)) {
	if (value.IsNull()) {
		return;
	}

	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		this->buffer_type = SQL_TINYINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<int8_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::TINYINT:
		this->buffer_type = SQL_TINYINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<int8_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::UTINYINT:
		this->buffer_type = SQL_TINYINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		this->is_unsigned = true;
		FillNumberBuffer<uint8_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::SMALLINT:
		this->buffer_type = SQL_SMALLINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<int16_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::USMALLINT:
		this->buffer_type = SQL_SMALLINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		this->is_unsigned = true;
		FillNumberBuffer<uint16_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::INTEGER:
		this->buffer_type = SQL_INTEGER;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<int32_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::UINTEGER:
		this->buffer_type = SQL_INTEGER;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		this->is_unsigned = true;
		FillNumberBuffer<uint32_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::BIGINT:
		this->buffer_type = SQL_BIGINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<int64_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::UBIGINT:
		this->buffer_type = SQL_BIGINT;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		this->is_unsigned = true;
		FillNumberBuffer<uint64_t>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::FLOAT:
		this->buffer_type = SQL_REAL;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<float>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::DOUBLE:
		this->buffer_type = SQL_DOUBLE;
		this->c_type = ODBCTypes::GetCType(this->buffer_type);
		FillNumberBuffer<double>(value, bind_buffer);
		this->column_size = bind_buffer.size();
		break;
	case LogicalTypeId::DATE:
		FillDateBuffer(value, bind_buffer, c_type, buffer_type, column_size, decimal_digits, bind_length);
		break;
	case LogicalTypeId::TIME:
		FillTimeBuffer(value, bind_buffer, c_type, buffer_type, column_size, decimal_digits, bind_length);
		break;
	case LogicalTypeId::TIMESTAMP:
		FillTimestampBuffer(value, bind_buffer, c_type, buffer_type, column_size, decimal_digits, bind_length);
		break;
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto utc_text = FormatODBCTimestampTZUTC(timestamp_tz_t(value.GetValueUnsafe<int64_t>()), true);
		FillTextBuffer(utc_text, bind_buffer, c_type, buffer_type, column_size, decimal_digits, bind_length);
		break;
	}
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::VARCHAR:
		FillTextBuffer(value.ToString(), bind_buffer, c_type, buffer_type, column_size, decimal_digits, bind_length);
		break;
	case LogicalTypeId::BLOB: {
		auto blob = StringValue::Get(value);
		this->c_type = SQL_C_BINARY;
		this->buffer_type = SQL_VARBINARY;
		bind_buffer.assign(blob.begin(), blob.end());
		this->column_size = MaxValue<SQLULEN>(bind_buffer.size(), 1);
		this->decimal_digits = 0;
		this->bind_length = NumericCast<SQLLEN>(bind_buffer.size());
		break;
	}
	default:
		throw IOException("Unsupported parameters type: \"%s\", ODBC query \"%s\"", value.type(), query.c_str());
	}
}

SQLRETURN ODBCParameter::BindParameter(SQLHSTMT stmt, SQLUSMALLINT param_idx) {
	if (value.IsNull()) {
		bind_length = SQL_NULL_DATA;
		return SQLBindParameter(stmt, param_idx, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 1, 0, nullptr, 0,
		                        &bind_length);
	}
	if (buffer_type == SQL_UNKNOWN_TYPE) {
		return SQL_ERROR;
	}
	return SQLBindParameter(stmt, param_idx, SQL_PARAM_INPUT, c_type, buffer_type, column_size, decimal_digits,
	                        bind_buffer.data(), bind_buffer.size(), &bind_length);
}

} // namespace duckdb
