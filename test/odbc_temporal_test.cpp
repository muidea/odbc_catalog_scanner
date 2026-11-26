#include "odbc_filter_pushdown.hpp"
#include "odbc_parameter.hpp"
#include "odbc_temporal_utils.hpp"
#include "odbc_types.hpp"
#include "odbc_utils.hpp"

#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace duckdb;

namespace {

void Require(bool cond, const char *msg) {
	if (!cond) {
		throw std::runtime_error(msg);
	}
}

template <typename Fn>
void RequireConversionError(Fn &&fn, const char *msg) {
	bool threw = false;
	try {
		fn();
	} catch (const ConversionException &) {
		threw = true;
	}
	Require(threw, msg);
}

} // namespace

int main() {
	// 1. Dialect temporal init & type-name normalization
	Require(string(ODBCSessionTemporalInitSQL(ODBCDialect::DAMENG)) == "SET TIME ZONE '+00:00'",
	        "Dameng temporal init SQL mismatch");
	Require(string(ODBCSessionTemporalInitSQL(ODBCDialect::DB2)).empty(), "Db2 temporal init SQL must be empty");
	Require(string(ODBCSessionTemporalInitSQL(ODBCDialect::HANA)).empty(), "HANA temporal init SQL must be empty");

	Require(NormalizeODBCTypeName("TIMESTAMP(6) WITH LOCAL TIME ZONE") == "timestamp with local time zone",
	        "NormalizeODBCTypeName TSLTZ");
	Require(NormalizeODBCTypeName("TIME(6)") == "time", "NormalizeODBCTypeName TIME(6)");
	Require(IsODBCLocalTimeZoneTypeName("DATETIME(6) WITH LOCAL TIME ZONE"), "IsODBCLocalTimeZoneTypeName DTLTZ");
	Require(!IsODBCLocalTimeZoneTypeName("TIMESTAMP(6) WITH TIME ZONE"), "TSTZ is not local TZ");
	Require(IsODBCTimeWithTimeZoneTypeName("TIME(6) WITH TIME ZONE"), "IsODBCTimeWithTimeZoneTypeName");

	// 2. ODBCTypes mapping (DM8 LTLZ, HANA LONGDATE, precision modifiers, unknown SQL_TYPE_* fallback)
	ODBCTypeConfig cfg;
	Require(ODBCTypes::TypeToLogicalType(cfg, {"TIMESTAMP(6) WITH LOCAL TIME ZONE", "", 0, 6}) ==
	            LogicalType::TIMESTAMP_TZ,
	        "TS(6) WITH LOCAL TIME ZONE -> TIMESTAMP_TZ");
	Require(ODBCTypes::TypeToLogicalType(cfg, {"DATETIME(6) WITH LOCAL TIME ZONE", "", 0, 6}) ==
	            LogicalType::TIMESTAMP_TZ,
	        "DT(6) WITH LOCAL TIME ZONE -> TIMESTAMP_TZ");
	Require(ODBCTypes::TypeToLogicalType(cfg, {"DATETIME(6) WITH TIME ZONE", "", 0, 6}) == LogicalType::TIMESTAMP_TZ,
	        "DT(6) WITH TIME ZONE -> TIMESTAMP_TZ");
	Require(ODBCTypes::TypeToLogicalType(cfg, {"TIME(6) WITH TIME ZONE", "", 0, 6}) == LogicalType::VARCHAR,
	        "TIME(6) WITH TIME ZONE -> VARCHAR");
	Require(ODBCTypes::TypeToLogicalType(cfg, {"TIME(6)", "", 0, 6}) == LogicalType::TIME, "TIME(6) -> TIME");
	ODBCTypeConfig cfg_time_text;
	cfg_time_text.time_as_time = false;
	Require(ODBCTypes::TypeToLogicalType(cfg_time_text, {"TIME(6)", "", 0, 6}) == LogicalType::VARCHAR,
	        "TIME(6) with time_as_time=false -> VARCHAR");
	Require(ODBCTypes::TypeToLogicalType(cfg, {"LONGDATE", "", 0, 7}) == LogicalType::TIMESTAMP,
	        "HANA LONGDATE -> TIMESTAMP");
	Require(ODBCTypes::TypeToLogicalType(cfg, {"SECONDDATE", "", 0, 0}) == LogicalType::TIMESTAMP,
	        "HANA SECONDDATE -> TIMESTAMP");
	Require(ODBCTypes::FieldToLogicalType(cfg, SQL_TYPE_TIMESTAMP, 26, 6, "UNKNOWN_VENDOR_TS") ==
	            LogicalType::TIMESTAMP,
	        "Unknown vendor SQL_TYPE_TIMESTAMP must fall back to TIMESTAMP");
	Require(ODBCTypes::FieldToLogicalType(cfg, SQL_TYPE_DATE, 10, 0, "UNKNOWN_VENDOR_DATE") == LogicalType::DATE,
	        "Unknown vendor SQL_TYPE_DATE must fall back to DATE");
	Require(ODBCTypes::FieldToLogicalType(cfg, SQL_TYPE_TIME, 8, 0, "UNKNOWN_VENDOR_TIME") == LogicalType::TIME,
	        "Unknown vendor SQL_TYPE_TIME must fall back to TIME");

	// 3. Struct/text formatters and strict parsers
	SQL_DATE_STRUCT d_struct = {2026, 6, 15};
	SQL_TIME_STRUCT t_struct = {0, 0, 0};
	SQL_TIMESTAMP_STRUCT ts_struct = {2026, 6, 15, 0, 0, 0, 123456000};
	Require(FormatODBCDateStruct(d_struct) == "2026-06-15", "FormatODBCDateStruct");
	Require(FormatODBCTimeStruct(t_struct) == "00:00:00", "FormatODBCTimeStruct midnight");
	Require(FormatODBCTimestampStruct(ts_struct) == "2026-06-15 00:00:00.123456", "FormatODBCTimestampStruct");
	Require(NormalizeODBCTimeText(" 08.09.10 ") == "08:09:10", "NormalizeODBCTimeText Db2 dot");
	Require(NormalizeODBCTimestampText("2026-06-15-12.34.56.123456") == "2026-06-15 12:34:56.123456",
	        "NormalizeODBCTimestampText Db2 dash");

	Value d_ad = Value::DATE(Date::FromDate(2026, 6, 15));
	Value d_bc = Value::DATE(Date::FromDate(-44, 3, 15));
	Value t_whole = Value::TIME(0, 0, 0, 0);
	Value t_micro = Value::TIME(0, 0, 0, 123456);
	Value t_24 = Value::TIME(24, 0, 0, 0);
	Value ts_ad = Value::TIMESTAMP(Timestamp::FromDatetime(Date::FromDate(2026, 6, 15), Time::FromTime(12, 34, 56, 123456)));
	Value ts_whole = Value::TIMESTAMP(Timestamp::FromDatetime(Date::FromDate(2026, 6, 15), Time::FromTime(12, 34, 56, 0)));
	Value ts_bc = Value::TIMESTAMP(Timestamp::FromDatetime(Date::FromDate(-44, 3, 15), Time::FromTime(12, 34, 56, 0)));
	Value tstz = Value::TIMESTAMPTZ(
	    timestamp_tz_t(Timestamp::FromDatetime(Date::FromDate(2026, 6, 15), Time::FromTime(12, 34, 56, 123456))));

	Require(FormatODBCTimestampTZUTC(tstz.GetValueUnsafe<timestamp_tz_t>(), true) == "2026-06-15 12:34:56.123456 +00:00",
	        "FormatODBCTimestampTZUTC space");
	Require(FormatODBCTimestampTZUTC(tstz.GetValueUnsafe<timestamp_tz_t>(), false) == "2026-06-15 12:34:56.123456+00:00",
	        "FormatODBCTimestampTZUTC no space");

	timestamp_tz_t parsed_tz;
	Require(ParseODBCTimestampTZText("2026-06-15 20:34:56.123456 +08:00", false, parsed_tz) &&
	            parsed_tz == tstz.GetValueUnsafe<timestamp_tz_t>(),
	        "ParseODBCTimestampTZText +08:00");
	Require(ParseODBCTimestampTZText("2026-06-15 07:34:56.123456 -05:00", false, parsed_tz) &&
	            parsed_tz == tstz.GetValueUnsafe<timestamp_tz_t>(),
	        "ParseODBCTimestampTZText -05:00");
	Require(ParseODBCTimestampTZText("2026-06-15 20:34:56.123456 +8", false, parsed_tz) &&
	            parsed_tz == tstz.GetValueUnsafe<timestamp_tz_t>(),
	        "ParseODBCTimestampTZText +8");
	Require(ParseODBCTimestampTZText("2026-06-15T12:34:56.123456Z", false, parsed_tz) &&
	            parsed_tz == tstz.GetValueUnsafe<timestamp_tz_t>(),
	        "ParseODBCTimestampTZText Z");
	Require(ParseODBCTimestampTZText("2026-06-15-20.34.56.123456 +08:00", false, parsed_tz) &&
	            parsed_tz == tstz.GetValueUnsafe<timestamp_tz_t>(),
	        "ParseODBCTimestampTZText Db2 dash + offset");
	Require(!ParseODBCTimestampTZText("2026-06-15 12:34:56.123456", false, parsed_tz),
	        "offset-less text rejected when allow_implicit_utc=false");
	Require(ParseODBCTimestampTZText("2026-06-15 12:34:56.123456", true, parsed_tz) &&
	            parsed_tz == tstz.GetValueUnsafe<timestamp_tz_t>(),
	        "offset-less text parsed as UTC when allow_implicit_utc=true");

	// 4. ODBCParameter bindings & guards
	{
		ODBCParameter p_date("q", d_ad);
		Require(p_date.c_type == SQL_C_TYPE_DATE && p_date.buffer_type == SQL_TYPE_DATE && p_date.column_size == 10 &&
		            p_date.decimal_digits == 0,
		        "DATE parameter binding metadata");
		SQL_DATE_STRUCT ds {};
		std::memcpy(&ds, p_date.bind_buffer.data(), sizeof(SQL_DATE_STRUCT));
		Require(ds.year == 2026 && ds.month == 6 && ds.day == 15, "DATE param struct values");

		ODBCParameter p_time_whole("q", t_whole);
		Require(p_time_whole.c_type == SQL_C_TYPE_TIME && p_time_whole.buffer_type == SQL_TYPE_TIME &&
		            p_time_whole.column_size == 8 && p_time_whole.decimal_digits == 0,
		        "TIME whole-second parameter metadata");

		ODBCParameter p_time_micro("q", t_micro);
		Require(p_time_micro.c_type == SQL_C_CHAR && p_time_micro.buffer_type == SQL_VARCHAR &&
		            string(p_time_micro.bind_buffer.data(), static_cast<size_t>(p_time_micro.bind_length)) ==
		                "00:00:00.123456",
		        "TIME microsecond parameter preserves fractional seconds via ISO text");

		ODBCParameter p_ts("q", ts_ad);
		Require(p_ts.c_type == SQL_C_TYPE_TIMESTAMP && p_ts.buffer_type == SQL_TYPE_TIMESTAMP &&
		            p_ts.column_size == 26 && p_ts.decimal_digits == 6,
		        "TIMESTAMP parameter metadata DecimalDigits=6");
		SQL_TIMESTAMP_STRUCT tss {};
		std::memcpy(&tss, p_ts.bind_buffer.data(), sizeof(SQL_TIMESTAMP_STRUCT));
		Require(tss.year == 2026 && tss.fraction == 123456000U, "TIMESTAMP param fraction nanoseconds");

		ODBCParameter p_tz("q", tstz);
		Require(p_tz.c_type == SQL_C_CHAR && p_tz.buffer_type == SQL_VARCHAR &&
		            string(p_tz.bind_buffer.data(), static_cast<size_t>(p_tz.bind_length)) ==
		                "2026-06-15 12:34:56.123456 +00:00",
		        "TIMESTAMP_TZ parameter preserves +00:00 UTC text");
	}
	RequireConversionError([&] { ODBCParameter p("q", d_bc); }, "BC DATE param must throw");
	RequireConversionError([&] { ODBCParameter p("q", Value::DATE(date_t::infinity())); },
	                       "infinity DATE param must throw");
	RequireConversionError([&] { ODBCParameter p("q", t_24); }, "24:00:00 TIME param must throw");
	RequireConversionError([&] { ODBCParameter p("q", ts_bc); }, "BC TIMESTAMP param must throw");
	RequireConversionError([&] { ODBCParameter p("q", Value::TIMESTAMP(timestamp_t::infinity())); },
	                       "infinity TIMESTAMP param must throw");
	RequireConversionError([&] { ODBCParameter p("q", Value::TIMESTAMPTZ(timestamp_tz_t(timestamp_t::infinity()))); },
	                       "infinity TIMESTAMPTZ param must throw");

	// 5. TryTransformConstant & TransformFiltersWithFallback
	string sql_lit;
	Require(ODBCUtils::TryTransformConstant(Value::BOOLEAN(true), ODBCDialect::DAMENG, "", sql_lit) && sql_lit == "1",
	        "Dameng BOOLEAN pushdown");
	Require(ODBCUtils::TryTransformConstant(t_micro, ODBCDialect::DAMENG, "TIME", sql_lit) &&
	            sql_lit == "TIME '00:00:00.123456'",
	        "Dameng TIME micros pushdown");
	Require(!ODBCUtils::TryTransformConstant(t_micro, ODBCDialect::DB2, "TIME", sql_lit),
	        "Db2 TIME micros must fall back");
	Require(ODBCUtils::TryTransformConstant(t_whole, ODBCDialect::DB2, "TIME", sql_lit) && sql_lit == "TIME '00:00:00'",
	        "Db2 TIME whole-second pushdown");
	Require(!ODBCUtils::TryTransformConstant(t_micro, ODBCDialect::HANA, "TIME", sql_lit),
	        "HANA TIME micros must fall back");
	Require(!ODBCUtils::TryTransformConstant(ts_ad, ODBCDialect::HANA, "SECONDDATE", sql_lit),
	        "HANA SECONDDATE micros must fall back");
	Require(ODBCUtils::TryTransformConstant(ts_whole, ODBCDialect::HANA, "SECONDDATE", sql_lit) &&
	            sql_lit == "TIMESTAMP '2026-06-15 12:34:56'",
	        "HANA SECONDDATE whole-second pushdown");
	Require(ODBCUtils::TryTransformConstant(ts_ad, ODBCDialect::HANA, "LONGDATE", sql_lit) &&
	            sql_lit == "TIMESTAMP '2026-06-15 12:34:56.123456'",
	        "HANA LONGDATE micros pushdown");
	Require(ODBCUtils::TryTransformConstant(tstz, ODBCDialect::DAMENG, "TIMESTAMP WITH TIME ZONE", sql_lit) &&
	            sql_lit == "TIMESTAMP '2026-06-15 12:34:56.123456+00:00'",
	        "Dameng TIMESTAMP_TZ pushdown");
	Require(!ODBCUtils::TryTransformConstant(tstz, ODBCDialect::HANA, "", sql_lit),
	        "Non-Dameng TIMESTAMP_TZ pushdown must fall back");
	Require(!ODBCUtils::TryTransformConstant(d_bc, ODBCDialect::DAMENG, "DATE", sql_lit),
	        "BC DATE pushdown must fall back");
	Require(!ODBCUtils::TryTransformConstant(Value::TIMESTAMP(timestamp_t::infinity()), ODBCDialect::DB2, "", sql_lit),
	        "infinity TIMESTAMP pushdown must fall back");
	Require(!ODBCUtils::TryTransformConstant(t_24, ODBCDialect::DAMENG, "", sql_lit),
	        "24:00:00 TIME pushdown must fall back");

	{
		TableFilterSet filters;
		filters.filters[0] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::INTEGER(42));
		auto or_filter = make_uniq<ConjunctionOrFilter>();
		or_filter->child_filters.push_back(make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, t_whole));
		or_filter->child_filters.push_back(make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, t_micro));
		filters.filters[1] = std::move(or_filter);
		filters.filters[2] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, ts_ad);

		vector<column_t> col_ids = {0, 1, 2};
		vector<string> names = {"ID", "C_TIME", "C_SEC"};
		vector<string> rtypes = {"INTEGER", "TIME", "SECONDDATE"};

		bool has_unpushed = false;
		auto where = ODBCFilterPushdown::TransformFiltersWithFallback(col_ids, &filters, names, rtypes, ODBCDialect::HANA,
		                                                              has_unpushed);
		Require(where == "\"ID\" = 42", "Only pushable ID=42 should appear in WHERE (OR + SECONDDATE fall back)");
		Require(has_unpushed, "has_unpushed must be true when OR / SECONDDATE falls back");
	}

	std::cout << "PASS: odbc_temporal_test\n";
	return 0;
}
