//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_temporal_utils.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "odbc_dialect.hpp"

#include <cctype>
#include <cstdint>
#include <sql.h>
#include <sqlext.h>

namespace duckdb {

static constexpr const char *ODBC_DAMENG_SESSION_TEMPORAL_SQL = "SET TIME ZONE '+00:00'";

inline const char *ODBCSessionTemporalInitSQL(ODBCDialect dialect) {
	switch (dialect) {
	case ODBCDialect::DAMENG:
		return ODBC_DAMENG_SESSION_TEMPORAL_SQL;
	case ODBCDialect::DB2:
	case ODBCDialect::HANA:
		return "";
	}
	return "";
}

inline string NormalizeODBCTypeName(const string &raw) {
	string stripped;
	stripped.reserve(raw.size());
	int paren_depth = 0;
	for (char c : raw) {
		if (c == '(') {
			paren_depth++;
			continue;
		}
		if (c == ')') {
			if (paren_depth > 0) {
				paren_depth--;
			}
			continue;
		}
		if (paren_depth == 0) {
			stripped.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
		}
	}
	string normalized;
	normalized.reserve(stripped.size());
	bool in_space = true;
	for (char c : stripped) {
		if (std::isspace(static_cast<unsigned char>(c))) {
			if (!in_space) {
				normalized.push_back(' ');
				in_space = true;
			}
		} else {
			normalized.push_back(c);
			in_space = false;
		}
	}
	if (!normalized.empty() && normalized.back() == ' ') {
		normalized.pop_back();
	}
	return normalized;
}

inline bool IsODBCLocalTimeZoneTypeName(const string &raw) {
	auto normalized = NormalizeODBCTypeName(raw);
	return (StringUtil::Contains(normalized, "timestamp") || StringUtil::Contains(normalized, "datetime")) &&
	       StringUtil::Contains(normalized, "with local time zone");
}

inline bool IsODBCTimeWithTimeZoneTypeName(const string &raw) {
	auto normalized = NormalizeODBCTypeName(raw);
	return normalized == "time with time zone" || normalized == "timetz";
}

inline bool IsODBCYearInRange(int32_t year) {
	return year >= 1 && year <= 9999;
}

inline bool IsODBCTimeInRange(int32_t hour, int32_t minute, int32_t second, int32_t micros) {
	return hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59 && second >= 0 && second <= 59 && micros >= 0 &&
	       micros <= 999999;
}

inline string FormatODBCYear(int32_t year) {
	if (year < 0) {
		uint32_t mag = static_cast<uint32_t>(-static_cast<int64_t>(year));
		return StringUtil::Format("-%04u", mag);
	}
	return StringUtil::Format("%04u", static_cast<uint32_t>(year));
}

inline string FormatODBCMicrosSuffix(int32_t micros) {
	if (micros <= 0) {
		return "";
	}
	return StringUtil::Format(".%06d", micros);
}

inline string FormatODBCDateParts(int32_t year, int32_t month, int32_t day) {
	return StringUtil::Format("%s-%02d-%02d", FormatODBCYear(year), month, day);
}

inline string FormatODBCTimeParts(int32_t hour, int32_t minute, int32_t second, int32_t micros) {
	return StringUtil::Format("%02d:%02d:%02d%s", hour, minute, second, FormatODBCMicrosSuffix(micros));
}

inline string FormatODBCDateStruct(const SQL_DATE_STRUCT &d) {
	if (d.month < 1 || d.month > 12 || d.day < 1 || d.day > 31) {
		throw ConversionException("Invalid ODBC DATE struct (%d-%u-%u)", d.year, d.month, d.day);
	}
	return FormatODBCDateParts(d.year, d.month, d.day);
}

inline string FormatODBCTimeStruct(const SQL_TIME_STRUCT &t) {
	if (!IsODBCTimeInRange(t.hour, t.minute, t.second, 0)) {
		throw ConversionException("Invalid ODBC TIME struct (%u:%u:%u)", t.hour, t.minute, t.second);
	}
	return FormatODBCTimeParts(t.hour, t.minute, t.second, 0);
}

inline string FormatODBCTimestampStruct(const SQL_TIMESTAMP_STRUCT &ts) {
	if (ts.month < 1 || ts.month > 12 || ts.day < 1 || ts.day > 31 || ts.hour > 23 || ts.minute > 59 ||
	    ts.second > 59 || ts.fraction >= 1000000000U) {
		throw ConversionException("Invalid ODBC TIMESTAMP struct (%d-%u-%u %u:%u:%u.%u)", ts.year, ts.month, ts.day,
		                          ts.hour, ts.minute, ts.second, ts.fraction);
	}
	int32_t micros = static_cast<int32_t>(ts.fraction / 1000U);
	return FormatODBCDateParts(ts.year, ts.month, ts.day) + " " +
	       FormatODBCTimeParts(ts.hour, ts.minute, ts.second, micros);
}

inline string FormatODBCTimestampTZUTC(timestamp_tz_t val, bool space_before_offset = true) {
	timestamp_t ts(val.value);
	if (!Timestamp::IsFinite(ts)) {
		throw ConversionException("ODBC TIMESTAMPTZ value must be finite: %s", Timestamp::ToString(ts));
	}
	date_t dd;
	dtime_t dt;
	Timestamp::Convert(ts, dd, dt);
	int32_t year, month, day, hour, minute, second, micros;
	Date::Convert(dd, year, month, day);
	if (!IsODBCYearInRange(year)) {
		throw ConversionException("ODBC TIMESTAMPTZ year out of supported range (1..9999): %s",
		                          Timestamp::ToString(ts));
	}
	Time::Convert(dt, hour, minute, second, micros);
	return FormatODBCDateParts(year, month, day) + " " + FormatODBCTimeParts(hour, minute, second, micros) +
	       (space_before_offset ? " +00:00" : "+00:00");
}

inline string NormalizeODBCTimeText(string text) {
	StringUtil::Trim(text);
	if (text.size() >= 8 && std::isdigit(static_cast<unsigned char>(text[0])) &&
	    std::isdigit(static_cast<unsigned char>(text[1])) && text[2] == '.' &&
	    std::isdigit(static_cast<unsigned char>(text[3])) && std::isdigit(static_cast<unsigned char>(text[4])) &&
	    text[5] == '.' && std::isdigit(static_cast<unsigned char>(text[6])) &&
	    std::isdigit(static_cast<unsigned char>(text[7]))) {
		text[2] = ':';
		text[5] = ':';
	}
	return text;
}

inline string NormalizeODBCTimestampText(string text) {
	StringUtil::Trim(text);
	if (text.size() >= 19 && std::isdigit(static_cast<unsigned char>(text[0])) && text[4] == '-' && text[7] == '-' &&
	    text[10] == '-' && std::isdigit(static_cast<unsigned char>(text[11])) && text[13] == '.' && text[16] == '.') {
		text[10] = ' ';
		text[13] = ':';
		text[16] = ':';
	}
	return text;
}

inline bool ParseODBCTimestampTZText(const string &raw, bool allow_implicit_utc, timestamp_tz_t &out) {
	auto text = NormalizeODBCTimestampText(raw);
	if (text.empty()) {
		return false;
	}
	auto date_time_sep = text.find_first_of(" T");
	if (date_time_sep == string::npos) {
		return false;
	}
	string normalized;
	auto offset_pos = text.find_first_of("+-Zz", date_time_sep + 1);
	if (offset_pos == string::npos) {
		if (!allow_implicit_utc) {
			return false;
		}
		normalized = text + "+00:00";
	} else {
		auto prefix = text.substr(0, offset_pos);
		StringUtil::Trim(prefix);
		auto suffix = text.substr(offset_pos);
		StringUtil::Trim(suffix);
		if (suffix == "z" || suffix == "Z") {
			suffix = "Z";
		} else if ((suffix[0] == '+' || suffix[0] == '-') && suffix.size() >= 2 &&
		           std::isdigit(static_cast<unsigned char>(suffix[1])) &&
		           (suffix.size() == 2 || !std::isdigit(static_cast<unsigned char>(suffix[2])))) {
			suffix.insert(1, 1, '0');
		}
		normalized = prefix + suffix;
	}
	timestamp_t ts_val;
	bool has_offset = false;
	string_t tz(nullptr, 0);
	if (Timestamp::TryConvertTimestampTZ(normalized.c_str(), normalized.size(), ts_val, true, has_offset, tz) !=
	        TimestampCastResult::SUCCESS ||
	    !has_offset || tz.GetSize() != 0 || !Timestamp::IsFinite(ts_val)) {
		return false;
	}
	out = timestamp_tz_t(ts_val.value);
	return true;
}

} // namespace duckdb
