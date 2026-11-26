#include "odbc_result.hpp"
#include "odbc_connection.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace duckdb;

namespace {

struct FakeTemporalResult {
	SQLSMALLINT sql_type = SQL_TYPE_TIMESTAMP;
	string type_name = "TIMESTAMP";
	SQLULEN column_size = 26;
	SQLSMALLINT decimal_digits = 6;
	bool send_struct_ts = false;
	SQL_TIMESTAMP_STRUCT ts_struct = {2026, 6, 15, 0, 0, 0, 123456000U};
	string char_value;
	char *bound_buf = nullptr;
	SQLLEN bound_cap = 0;
	SQLLEN *indicator = nullptr;
	bool fetched = false;
};

FakeTemporalResult &Get(SQLHSTMT stmt) {
	return *static_cast<FakeTemporalResult *>(stmt);
}

void Require(bool cond, const char *msg) {
	if (!cond) {
		throw std::runtime_error(msg);
	}
}

} // namespace

extern "C" {
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *count) {
	*count = 1;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT, SQLCHAR *name, SQLSMALLINT, SQLSMALLINT *length,
                                 SQLSMALLINT *type, SQLULEN *size, SQLSMALLINT *scale, SQLSMALLINT *nullable) {
	auto &s = Get(stmt);
	std::memcpy(name, "col", 4);
	*length = 3;
	*type = s.sql_type;
	*size = s.column_size;
	*scale = s.decimal_digits;
	*nullable = 1;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER name, SQLSMALLINT,
                                  SQLSMALLINT *length, SQLLEN *) {
	auto &s = Get(stmt);
	std::memcpy(name, s.type_name.c_str(), s.type_name.size() + 1);
	*length = static_cast<SQLSMALLINT>(s.type_name.size());
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER buffer, SQLLEN capacity,
                             SQLLEN *indicator) {
	auto &s = Get(stmt);
	s.bound_buf = static_cast<char *>(buffer);
	s.bound_cap = capacity;
	s.indicator = indicator;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) {
	auto &s = Get(stmt);
	if (s.fetched) {
		return SQL_NO_DATA;
	}
	s.fetched = true;
	if (s.send_struct_ts) {
		Require(s.bound_buf != nullptr && static_cast<size_t>(s.bound_cap) >= sizeof(SQL_TIMESTAMP_STRUCT),
		        "expected SQL_TIMESTAMP_STRUCT bind buffer");
		std::memcpy(s.bound_buf, &s.ts_struct, sizeof(SQL_TIMESTAMP_STRUCT));
		*s.indicator = sizeof(SQL_TIMESTAMP_STRUCT);
		return SQL_SUCCESS;
	}
	Require(s.bound_buf != nullptr && static_cast<size_t>(s.bound_cap) > s.char_value.size(),
	        "expected SQL_C_CHAR bind buffer");
	std::memcpy(s.bound_buf, s.char_value.c_str(), s.char_value.size() + 1);
	*s.indicator = static_cast<SQLLEN>(s.char_value.size());
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT) {
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE) {
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLCancel(SQLHSTMT) {
	return SQL_SUCCESS;
}
}

namespace duckdb {
void ODBCConnection::ReleaseConnection() {
}
} // namespace duckdb

int main() {
	try {
		ODBCTypeConfig cfg;

		{
			FakeTemporalResult unknown_ts;
			unknown_ts.sql_type = SQL_TYPE_TIMESTAMP;
			unknown_ts.type_name = "FUTURE_VENDOR_TS";
			unknown_ts.send_struct_ts = true;
			ODBCResult res("unknown_ts", ODBCStatementPtr(&unknown_ts, ODBCStatementDelete), cfg, idx_t(-1), 1, nullptr);
			Require(res.Fields()[0].duckdb_type == LogicalType::TIMESTAMP,
			        "unknown SQL_TYPE_TIMESTAMP must resolve to TIMESTAMP");
			auto v = res.NextChunk().GetValue(0, 0);
			Require(!v.IsNull() && v.ToString() == "2026-06-15 00:00:00.123456",
			        "midnight microsecond TIMESTAMP was altered or turned NULL");
		}

		{
			FakeTemporalResult hana_longdate;
			hana_longdate.sql_type = SQL_TYPE_TIMESTAMP;
			hana_longdate.type_name = "LONGDATE";
			hana_longdate.send_struct_ts = true;
			ODBCResult res("longdate", ODBCStatementPtr(&hana_longdate, ODBCStatementDelete), cfg, idx_t(-1), 1,
			               nullptr);
			Require(res.Fields()[0].duckdb_type == LogicalType::TIMESTAMP, "HANA LONGDATE must map to TIMESTAMP");
			auto v = res.NextChunk().GetValue(0, 0);
			Require(v.ToString() == "2026-06-15 00:00:00.123456", "HANA LONGDATE value mismatch");
		}

		{
			FakeTemporalResult db2_time_text;
			db2_time_text.sql_type = SQL_TYPE_TIME;
			db2_time_text.type_name = "TIME";
			db2_time_text.char_value = "00.00.00";
			ODBCTypeConfig cfg_text;
			cfg_text.time_as_time = false;
			ODBCResult res("time_as_text", ODBCStatementPtr(&db2_time_text, ODBCStatementDelete), cfg_text, idx_t(-1),
			               1, nullptr);
			Require(res.Fields()[0].duckdb_type == LogicalType::VARCHAR && res.Fields()[0].c_type == SQL_C_CHAR,
			        "time_as_time=false should bind SQL_C_CHAR VARCHAR");
			Require(res.NextChunk().GetValue(0, 0).ToString() == "00:00:00",
			        "Db2 TIME dot text must normalize cleanly without raw SQL_TIME_STRUCT cast");
		}

		{
			FakeTemporalResult dm_ltlz;
			dm_ltlz.sql_type = SQL_TYPE_TIMESTAMP;
			dm_ltlz.type_name = "TIMESTAMP(6) WITH LOCAL TIME ZONE";
			dm_ltlz.char_value = "2026-06-15 12:34:56.123456";
			ODBCResult res("ltlz", ODBCStatementPtr(&dm_ltlz, ODBCStatementDelete), cfg, idx_t(-1), 1, nullptr);
			Require(res.Fields()[0].duckdb_type == LogicalType::TIMESTAMP_TZ && res.Fields()[0].is_local_tz,
			        "DM LTLZ must map to TIMESTAMP_TZ with is_local_tz");
			Require(res.NextChunk().GetValue(0, 0).ToString() == "2026-06-15 12:34:56.123456+00",
			        "DM LTLZ implicit UTC value mismatch");
		}

		{
			FakeTemporalResult bad_tstz;
			bad_tstz.sql_type = SQL_TYPE_TIMESTAMP;
			bad_tstz.type_name = "TIMESTAMP(6) WITH TIME ZONE";
			bad_tstz.char_value = "2026-06-15 12:34:56.123456";
			bool threw = false;
			try {
				ODBCResult bad("tstz_missing_offset", ODBCStatementPtr(&bad_tstz, ODBCStatementDelete), cfg, idx_t(-1),
				               1, nullptr);
			} catch (const std::exception &) {
				threw = true;
			}
			Require(threw, "TIMESTAMP WITH TIME ZONE without explicit offset must fail");
		}

		std::cout << "PASS: odbc_result_temporal_test\n";
		return 0;
	} catch (const std::exception &err) {
		std::cerr << "FAIL: " << err.what() << '\n';
		return 1;
	}
}
