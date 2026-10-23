#include "odbc_result.hpp"
#include "odbc_connection.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace duckdb;
namespace {
struct FakeResult {
	char *buffer = nullptr;
	SQLLEN capacity = 0;
	SQLLEN *indicator = nullptr;
	idx_t row = 0;
	idx_t offset = 0;
	bool fail = false;
	bool decimal = false;
	bool narrow_indicator = false;
	bool null_first = false;
	bool no_total = false;
	bool unwritten = false;
	uint32_t precision = 31;
	SQLSMALLINT scale = 6;
	string decimal_value = "-999.99";
};
string first_value(20000, 'a');
string second_value = "second-row";
void Require(bool test, const char *message) {
	if (!test)
		throw std::runtime_error(message);
}
FakeResult &Get(SQLHSTMT stmt) {
	return *static_cast<FakeResult *>(stmt);
}
} // namespace
extern "C" {
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *count) {
	*count = 1;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT, SQLCHAR *name, SQLSMALLINT, SQLSMALLINT *length,
                                 SQLSMALLINT *type, SQLULEN *size, SQLSMALLINT *scale, SQLSMALLINT *nullable) {
	if (Get(stmt).decimal) {
		Require(*size == 0, "SQLDescribeCol output size must be fully initialized for narrow CLI drivers");
		std::memcpy(size, &Get(stmt).precision, sizeof(uint32_t));
		std::memcpy(name, "value", 6);
		*length = 5;
		*type = SQL_DECIMAL;
		*scale = Get(stmt).scale;
		*nullable = 1;
		return SQL_SUCCESS;
	}
	std::memcpy(name, "value", 6);
	*length = 5;
	*type = SQL_LONGVARCHAR;
	*size = 0;
	*scale = 0;
	*nullable = 1;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER name, SQLSMALLINT,
                                  SQLSMALLINT *length, SQLLEN *) {
	if (Get(stmt).decimal) {
		std::memcpy(name, "DECIMAL", 8);
		*length = 7;
		return SQL_SUCCESS;
	}
	std::memcpy(name, "CLOB", 5);
	*length = 4;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER buffer, SQLLEN capacity,
                             SQLLEN *indicator) {
	auto &s = Get(stmt);
	s.buffer = static_cast<char *>(buffer);
	s.capacity = capacity;
	s.indicator = indicator;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) {
	auto &s = Get(stmt);
	if (s.row == 2)
		return SQL_NO_DATA;
	if (s.null_first && s.row == 0) {
		int32_t null_indicator = SQL_NULL_DATA;
		std::memcpy(s.indicator, &null_indicator, sizeof(null_indicator));
		s.row++;
		return SQL_SUCCESS;
	}
	auto &value = s.decimal ? s.decimal_value : (s.row == 0 ? first_value : second_value);
	s.row++;
	auto length = std::min<idx_t>(value.size(), s.capacity - 1);
	std::memcpy(s.buffer, value.data(), length);
	s.buffer[length] = 0;
	s.offset = length;
	if (!s.unwritten) {
		if (s.narrow_indicator) {
			int32_t indicator = s.no_total && length < value.size() ? SQL_NO_TOTAL : value.size();
			std::memcpy(s.indicator, &indicator, sizeof(indicator));
		} else {
			*s.indicator = value.size();
		}
	}
	return length == value.size() ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO;
}
SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER buffer, SQLLEN capacity,
                             SQLLEN *indicator) {
	auto &s = Get(stmt);
	if (s.fail)
		return SQL_ERROR;
	auto length = std::min<idx_t>(first_value.size() - s.offset, capacity - 1);
	std::memcpy(buffer, first_value.data() + s.offset, length);
	static_cast<char *>(buffer)[length] = 0;
	s.offset += length;
	if (s.narrow_indicator) {
		int32_t narrow = length;
		std::memcpy(indicator, &narrow, sizeof(narrow));
	} else {
		*indicator = length;
	}
	return s.offset < first_value.size() ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
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
// 无连接结果测试不创建原生连接；busy 不参与此测试。
namespace duckdb {
void ODBCConnection::ReleaseConnection() {
}
} // namespace duckdb

int main() {
	try {
		for (auto precision : {5U, 31U}) {
			FakeResult narrow;
			narrow.decimal = true;
			narrow.precision = precision;
			narrow.scale = precision == 5 ? 2 : 6;
			ODBCTypeConfig config;
			ODBCResult result("decimal", ODBCStatementPtr(&narrow, ODBCStatementDelete), config, idx_t(-1), 1, nullptr);
			Require(result.Fields()[0].duckdb_type == LogicalType::DECIMAL(precision, narrow.scale),
			        "narrow CLI descriptor lost DECIMAL precision/scale");
			Require(result.Fields()[0].bind_buffer.size() >= 8,
			        "DECIMAL text buffer must include sign, decimal point and terminator");
			Require(result.NextChunk().GetValue(0, 0).ToString() == (precision == 5 ? "-999.99" : "-999.990000"),
			        "signed DECIMAL value changed");
		}
		for (auto text : {"0.000001", "-0.000001", "+000.000001"}) {
			FakeResult small;
			small.decimal = true;
			small.decimal_value = text;
			ODBCTypeConfig config;
			ODBCResult result("decimal", ODBCStatementPtr(&small, ODBCStatementDelete), config, idx_t(-1), 1, nullptr);
			Require(result.NextChunk().GetValue(0, 0).ToString() == (text[0] == '-' ? "-0.000001" : "0.000001"),
			        "leading-zero DECIMAL value changed");
		}
		for (auto text : {"1.239", "1000", "1x", "NaN"}) {
			FakeResult invalid;
			invalid.decimal = true;
			invalid.precision = 5;
			invalid.scale = 2;
			invalid.decimal_value = text;
			bool threw = false;
			try {
				ODBCTypeConfig config;
				ODBCResult result("decimal", ODBCStatementPtr(&invalid, ODBCStatementDelete), config, idx_t(-1), 1,
				                  nullptr);
			} catch (const ConversionException &) {
				threw = true;
			}
			Require(threw, "malformed or lossy DECIMAL transfer accepted");
		}
		{
			FakeResult nullable;
			nullable.decimal = true;
			nullable.narrow_indicator = true;
			nullable.null_first = true;
			ODBCTypeConfig config;
			ODBCResult result("null", ODBCStatementPtr(&nullable, ODBCStatementDelete), config, idx_t(-1), 1, nullptr);
			auto &chunk = result.NextChunk();
			Require(chunk.GetValue(0, 0).IsNull() && chunk.GetValue(0, 1).ToString() == "-999.990000",
			        "narrow NULL indicator or following decimal row corrupted");
		}
		{
			FakeResult unwritten;
			unwritten.unwritten = true;
			bool threw = false;
			try {
				ODBCTypeConfig config;
				ODBCResult result("unwritten", ODBCStatementPtr(&unwritten, ODBCStatementDelete), config, idx_t(-1), 1,
				                  nullptr);
			} catch (const IOException &) {
				threw = true;
			}
			Require(threw, "unwritten result indicator silently accepted");
		}
		FakeResult statement;
		statement.narrow_indicator = true;
		statement.no_total = true;
		ODBCTypeConfig config;
		ODBCResult result("test", ODBCStatementPtr(&statement, ODBCStatementDelete), config, idx_t(-1), 1, nullptr);
		Require(result.Next() && result.GetString(0) == first_value, "long CLOB value corrupted");
		Require(result.Next() && result.GetString(0) == second_value, "second Fetch buffer invalidated");
		Require(!result.Next(), "unexpected extra row");
		FakeResult failed;
		failed.fail = true;
		bool threw = false;
		try {
			ODBCResult bad("test", ODBCStatementPtr(&failed, ODBCStatementDelete), config, idx_t(-1), 1, nullptr);
		} catch (const std::exception &) {
			threw = true;
		}
		Require(threw, "truncated value read failure silently accepted");
		std::cout << "PASS: narrow CLI descriptor/NULL/NO_TOTAL indicators, signed DECIMAL buffer, multi-part CLOB, "
		             "stable bound "
		             "buffer, subsequent Fetch, truncated read failure\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "FAIL: " << error.what() << '\n';
		return 1;
	}
}
