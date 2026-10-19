#include "odbc_result.hpp"
#include "odbc_connection.hpp"

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
SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT, SQLUSMALLINT, SQLCHAR *name, SQLSMALLINT, SQLSMALLINT *length,
                                 SQLSMALLINT *type, SQLULEN *size, SQLSMALLINT *scale, SQLSMALLINT *nullable) {
	std::memcpy(name, "value", 6);
	*length = 5;
	*type = SQL_LONGVARCHAR;
	*size = 0;
	*scale = 0;
	*nullable = 1;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER name, SQLSMALLINT,
                                  SQLSMALLINT *length, SQLLEN *) {
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
	auto &value = s.row++ == 0 ? first_value : second_value;
	auto length = std::min<idx_t>(value.size(), s.capacity - 1);
	std::memcpy(s.buffer, value.data(), length);
	s.buffer[length] = 0;
	s.offset = length;
	*s.indicator = value.size();
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
	*indicator = length;
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
		FakeResult statement;
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
		std::cout << "PASS: multi-part CLOB, stable bound buffer, subsequent Fetch, truncated read failure\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "FAIL: " << error.what() << '\n';
		return 1;
	}
}
