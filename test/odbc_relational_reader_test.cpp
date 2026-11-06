#include "odbc_relational_reader.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace duckdb;

namespace {
struct FakeStatement {
	vector<vector<string>> rows;
	idx_t row = 0;
	SQLUSMALLINT column = 0;
	idx_t offset = 0;
	SQLULEN timeout = 0;
	bool truncated = false;
};
idx_t handles = 0;
bool fail_execute = false;
bool wrong_columns = false;
bool extra_results = false;
string vendor_value = "  expression('默认')  ";
bool fail_timeout = false;
bool fail_fetch = false;
bool wrong_identity = false;
bool large_default = false;
bool repeated_rows = false;
string observed_schema;
string observed_table;

void Require(bool value, const char *message) {
	if (!value)
		throw std::runtime_error(message);
}

template <class F>
void ExpectFailure(F &&fn, const char *message) {
	bool failed = false;
	try {
		fn();
	} catch (const std::exception &) {
		failed = true;
	}
	Require(failed, message);
	Require(handles == 0, "statement leaked after failure");
}

FakeStatement &Stmt(SQLHSTMT value) {
	return *static_cast<FakeStatement *>(value);
}

vector<string> Columns(const string &name, const string &ordinal) {
	vector<string> row(18);
	row[0] = "db";
	row[1] = wrong_identity ? "other" : "a_%";
	row[2] = "t_%";
	row[3] = name;
	row[5] = "VARCHAR";
	row[10] = "1";
	row[12] = "  '默认值'  ";
	row[16] = ordinal;
	if (large_default)
		row[12] = string(ODBCRelationalReader::WINDOW_LIMIT / 3 + 1, 'x');
	return row;
}
} // namespace

extern "C" {
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLCHAR *text, SQLINTEGER) {
	Require(string(reinterpret_cast<char *>(text)) == "fixed vendor metadata", "unexpected vendor query");
	if (fail_execute)
		return SQL_ERROR;
	Stmt(stmt).rows = {{"APP", vendor_value}};
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *columns) {
	*columns = wrong_columns ? 3 : 2;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLMoreResults(SQLHSTMT) {
	return extra_results ? SQL_SUCCESS : SQL_NO_DATA;
}
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE, SQLHANDLE *output) {
	Require(type == SQL_HANDLE_STMT, "unexpected handle type");
	*output = new FakeStatement();
	handles++;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE value) {
	delete static_cast<FakeStatement *>(value);
	handles--;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT) {
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER) {
	Require(attr == SQL_ATTR_QUERY_TIMEOUT, "unexpected statement attribute");
	if (fail_timeout)
		return SQL_ERROR;
	Stmt(stmt).timeout = reinterpret_cast<uintptr_t>(value);
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER, SQLPOINTER value, SQLINTEGER, SQLINTEGER *) {
	*static_cast<SQLULEN *>(value) = Stmt(stmt).timeout;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT type, SQLPOINTER value, SQLSMALLINT, SQLSMALLINT *length) {
	Require(type == SQL_SEARCH_PATTERN_ESCAPE, "unexpected info type");
	std::memcpy(value, "\\", 2);
	*length = 1;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE stmt, SQLSMALLINT, SQLCHAR *state, SQLINTEGER *native, SQLCHAR *,
                                SQLSMALLINT, SQLSMALLINT *length) {
	std::memcpy(state, Stmt(stmt).truncated ? "01004" : "HY000", 6);
	*native = 0;
	*length = 0;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) {
	auto &s = Stmt(stmt);
	if (fail_fetch)
		return SQL_ERROR;
	if (s.row >= s.rows.size())
		return SQL_NO_DATA;
	s.row++;
	s.column = 0;
	s.offset = 0;
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT column, SQLSMALLINT type, SQLPOINTER output, SQLLEN capacity,
                             SQLLEN *indicator) {
	auto &s = Stmt(stmt);
	Require(type == SQL_C_CHAR, "unexpected catalog transfer type");
	Require(column > s.column || (column == s.column && s.truncated), "non-monotonic SQLGetData");
	if (column != s.column) {
		s.column = column;
		s.offset = 0;
	}
	auto &value = s.rows.at(s.row - 1).at(column - 1);
	auto count = std::min<idx_t>(value.size() - s.offset, capacity - 1);
	std::memcpy(output, value.data() + s.offset, count);
	static_cast<char *>(output)[count] = '\0';
	s.offset += count;
	s.truncated = s.offset < value.size();
	*indicator = s.truncated ? SQL_NO_TOTAL : static_cast<SQLLEN>(count);
	return s.truncated ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
SQLRETURN SQL_API SQLTables(SQLHSTMT stmt, SQLCHAR *, SQLSMALLINT, SQLCHAR *schema, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                            SQLCHAR *, SQLSMALLINT) {
	observed_schema = reinterpret_cast<char *>(schema);
	Stmt(stmt).rows = {{"db", "a_%", "t_%", "TABLE"}};
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLColumns(SQLHSTMT stmt, SQLCHAR *, SQLSMALLINT, SQLCHAR *schema, SQLSMALLINT, SQLCHAR *table,
                             SQLSMALLINT, SQLCHAR *, SQLSMALLINT) {
	observed_schema = reinterpret_cast<char *>(schema);
	observed_table = reinterpret_cast<char *>(table);
	Stmt(stmt).rows = {Columns("second", "2"), Columns("first", "1")};
	if (repeated_rows)
		Stmt(stmt).rows.assign(25000, Columns("field", "1"));
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT stmt, SQLCHAR *, SQLSMALLINT, SQLCHAR *schema, SQLSMALLINT, SQLCHAR *table,
                                 SQLSMALLINT) {
	Require(string(reinterpret_cast<char *>(schema)) == "a_%", "PK must use literal schema");
	Require(string(reinterpret_cast<char *>(table)) == "t_%", "PK must use literal table");
	Stmt(stmt).rows = {{"db", "a_%", "t_%", "second", "2", "pk"}, {"db", "a_%", "t_%", "first", "1", "pk"}};
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT stmt, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                 SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT) {
	Stmt(stmt).rows = {{"db", "ref", "parent", "id", "db", "a_%", "t_%", "first", "1", "0", "3", "fk", "pk", "7"}};
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLStatistics(SQLHSTMT stmt, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                SQLUSMALLINT, SQLUSMALLINT accuracy) {
	Require(accuracy == SQL_QUICK, "statistics must not recompute source data");
	Stmt(stmt).rows = {{"db", "a_%", "t_%", "0", "", "ix", "3", "2", "second", "D", "", "", ""},
	                   {"db", "a_%", "t_%", "0", "", "ix", "3", "1", "first", "A", "", "", "x > 0"}};
	return SQL_SUCCESS;
}
}

int main() {
	try {
		auto deadline = [] {
			return std::chrono::steady_clock::now() + std::chrono::seconds(60);
		};
		ODBCRelationalReader reader(
		    nullptr, [] {}, deadline());
		auto objects = reader.Objects("a_%");
		Require(objects.size() == 1 && observed_schema == "a\\_\\%", "schema pattern not escaped");
		auto columns = reader.ReadTable(objects[0], "columnProperties");
		Require(columns.size() == 2 && columns[0].values.size() == 8, "column ABI mismatch");
		Require(columns[0].values[2] == "first" && columns[0].values[6] == "  '默认值'  ",
		        "column sort/default changed");
		Require(observed_table == "t\\_\\%", "table pattern not escaped");
		auto keys = reader.ReadTable(objects[0], "constraints");
		Require(keys.size() == 3 && keys[0].values.size() == 13, "constraint ABI mismatch");
		Require(keys[0].values[10] == "cascade" && keys[1].values[5] == "1", "FK mapping or PK order changed");
		auto indexes = reader.ReadTable(objects[0], "indexes");
		Require(indexes.size() == 2 && indexes[0].values.size() == 10, "index ABI mismatch");
		Require(indexes[0].values[5] == "true" && indexes[0].values[6] == "1", "index uniqueness or order changed");
		Require(handles == 0, "statement leaked on success");
		auto vendor = reader.ReadQuery("fixed vendor metadata", 2);
		Require(vendor.size() == 1 && vendor[0].values[1] == vendor_value, "vendor expression changed");
		Require(handles == 0, "vendor statement leaked on success");
		fail_execute = true;
		ExpectFailure([&] { reader.ReadQuery("fixed vendor metadata", 2); }, "vendor execute failure ignored");
		fail_execute = false;
		wrong_columns = true;
		ExpectFailure([&] { reader.ReadQuery("fixed vendor metadata", 2); }, "vendor column mismatch ignored");
		wrong_columns = false;
		extra_results = true;
		ExpectFailure([&] { reader.ReadQuery("fixed vendor metadata", 2); }, "vendor extra results ignored");
		extra_results = false;
		vendor_value = string(18000, 'x');
		vendor = reader.ReadQuery("fixed vendor metadata", 2);
		Require(vendor[0].values[1] == vendor_value, "chunked vendor expression truncated");
		vendor_value = string(ODBCRelationalReader::WINDOW_LIMIT / 3 + 1, 'x');
		ExpectFailure([&] { reader.ReadQuery("fixed vendor metadata", 2); }, "oversized vendor expression accepted");
		fail_timeout = true;
		ExpectFailure([&] { reader.Objects("a_%"); }, "timeout setup failure ignored");
		fail_timeout = false;
		fail_fetch = true;
		ExpectFailure([&] { reader.Objects("a_%"); }, "fetch failure ignored");
		fail_fetch = false;
		wrong_identity = true;
		ExpectFailure([&] { reader.ReadTable(objects[0], "columnProperties"); }, "wrong identity accepted");
		wrong_identity = false;
		large_default = true;
		ExpectFailure([&] { reader.ReadTable(objects[0], "columnProperties"); }, "oversized value accepted");
		large_default = false;
		repeated_rows = true;
		ExpectFailure([&] { reader.ReadTable(objects[0], "columnProperties"); }, "unbounded table window");
		repeated_rows = false;
		ODBCRelationalReader expired(
		    nullptr, [] {}, std::chrono::steady_clock::now());
		ExpectFailure([&] { expired.Objects("a_%"); }, "expired deadline ignored");
		ODBCRelationalReader cancelled(
		    nullptr, [] { throw std::runtime_error("cancelled"); }, deadline());
		ExpectFailure([&] { cancelled.Objects("a_%"); }, "cancellation ignored");
		std::cout << "PASS: ODBC relational ABI, monotonic reads, identity, window, timeout, cancellation, handles\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "FAIL: " << error.what() << '\n';
		return 1;
	}
}
