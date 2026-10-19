#pragma once

#include "odbc_utils.hpp"

#include <chrono>
#include <functional>

namespace duckdb {

struct ODBCRelationalObject {
	string catalog;
	string schema;
	string name;
};

struct ODBCRelationalRow {
	vector<string> values;
};

class ODBCRelationalReader {
public:
	static constexpr idx_t WINDOW_LIMIT = 16 * 1024 * 1024;

	ODBCRelationalReader(SQLHDBC connection, std::function<void()> check_interrupt,
	                     std::chrono::steady_clock::time_point deadline);
	vector<ODBCRelationalObject> Objects(const string &schema);
	vector<ODBCRelationalRow> ReadTable(const ODBCRelationalObject &object, const string &category);
	void Check() const;

private:
	SQLHDBC connection;
	std::function<void()> check_interrupt;
	std::chrono::steady_clock::time_point deadline;

	ODBCStatementPtr Statement();
	string ReadString(SQLHSTMT statement, SQLUSMALLINT column, idx_t limit = WINDOW_LIMIT / 3);
	string Pattern(const string &identifier);
	void ReadCatalog(SQLHSTMT statement, SQLRETURN status, const string &operation,
	                 const std::function<void(SQLHSTMT)> &row);
};

} // namespace duckdb
