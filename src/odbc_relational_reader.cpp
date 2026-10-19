#include "odbc_relational_reader.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace duckdb {

static SQLCHAR *ODBCText(const string &value) {
	return reinterpret_cast<SQLCHAR *>(const_cast<char *>(value.c_str()));
}

static SQLCHAR *OptionalODBCText(const string &value) {
	return value.empty() ? nullptr : ODBCText(value);
}

static void CatalogError(SQLHSTMT statement, const string &operation) {
	SQLCHAR state[6] = {0};
	SQLINTEGER native_error = 0;
	SQLSMALLINT length = 0;
	// 不携带驱动原始消息，避免其中的连接参数或默认表达式泄露。
	SQLGetDiagRec(SQL_HANDLE_STMT, statement, 1, state, &native_error, nullptr, 0, &length);
	throw IOException("ODBC relational catalog %s failed: SQLState=%s NativeError=%d", operation,
	                  reinterpret_cast<const char *>(state), native_error);
}

static void Account(idx_t &used, idx_t payload) {
	if (payload > ODBCRelationalReader::WINDOW_LIMIT / 3 || used > ODBCRelationalReader::WINDOW_LIMIT - payload * 3) {
		throw IOException("ODBC relational metadata RESULT_LIMIT: 16 MiB estimated window exceeded");
	}
	used += payload * 3;
}

ODBCRelationalReader::ODBCRelationalReader(SQLHDBC connection_p, std::function<void()> check_interrupt_p,
                                           std::chrono::steady_clock::time_point deadline_p)
    : connection(connection_p), check_interrupt(std::move(check_interrupt_p)), deadline(deadline_p) {
}

void ODBCRelationalReader::Check() const {
	check_interrupt();
	if (std::chrono::steady_clock::now() >= deadline) {
		throw IOException("ODBC relational metadata deadline exceeded");
	}
}

ODBCStatementPtr ODBCRelationalReader::Statement() {
	Check();
	SQLHSTMT raw = SQL_NULL_HSTMT;
	if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection, &raw))) {
		throw IOException("Failed to allocate ODBC relational catalog statement");
	}
	ODBCStatementPtr statement(raw, ODBCStatementDelete);
	auto remaining =
	    std::chrono::duration_cast<std::chrono::seconds>(deadline - std::chrono::steady_clock::now()).count();
	if (remaining < 1) {
		throw IOException("ODBC relational metadata deadline exceeded");
	}
	if (!SQL_SUCCEEDED(SQLSetStmtAttr(raw, SQL_ATTR_QUERY_TIMEOUT,
	                                  reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(remaining)), 0))) {
		CatalogError(raw, "set query timeout");
	}
	SQLULEN actual_timeout = 0;
	if (!SQL_SUCCEEDED(SQLGetStmtAttr(raw, SQL_ATTR_QUERY_TIMEOUT, &actual_timeout, sizeof(actual_timeout), nullptr)) ||
	    actual_timeout == 0 || actual_timeout > static_cast<SQLULEN>(remaining)) {
		throw IOException("ODBC relational catalog driver cannot enforce remaining query timeout");
	}
	return statement;
}

string ODBCRelationalReader::ReadString(SQLHSTMT statement, SQLUSMALLINT column, idx_t limit) {
	string result;
	while (true) {
		Check();
		char buffer[512] = {0};
		SQLLEN indicator = 0;
		auto status = SQLGetData(statement, column, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
		if (status == SQL_NO_DATA) {
			return result;
		}
		if (!SQL_SUCCEEDED(status)) {
			CatalogError(statement, "read column");
		}
		if (indicator == SQL_NULL_DATA) {
			if (!result.empty()) {
				throw IOException("ODBC relational catalog changed NULL state during a value read");
			}
			return string();
		}
		auto length = static_cast<idx_t>(std::find(buffer, buffer + sizeof(buffer), '\0') - buffer);
		if (length == sizeof(buffer) || result.size() + length > limit) {
			throw IOException("ODBC relational metadata RESULT_LIMIT: catalog value too large or unterminated");
		}
		result.append(buffer, length);
		if (status == SQL_SUCCESS) {
			return result;
		}
		SQLCHAR state[6] = {0};
		SQLINTEGER native_error = 0;
		SQLSMALLINT message_length = 0;
		SQLGetDiagRec(SQL_HANDLE_STMT, statement, 1, state, &native_error, nullptr, 0, &message_length);
		if (std::strcmp(reinterpret_cast<const char *>(state), "01004") != 0) {
			return result;
		}
		if (length == 0) {
			throw IOException("ODBC relational catalog made no progress reading a truncated value");
		}
	}
}

string ODBCRelationalReader::Pattern(const string &identifier) {
	SQLCHAR escape[16] = {0};
	SQLSMALLINT length = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(connection, SQL_SEARCH_PATTERN_ESCAPE, escape, sizeof(escape), &length)) ||
	    length < 0 || length >= static_cast<SQLSMALLINT>(sizeof(escape))) {
		throw IOException("ODBC relational catalog cannot determine identifier pattern escaping");
	}
	string result;
	for (auto c : identifier) {
		if (c == '%' || c == '_' || (length == 1 && c == static_cast<char>(escape[0]))) {
			if (length != 1) {
				throw IOException("ODBC relational catalog cannot match identifiers containing pattern characters");
			}
			result += static_cast<char>(escape[0]);
		}
		result += c;
	}
	return result;
}

void ODBCRelationalReader::ReadCatalog(SQLHSTMT statement, SQLRETURN status, const string &operation,
                                       const std::function<void(SQLHSTMT)> &row) {
	Check();
	if (!SQL_SUCCEEDED(status)) {
		CatalogError(statement, operation);
	}
	while (true) {
		Check();
		status = SQLFetch(statement);
		Check();
		if (status == SQL_NO_DATA) {
			return;
		}
		if (!SQL_SUCCEEDED(status)) {
			CatalogError(statement, operation + " fetch");
		}
		row(statement);
	}
}

vector<ODBCRelationalObject> ODBCRelationalReader::Objects(const string &schema) {
	auto statement = Statement();
	auto schema_pattern = Pattern(schema);
	vector<ODBCRelationalObject> result;
	idx_t used = 0;
	auto status = SQLTables(statement.get(), nullptr, 0, ODBCText(schema_pattern), SQL_NTS, ODBCText("%"), SQL_NTS,
	                        ODBCText("'TABLE','VIEW'"), SQL_NTS);
	ReadCatalog(statement.get(), status, "SQLTables", [&](SQLHSTMT stmt) {
		auto catalog = ReadString(stmt, 1, 4096);
		auto actual_schema = ReadString(stmt, 2, 4096);
		auto name = ReadString(stmt, 3, 4096);
		auto type = ReadString(stmt, 4, 256);
		StringUtil::Trim(type);
		if (actual_schema != schema || name.empty() || (type != "TABLE" && type != "VIEW")) {
			throw IOException("ODBC relational catalog returned an unexpected table identity");
		}
		Account(used, sizeof(ODBCRelationalObject) + catalog.size() + actual_schema.size() + name.size());
		result.push_back({std::move(catalog), std::move(actual_schema), std::move(name)});
	});
	std::sort(result.begin(), result.end(), [](const ODBCRelationalObject &a, const ODBCRelationalObject &b) {
		if (a.schema != b.schema) {
			return a.schema < b.schema;
		}
		if (a.name != b.name) {
			return a.name < b.name;
		}
		return a.catalog < b.catalog;
	});
	for (idx_t i = 1; i < result.size(); i++) {
		if (result[i - 1].name == result[i].name) {
			throw IOException("ODBC relational catalog returned ambiguous table identity");
		}
	}
	return result;
}

static string ReferentialAction(const string &value) {
	if (value == "0")
		return "cascade";
	if (value == "1")
		return "restrict";
	if (value == "2")
		return "set_null";
	if (value == "3")
		return "no_action";
	if (value == "4")
		return "set_default";
	throw InvalidInputException("Unknown ODBC referential action %s", value);
}

static idx_t Ordinal(const string &value) {
	idx_t result = 0;
	if (value.empty()) {
		throw IOException("ODBC relational catalog missing ordinal");
	}
	for (auto c : value) {
		if (c < '0' || c > '9' || result > (std::numeric_limits<idx_t>::max() - (c - '0')) / 10) {
			throw IOException("ODBC relational catalog invalid ordinal");
		}
		result = result * 10 + (c - '0');
	}
	if (result == 0) {
		throw IOException("ODBC relational catalog ordinal must be positive");
	}
	return result;
}

vector<ODBCRelationalRow> ODBCRelationalReader::ReadTable(const ODBCRelationalObject &object, const string &category) {
	vector<ODBCRelationalRow> result;
	idx_t used = 0;
	auto append = [&](vector<string> values) {
		idx_t payload = sizeof(ODBCRelationalRow) + values.size() * sizeof(string);
		for (auto &value : values) {
			payload += value.size();
		}
		Account(used, payload);
		result.push_back({std::move(values)});
	};
	auto identity = [&](const vector<string> &v, idx_t catalog, idx_t schema, idx_t name) {
		if (v[catalog] != object.catalog || v[schema] != object.schema || v[name] != object.name) {
			throw IOException("ODBC relational catalog returned an unexpected object identity");
		}
	};
	auto read = [&](SQLHSTMT stmt, SQLUSMALLINT count) {
		vector<string> values;
		idx_t remaining = WINDOW_LIMIT / 3;
		for (SQLUSMALLINT i = 1; i <= count; i++) {
			auto value = ReadString(stmt, i, remaining);
			remaining -= value.size();
			values.push_back(std::move(value));
		}
		return values;
	};
	if (category == "columnProperties") {
		auto stmt = Statement();
		// SQLColumns 是 pattern API；其 ColumnName=nullptr 表示所有列，不能使用 METADATA_ID=true。
		auto schema = Pattern(object.schema), table = Pattern(object.name);
		auto status = SQLColumns(stmt.get(), OptionalODBCText(object.catalog), object.catalog.empty() ? 0 : SQL_NTS,
		                         ODBCText(schema), SQL_NTS, ODBCText(table), SQL_NTS, nullptr, 0);
		ReadCatalog(stmt.get(), status, "SQLColumns", [&](SQLHSTMT statement) {
			auto v = read(statement, 17);
			identity(v, 0, 1, 2);
			auto nullable = v[10];
			StringUtil::Trim(nullable);
			if (nullable != "0" && nullable != "1") {
				throw InvalidInputException("Unknown ODBC SQLColumns NULLABLE value");
			}
			append({v[1], v[2], v[3], v[3], v[16], nullable == "1" ? "true" : "false", v[12], v[5]});
		});
	} else if (category == "constraints") {
		auto primary = Statement();
		auto status =
		    SQLPrimaryKeys(primary.get(), OptionalODBCText(object.catalog), object.catalog.empty() ? 0 : SQL_NTS,
		                   ODBCText(object.schema), SQL_NTS, ODBCText(object.name), SQL_NTS);
		ReadCatalog(primary.get(), status, "SQLPrimaryKeys", [&](SQLHSTMT statement) {
			auto v = read(statement, 6);
			identity(v, 0, 1, 2);
			if (v[5].empty())
				throw IOException("ODBC primary key name unavailable");
			append({v[1], v[2], v[5], v[5], "P", v[4], v[3], "", "", "", "", "", ""});
		});
		primary.reset();
		auto foreign = Statement();
		status = SQLForeignKeys(foreign.get(), nullptr, 0, nullptr, 0, nullptr, 0, OptionalODBCText(object.catalog),
		                        object.catalog.empty() ? 0 : SQL_NTS, ODBCText(object.schema), SQL_NTS,
		                        ODBCText(object.name), SQL_NTS);
		ReadCatalog(foreign.get(), status, "SQLForeignKeys", [&](SQLHSTMT statement) {
			auto v = read(statement, 14);
			identity(v, 4, 5, 6);
			if (v[11].empty())
				throw IOException("ODBC foreign key name unavailable");
			append({v[5], v[6], v[11], v[11], "F", v[8], v[7], v[1], v[2], v[3], ReferentialAction(v[9]),
			        ReferentialAction(v[10]), v[13]});
		});
	} else if (category == "indexes") {
		auto stmt = Statement();
		auto status =
		    SQLStatistics(stmt.get(), OptionalODBCText(object.catalog), object.catalog.empty() ? 0 : SQL_NTS,
		                  ODBCText(object.schema), SQL_NTS, ODBCText(object.name), SQL_NTS, SQL_INDEX_ALL, SQL_QUICK);
		ReadCatalog(stmt.get(), status, "SQLStatistics", [&](SQLHSTMT statement) {
			auto v = read(statement, 13);
			identity(v, 0, 1, 2);
			if (v[6] == "0")
				return;
			if (v[5].empty() || (v[3] != "0" && v[3] != "1")) {
				throw IOException("ODBC index name or uniqueness unavailable");
			}
			auto direction = StringUtil::Upper(v[9]);
			if (direction.empty())
				direction = "UNKNOWN";
			if (direction != "A" && direction != "D" && direction != "UNKNOWN") {
				throw IOException("ODBC index direction unavailable");
			}
			append({v[1], v[2], v[5], v[5], v[6], v[3] == "0" ? "true" : "false", v[7], v[8], direction, v[12]});
		});
	} else {
		throw InvalidInputException("Unsupported odbc_relational_metadata category %s", category);
	}
	const idx_t ordinal_column = category == "columnProperties" ? 4 : category == "constraints" ? 5 : 6;
	for (auto &row : result) {
		Ordinal(row.values[ordinal_column]);
	}
	std::sort(result.begin(), result.end(), [&](const ODBCRelationalRow &a, const ODBCRelationalRow &b) {
		if (category != "columnProperties" && a.values[2] != b.values[2])
			return a.values[2] < b.values[2];
		return Ordinal(a.values[ordinal_column]) < Ordinal(b.values[ordinal_column]);
	});
	Check();
	return result;
}

} // namespace duckdb
