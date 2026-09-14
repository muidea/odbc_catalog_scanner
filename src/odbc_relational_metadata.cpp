#include "odbc_scanner.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/odbc_catalog.hpp"

namespace duckdb {

struct ODBCRelationalRow {
	vector<string> values;
};

struct ODBCRelationalBindData : public FunctionData {
	vector<ODBCRelationalRow> rows;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("ODBCRelationalBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

struct ODBCRelationalGlobalState : public GlobalTableFunctionState {
	explicit ODBCRelationalGlobalState(vector<ODBCRelationalRow> rows_p) : rows(std::move(rows_p)) {
	}
	vector<ODBCRelationalRow> rows;
	idx_t offset = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static string ReadRelationalString(SQLHSTMT statement, SQLUSMALLINT column) {
	vector<char> buffer(512, 0);
	string result;
	SQLLEN indicator = 0;
	while (true) {
		std::fill(buffer.begin(), buffer.end(), 0);
		auto status = SQLGetData(statement, column, SQL_C_CHAR, buffer.data(), buffer.size(), &indicator);
		if (status == SQL_NO_DATA || indicator == SQL_NULL_DATA) {
			break;
		}
		if (!SQL_SUCCEEDED(status)) {
			throw IOException("Failed to read ODBC relational catalog column %d", column);
		}
		result.append(buffer.data());
		if (status == SQL_SUCCESS) {
			break;
		}
	}
	StringUtil::Trim(result);
	return result;
}

static void ThrowRelationalCatalogError(SQLHSTMT statement, const string &operation) {
	SQLCHAR state[6] = {0};
	SQLCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
	SQLINTEGER native_error = 0;
	SQLSMALLINT length = 0;
	SQLGetDiagRec(SQL_HANDLE_STMT, statement, 1, state, &native_error, message, sizeof(message), &length);
	throw IOException("ODBC relational catalog %s failed: SQLState=%s NativeError=%d Message=%s", operation,
	                  reinterpret_cast<const char *>(state), native_error, reinterpret_cast<const char *>(message));
}

template <class START, class ROW>
static void ReadRelationalCatalog(SQLHDBC connection, const string &operation, START &&start, ROW &&row) {
	SQLHSTMT raw_statement = SQL_NULL_HSTMT;
	if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection, &raw_statement))) {
		throw IOException("Failed to allocate ODBC relational catalog statement for %s", operation);
	}
	ODBCStatementPtr statement(raw_statement, ODBCStatementDelete);
	SQLSetStmtAttr(raw_statement, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(60), 0);
	auto status = start(raw_statement);
	if (!SQL_SUCCEEDED(status)) {
		ThrowRelationalCatalogError(raw_statement, operation);
	}
	while (SQL_SUCCEEDED(status = SQLFetch(raw_statement))) {
		row(raw_statement);
	}
	if (status != SQL_NO_DATA) {
		ThrowRelationalCatalogError(raw_statement, operation + " fetch");
	}
}

static SQLCHAR *ODBCText(const string &value) {
	return reinterpret_cast<SQLCHAR *>(const_cast<char *>(value.c_str()));
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

static void ReadColumns(SQLHDBC connection, const ODBCCatalogObject &object, ODBCRelationalBindData &result) {
	ReadRelationalCatalog(
	    connection, "SQLColumns",
	    [&](SQLHSTMT statement) {
		    return SQLColumns(statement, nullptr, 0, ODBCText(object.schema), SQL_NTS, ODBCText(object.name), SQL_NTS,
		                      nullptr, 0);
	    },
	    [&](SQLHSTMT statement) {
		    auto nullable = ReadRelationalString(statement, 11);
		    if (nullable == "0")
			    nullable = "false";
		    else if (nullable == "1")
			    nullable = "true";
		    else
			    throw InvalidInputException("Unknown ODBC SQLColumns NULLABLE value %s", nullable);
		    auto column_name = ReadRelationalString(statement, 4);
		    result.rows.push_back({{ReadRelationalString(statement, 2), ReadRelationalString(statement, 3), column_name,
		                            column_name, ReadRelationalString(statement, 17), nullable,
		                            ReadRelationalString(statement, 13), ReadRelationalString(statement, 6)}});
	    });
}

static void ReadPrimaryKeys(SQLHDBC connection, const ODBCCatalogObject &object, ODBCRelationalBindData &result) {
	ReadRelationalCatalog(
	    connection, "SQLPrimaryKeys",
	    [&](SQLHSTMT statement) {
		    return SQLPrimaryKeys(statement, nullptr, 0, ODBCText(object.schema), SQL_NTS, ODBCText(object.name),
		                          SQL_NTS);
	    },
	    [&](SQLHSTMT statement) {
		    auto name = ReadRelationalString(statement, 6);
		    if (name.empty())
			    throw InvalidInputException("ODBC primary key name is unavailable for %s.%s", object.schema,
			                                object.name);
		    result.rows.push_back(
		        {{ReadRelationalString(statement, 2), ReadRelationalString(statement, 3), name, name, "P",
		          ReadRelationalString(statement, 5), ReadRelationalString(statement, 4), "", "", "", "", "", ""}});
	    });
}

static void ReadForeignKeys(SQLHDBC connection, const ODBCCatalogObject &object, ODBCRelationalBindData &result) {
	ReadRelationalCatalog(
	    connection, "SQLForeignKeys",
	    [&](SQLHSTMT statement) {
		    return SQLForeignKeys(statement, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0, ODBCText(object.schema),
		                          SQL_NTS, ODBCText(object.name), SQL_NTS);
	    },
	    [&](SQLHSTMT statement) {
		    auto name = ReadRelationalString(statement, 12);
		    if (name.empty())
			    throw InvalidInputException("ODBC foreign key name is unavailable for %s.%s", object.schema,
			                                object.name);
		    result.rows.push_back(
		        {{ReadRelationalString(statement, 6), ReadRelationalString(statement, 7), name, name, "F",
		          ReadRelationalString(statement, 9), ReadRelationalString(statement, 8),
		          ReadRelationalString(statement, 2), ReadRelationalString(statement, 3),
		          ReadRelationalString(statement, 4), ReferentialAction(ReadRelationalString(statement, 10)),
		          ReferentialAction(ReadRelationalString(statement, 11)), ReadRelationalString(statement, 14)}});
	    });
}

static void ReadIndexes(SQLHDBC connection, const ODBCCatalogObject &object, ODBCRelationalBindData &result) {
	ReadRelationalCatalog(
	    connection, "SQLStatistics",
	    [&](SQLHSTMT statement) {
		    return SQLStatistics(statement, nullptr, 0, ODBCText(object.schema), SQL_NTS, ODBCText(object.name),
		                         SQL_NTS, SQL_INDEX_ALL, SQL_ENSURE);
	    },
	    [&](SQLHSTMT statement) {
		    auto type = ReadRelationalString(statement, 7);
		    auto name = ReadRelationalString(statement, 6);
		    if (type == "0")
			    return;
		    if (name.empty())
			    throw InvalidInputException("ODBC index name is unavailable for %s.%s", object.schema, object.name);
		    auto non_unique = ReadRelationalString(statement, 4);
		    if (non_unique != "0" && non_unique != "1") {
			    throw InvalidInputException("Unknown ODBC SQLStatistics NON_UNIQUE value %s", non_unique);
		    }
		    auto direction = StringUtil::Upper(ReadRelationalString(statement, 10));
		    if (direction.empty())
			    direction = "UNKNOWN";
		    else if (direction != "A" && direction != "D") {
			    throw InvalidInputException("Unknown ODBC SQLStatistics ASC_OR_DESC value %s", direction);
		    }
		    result.rows.push_back(
		        {{ReadRelationalString(statement, 2), ReadRelationalString(statement, 3), name, name, type,
		          non_unique == "0" ? "true" : "false", ReadRelationalString(statement, 8),
		          ReadRelationalString(statement, 9), direction, ReadRelationalString(statement, 13)}});
	    });
}

static unique_ptr<FunctionData> ODBCRelationalBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull())
			throw BinderException("Parameters to odbc_relational_metadata cannot be NULL");
	}
	auto database_name = input.inputs[0].GetValue<string>();
	auto category = input.inputs[1].GetValue<string>();
	auto schema = input.inputs[2].GetValue<string>();
	StringUtil::Trim(schema);
	if (schema.empty())
		throw BinderException("ODBC relational metadata schema cannot be empty");

	if (category == "columnProperties") {
		names = {"table_schema",     "table_name",  "column_name",        "source_id",
		         "ordinal_position", "is_nullable", "default_definition", "source_type"};
	} else if (category == "constraints") {
		names = {"table_schema",    "table_name",  "source_id",         "constraint_name",  "constraint_type",
		         "column_position", "column_name", "referenced_schema", "referenced_table", "referenced_column",
		         "update_rule",     "delete_rule", "deferrability"};
	} else if (category == "indexes") {
		names = {"table_schema", "table_name",  "source_id",   "index_name", "source_type",
		         "is_unique",    "key_ordinal", "column_name", "direction",  "filter_condition"};
	} else {
		throw InvalidInputException("Unsupported odbc_relational_metadata category %s", category);
	}
	return_types.assign(names.size(), LogicalType::VARCHAR);

	auto database = DatabaseManager::Get(context).GetDatabase(context, database_name);
	if (!database || database->GetCatalog().GetCatalogType() != "odbc_catalog") {
		throw BinderException("Attached database %s is not an ODBC catalog", database_name);
	}
	auto &catalog = database->GetCatalog().Cast<ODBCCatalog>();
	auto lease = catalog.GetConnectionPool().GetConnection();
	auto connection = lease.GetConnection()->GetConn();
	auto objects = ODBCUtils::GetCatalogObjects(connection, schema);
	auto result = make_uniq<ODBCRelationalBindData>();
	for (auto &object : objects) {
		if (category == "columnProperties")
			ReadColumns(connection, object, *result);
		else if (category == "constraints") {
			ReadPrimaryKeys(connection, object, *result);
			ReadForeignKeys(connection, object, *result);
		} else
			ReadIndexes(connection, object, *result);
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ODBCRelationalInit(ClientContext &, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->CastNoConst<ODBCRelationalBindData>();
	return make_uniq<ODBCRelationalGlobalState>(std::move(bind.rows));
}

static void ODBCRelationalScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<ODBCRelationalGlobalState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++].values;
		for (idx_t column = 0; column < row.size(); column++)
			output.SetValue(column, count, Value(row[column]));
		count++;
	}
	output.SetCardinality(count);
}

ODBCRelationalMetadataFunction::ODBCRelationalMetadataFunction()
    : TableFunction("odbc_relational_metadata", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ODBCRelationalScan, ODBCRelationalBind, ODBCRelationalInit) {
}

} // namespace duckdb
