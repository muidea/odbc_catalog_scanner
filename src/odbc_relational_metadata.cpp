#include "odbc_scanner.hpp"
#include "odbc_relational_reader.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/odbc_catalog.hpp"

namespace duckdb {

struct ODBCRelationalBindData : public FunctionData {
	string database;
	string category;
	string schema;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<ODBCRelationalBindData>();
		copy->database = database;
		copy->category = category;
		copy->schema = schema;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other) const override {
		auto &bind = other.Cast<ODBCRelationalBindData>();
		return database == bind.database && category == bind.category && schema == bind.schema;
	}
};

struct ODBCRelationalGlobalState : public GlobalTableFunctionState {
	ODBCPoolConnection lease;
	unique_ptr<ODBCRelationalReader> reader;
	vector<ODBCRelationalObject> objects;
	vector<ODBCRelationalRow> rows;
	idx_t object_offset = 0;
	idx_t row_offset = 0;
	bool initialized = false;
	bool finished = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static ODBCCatalog &RelationalCatalog(ClientContext &context, const string &name) {
	auto database = DatabaseManager::Get(context).GetDatabase(context, name);
	if (!database || database->GetCatalog().GetCatalogType() != "odbc_catalog") {
		throw BinderException("Attached database %s is not an ODBC catalog", name);
	}
	return database->GetCatalog().Cast<ODBCCatalog>();
}

static unique_ptr<FunctionData> ODBCRelationalBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("Parameters to odbc_relational_metadata cannot be NULL");
		}
	}
	auto result = make_uniq<ODBCRelationalBindData>();
	result->database = input.inputs[0].GetValue<string>();
	result->category = input.inputs[1].GetValue<string>();
	result->schema = input.inputs[2].GetValue<string>();
	if (result->schema.empty()) {
		throw BinderException("ODBC relational metadata schema cannot be empty");
	}
	if (result->category == "columnProperties") {
		names = {"table_schema",     "table_name",  "column_name",        "source_id",
		         "ordinal_position", "is_nullable", "default_definition", "source_type"};
	} else if (result->category == "constraints") {
		names = {"table_schema",    "table_name",  "source_id",         "constraint_name",  "constraint_type",
		         "column_position", "column_name", "referenced_schema", "referenced_table", "referenced_column",
		         "update_rule",     "delete_rule", "deferrability"};
	} else if (result->category == "indexes") {
		names = {"table_schema", "table_name",  "source_id",   "index_name", "source_type",
		         "is_unique",    "key_ordinal", "column_name", "direction",  "filter_condition"};
	} else {
		throw InvalidInputException("Unsupported odbc_relational_metadata category %s", result->category);
	}
	return_types.assign(names.size(), LogicalType::VARCHAR);
	RelationalCatalog(context, result->database);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ODBCRelationalInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<ODBCRelationalGlobalState>();
}

static void ODBCRelationalScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<ODBCRelationalGlobalState>();
	auto &bind = input.bind_data->Cast<ODBCRelationalBindData>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}
	if (!state.initialized) {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
		state.lease = RelationalCatalog(context, bind.database).GetConnectionPool().GetConnection();
		state.reader = make_uniq<ODBCRelationalReader>(
		    state.lease.GetConnection()->GetConn(),
		    [&context]() {
			    if (context.IsInterrupted()) {
				    throw InterruptException();
			    }
		    },
		    deadline);
		state.objects = state.reader->Objects(bind.schema);
		state.initialized = true;
	}
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		if (state.row_offset == state.rows.size()) {
			vector<ODBCRelationalRow>().swap(state.rows);
			state.row_offset = 0;
			if (state.object_offset == state.objects.size()) {
				state.reader.reset();
				state.finished = true;
				vector<ODBCRelationalObject>().swap(state.objects);
				state.lease = ODBCPoolConnection();
				break;
			}
			state.reader->Check();
			state.rows = state.reader->ReadTable(state.objects[state.object_offset++], bind.category);
			continue;
		}
		state.reader->Check();
		auto &row = state.rows[state.row_offset++].values;
		for (idx_t column = 0; column < row.size(); column++) {
			output.SetValue(column, count, Value(row[column]));
		}
		count++;
	}
	output.SetCardinality(count);
}

ODBCRelationalMetadataFunction::ODBCRelationalMetadataFunction()
    : TableFunction("odbc_relational_metadata", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ODBCRelationalScan, ODBCRelationalBind, ODBCRelationalInit) {
}

} // namespace duckdb
