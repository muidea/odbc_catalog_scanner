#include "odbc_scanner.hpp"
#include "odbc_relational_reader.hpp"
#include "odbc_vendor_metadata.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/odbc_catalog.hpp"

namespace duckdb {

struct ODBCRelationalBindData : public FunctionData {
	string database;
	string category;
	string schema;
	bool vendor = false;
	vector<string> tables;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<ODBCRelationalBindData>();
		copy->database = database;
		copy->category = category;
		copy->schema = schema;
		copy->vendor = vendor;
		copy->tables = tables;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other) const override {
		auto &bind = other.Cast<ODBCRelationalBindData>();
		return database == bind.database && category == bind.category && schema == bind.schema &&
		       vendor == bind.vendor && tables == bind.tables;
	}
};

struct ODBCRelationalGlobalState : public GlobalTableFunctionState {
	ODBCPoolConnection lease;
	unique_ptr<ODBCRelationalReader> reader;
	vector<ODBCRelationalObject> objects;
	vector<ODBCRelationalRow> rows;
	idx_t object_offset = 0;
	idx_t row_offset = 0;
	bool batch_query = false;
	bool initialized = false;
	bool finished = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static ODBCCatalog &RelationalCatalog(ClientContext &context, const string &name) {
	auto database = DatabaseManager::Get(context).GetDatabase(context, Identifier(name));
	if (!database || database->GetCatalog().GetCatalogType() != "odbc_catalog") {
		throw BinderException("Attached database %s is not an ODBC catalog", name);
	}
	return database->GetCatalog().Cast<ODBCCatalog>();
}

static unique_ptr<FunctionData> ODBCRelationalBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("Parameters to odbc_relational_metadata cannot be NULL");
		}
	}
	auto result = make_uniq<ODBCRelationalBindData>();
	result->database = input.inputs[0].GetValue<string>();
	result->category = input.inputs[1].GetValue<string>();
	result->schema = input.inputs[2].GetValue<string>();
	result->vendor = true;
	if (input.inputs.size() == 4) {
		for (const auto &value : ListValue::GetChildren(input.inputs[3])) {
			if (value.IsNull() || value.GetValue<string>().empty() ||
			    value.GetValue<string>().find('\0') != string::npos)
				throw BinderException("ODBC metadata table scope is invalid");
			if (std::find(result->tables.begin(), result->tables.end(), value.GetValue<string>()) !=
			    result->tables.end())
				throw BinderException("ODBC metadata table scope contains duplicate names");
			result->tables.push_back(value.GetValue<string>());
		}
		if (result->tables.empty() || result->tables.size() > 128)
			throw BinderException("ODBC metadata table scope must contain 1 to 128 names");
	}
	if (result->schema.empty()) {
		throw BinderException("ODBC relational metadata schema cannot be empty");
	}
	if (result->category == "capabilities" && result->vendor) {
		names = {"catalog_name", "column_name"};
	} else if (result->vendor && result->category == "columnDetails") {
		names = {"table_schema",   "table_name", "column_name",       "identity_kind",
		         "generated_kind", "expression", "default_definition"};
	} else if (result->vendor && result->category == "constraintDetails") {
		names = {"table_schema",
		         "table_name",
		         "constraint_name",
		         "constraint_type",
		         "ordinal_position",
		         "column_name",
		         "definition",
		         "enforced",
		         "validated",
		         "backing_index",
		         "deferrable",
		         "initially_deferred",
		         "backing_index_schema",
		         "backing_index_matches"};
	} else if (result->vendor && result->category == "indexDetails") {
		names = {"table_schema", "table_name", "index_name", "ordinal_position",
		         "column_name",  "expression", "included",   "direction",
		         "nulls_order",  "valid",      "enabled",    "method",
		         "is_unique",    "is_primary", "definition", "index_schema"};
	} else if (result->category == "columnProperties") {
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
		output.SetChildCardinality(0);
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
		if (bind.vendor && bind.category == "capabilities") {
			string sql;
			switch (RelationalCatalog(context, bind.database).GetDialect()) {
			case ODBCDialect::DB2:
				sql = "SELECT TABNAME,COLNAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA='SYSCAT' AND TABNAME IN "
				      "('TABCONST','COLUMNS','INDEXES','INDEXCOLUSE','CHECKS','KEYCOLUSE','CONSTDEP') ORDER BY "
				      "TABNAME,COLNO";
				break;
			case ODBCDialect::DAMENG:
				sql = "SELECT TABLE_NAME,COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND TABLE_NAME IN "
				      "('ALL_TAB_COLUMNS','ALL_CONSTRAINTS','ALL_INDEXES','ALL_IND_COLUMNS','ALL_IND_EXPRESSIONS') "
				      "ORDER BY TABLE_NAME,COLUMN_ID";
				break;
			case ODBCDialect::HANA:
				sql = "SELECT VIEW_NAME,COLUMN_NAME FROM SYS.VIEW_COLUMNS WHERE SCHEMA_NAME='SYS' AND VIEW_NAME IN "
				      "('TABLE_COLUMNS','CONSTRAINTS','REFERENTIAL_CONSTRAINTS','INDEXES','INDEX_COLUMNS') "
				      "ORDER BY VIEW_NAME,POSITION";
				break;
			}
			state.rows = state.reader->ReadQuery(sql, 2);
		} else {
			if (!bind.tables.empty()) {
				// Core discovery uses SQLTables. Vendor dictionaries can hide live
				// objects (for example DM SREF tables), so they cannot certify
				// deletion or invalidate the scope discovered by that same driver.
				// Exact escaped lookups keep this bounded to the requested objects.
				for (const auto &name : bind.tables) {
					auto identities = state.reader->Objects(bind.schema, name);
					if (identities.size() != 1)
						throw IOException(
						    "ODBC metadata table scope changed during collection: schema=%s table=%s matches=%llu",
						    bind.schema, name, static_cast<unsigned long long>(identities.size()));
					state.objects.push_back(std::move(identities[0]));
				}
			} else {
				state.objects = state.reader->Objects(bind.schema);
			}
			if (!bind.tables.empty() && bind.vendor &&
			    (bind.category == "columnDetails" || bind.category == "constraintDetails" ||
			     bind.category == "indexDetails" ||
			     (bind.category == "columnProperties" &&
			      RelationalCatalog(context, bind.database).GetDialect() == ODBCDialect::DAMENG))) {
				auto columns = bind.category == "columnDetails"       ? 7
				               : bind.category == "constraintDetails" ? 14
				               : bind.category == "indexDetails"      ? 16
				                                                      : 8;
				state.reader->StartQuery(
				    ODBCVendorMetadataBatchSQL(RelationalCatalog(context, bind.database).GetDialect(), bind.category,
				                               bind.schema, bind.tables),
				    columns);
				state.batch_query = true;
				state.objects.clear();
			}
		}
		state.initialized = true;
	}
	idx_t count = 0;
	if (state.batch_query) {
		idx_t chunk_bytes = 0;
		ODBCRelationalRow row;
		while (count < STANDARD_VECTOR_SIZE) {
			if (!state.reader->ReadQueryRow(row)) {
				state.reader.reset();
				state.finished = true;
				state.lease = ODBCPoolConnection();
				break;
			}
			for (idx_t column = 0; column < row.values.size(); column++)
				output.SetValue(column, count, Value(row.values[column]));
			for (const auto &value : row.values)
				chunk_bytes += value.size() + sizeof(string);
			count++;
			// Bound output bytes as well as row count; one oversized row is
			// already bounded by the reader's 16 MiB estimated row limit.
			if (chunk_bytes >= 1024 * 1024)
				break;
		}
		output.SetChildCardinality(count);
		return;
	}
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
			auto &object = state.objects[state.object_offset++];
			if (bind.vendor && (bind.category == "columnDetails" || bind.category == "constraintDetails" ||
			                    bind.category == "indexDetails")) {
				auto columns = bind.category == "columnDetails" ? 7 : bind.category == "constraintDetails" ? 14 : 16;
				state.rows = state.reader->ReadQuery(
				    ODBCVendorMetadataSQL(RelationalCatalog(context, bind.database).GetDialect(), bind.category,
				                          object),
				    columns);
			} else if (bind.vendor && bind.category == "columnProperties" &&
			           RelationalCatalog(context, bind.database).GetDialect() == ODBCDialect::DAMENG) {
				state.rows =
				    state.reader->ReadQuery(ODBCVendorMetadataSQL(ODBCDialect::DAMENG, bind.category, object), 8);
			} else {
				state.rows = state.reader->ReadTable(object, bind.category);
			}
			continue;
		}
		state.reader->Check();
		auto &row = state.rows[state.row_offset++].values;
		for (idx_t column = 0; column < row.size(); column++) {
			output.SetValue(column, count, Value(row[column]));
		}
		count++;
	}
	output.SetChildCardinality(count);
}

ODBCRelationalMetadataFunction::ODBCRelationalMetadataFunction(bool vendor, bool scoped)
    : TableFunction("odbc_relational_metadata",
                    scoped ? vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                                  LogicalType::LIST(LogicalType::VARCHAR)}
                           : vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ODBCRelationalScan, ODBCRelationalBind, ODBCRelationalInit) {
}

} // namespace duckdb
