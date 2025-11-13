#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "odbc_scanner.hpp"
#include "odbc_result.hpp"
#include "storage/odbc_transaction.hpp"
#include "storage/odbc_table_set.hpp"
#include "odbc_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct ODBCGlobalState;

struct ODBCLocalState : public LocalTableFunctionState {};

struct ODBCGlobalState : public GlobalTableFunctionState {
	explicit ODBCGlobalState(unique_ptr<ODBCResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<ODBCResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> ODBCBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("ODBCBind");
}

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ODBCBindData>();
	// generate the SELECT statement
	string select;
	select += "SELECT ";
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
			auto col_name = col.GetName();
			select += ODBCUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += ODBCUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += ODBCUtils::WriteIdentifier(bind_data.table.name);
	string filter_string = ODBCFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}
	if (!bind_data.limit.empty()) {
		select += bind_data.limit;
	}
	// run the query
	auto &transaction = ODBCTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();
	auto query_result = con.Query(select, bind_data.streaming);
	auto result = make_uniq<ODBCGlobalState>(std::move(query_result));

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ODBCInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<ODBCLocalState>();
}

static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ODBCGlobalState>();
	DataChunk &res_chunk = gstate.result->NextChunk();
	D_ASSERT(output.ColumnCount() == res_chunk.ColumnCount());
	string error;
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		Vector &output_vec = output.data[c];
		Vector &res_vec = res_chunk.data[c];
		switch (output_vec.GetType().id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BLOB:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			if (output_vec.GetType().id() == res_vec.GetType().id() ||
			    (output_vec.GetType().id() == LogicalTypeId::BLOB &&
			     res_vec.GetType().id() == LogicalTypeId::VARCHAR)) {
				output_vec.Reinterpret(res_vec);
			} else {
				VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
			}
			break;
		}
		default: {
			VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
			break;
		}
		}
		// throw an error in case of TryCast fails
		if (!error.empty()) {
			throw BinderException(error);
		}
	}
	output.SetCardinality(res_chunk.size());
}

static InsertionOrderPreservingMap<string> ODBCScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<ODBCBindData>();
	result["Table"] = bind_data.table.name;
	return result;
}

static void ODBCScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("ODBCScanSerialize");
}

static unique_ptr<FunctionData> ODBCScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("ODBCScanDeserialize");
}

static BindInfo ODBCGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ODBCBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

ODBCScanFunction::ODBCScanFunction()
    : TableFunction("odbc_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan,
                    ODBCBind, ODBCInitGlobalState, ODBCInitLocalState) {
	to_string = ODBCScanToString;
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
	get_bind_info = ODBCGetBindInfo;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// ODBC Query
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> ODBCQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to odbc_query cannot be NULL");
	}

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in odbc_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "odbc") {
		throw BinderException("Attached database \"%s\" does not refer to a ODBC database", db_name);
	}
	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();

	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to odbc_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	auto result =
	    transaction.GetConnection().Query(sql, std::move(params), ODBCResultStreaming::FORCE_MATERIALIZATION);
	for (auto &field : result->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	if (return_types.empty()) {
		throw InvalidInputException("Failed to fetch return types for query '%s'", sql);
	}
	return make_uniq<ODBCQueryBindData>(catalog, std::move(result), std::move(sql));
}

static unique_ptr<GlobalTableFunctionState> ODBCQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<ODBCQueryBindData>();
	unique_ptr<ODBCResult> odbc_result;
	if (bind_data.result) {
		odbc_result = std::move(bind_data.result);
	} else {
		auto &transaction = ODBCTransaction::Get(context, bind_data.catalog);
		odbc_result = transaction.GetConnection().Query(bind_data.query, ODBCResultStreaming::FORCE_MATERIALIZATION);
	}

	return make_uniq<ODBCGlobalState>(std::move(odbc_result));
}

ODBCQueryFunction::ODBCQueryFunction()
    : TableFunction("odbc_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan, ODBCQueryBind,
                    ODBCQueryInitGlobalState, ODBCInitLocalState) {
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
}

} // namespace duckdb
