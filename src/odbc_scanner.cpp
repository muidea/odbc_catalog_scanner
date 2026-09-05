#include "duckdb.hpp"
#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "odbc_filter_pushdown.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "storage/odbc_connection_pool.hpp"
#include "storage/odbc_table_set.hpp"
#include "odbc_filter_pushdown.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include <thread>

namespace duckdb {

struct ODBCGlobalState;

struct ODBCLocalState : public LocalTableFunctionState {
	ODBCPoolConnection pool_connection;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<ODBCResult> result;
};

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

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ODBCBindData>();
	// generate the SELECT statement
	string select;
	select += "SELECT ";

	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			// DuckDB requests the row-id pseudo column for projections such as
			// count(*). A bare NULL has driver-specific metadata (Db2 reports it
			// as a large character value), so use a portable typed value instead.
			select += "CAST(0 AS BIGINT)";
		} else {
			auto column_id = input.column_ids[c];
			string col_name;
			if (column_id < bind_data.remote_names.size()) {
				col_name = bind_data.remote_names[column_id];
			} else {
				auto &col = bind_data.table.GetColumn(LogicalIndex(column_id));
				col_name = col.GetName();
			}
			select += ODBCUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += ODBCUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += ODBCUtils::WriteIdentifier(bind_data.table.name);
	auto &filter_names = bind_data.remote_names.empty() ? bind_data.names : bind_data.remote_names;
	string filter_string = ODBCFilterPushdown::TransformFilters(input.column_ids, input.filters, filter_names);
	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}

	// run the query
	auto &transaction = ODBCTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();

	auto query_result = con.Query(select, bind_data.streaming);

	// 检查查询结果是否为空
	if (!query_result) {
		throw InternalException("ODBCInitGlobalState: Query returned null result for table %s.%s",
		                        bind_data.table.schema.name.c_str(), bind_data.table.name.c_str());
	}

	auto result = make_uniq<ODBCGlobalState>(std::move(query_result));

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ODBCInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	return make_uniq<ODBCLocalState>();
}

static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ODBCGlobalState>();

	if (!gstate.result) {
		throw InternalException("ODBCScan: result pointer is null");
	}

	DataChunk &res_chunk = gstate.result->NextChunk();

	// 空结果集，设置输出为空
	if (res_chunk.size() == 0) {
		output.SetCardinality(0);
		return;
	}

	// 没有列的结果集，设置输出为空
	if (res_chunk.ColumnCount() == 0) {
		output.SetCardinality(0);
		return;
	}

	// 检查列数匹配
	if (output.ColumnCount() != res_chunk.ColumnCount()) {
		throw InternalException("ODBCScan: column count mismatch. Output has %lu columns, result has %lu columns",
		                        output.ColumnCount(), res_chunk.ColumnCount());
	}

	string error;
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		Vector &output_vec = output.data[c];
		Vector &res_vec = res_chunk.data[c];

		// 检查向量有效性
		if (output_vec.GetType().id() == LogicalTypeId::INVALID) {
			throw InternalException("ODBCScan: output vector %lu has invalid type", c);
		}
		if (res_vec.GetType().id() == LogicalTypeId::INVALID) {
			throw InternalException("ODBCScan: result vector %lu has invalid type", c);
		}

		// 总是使用 TryCast 进行安全的数据复制，避免引用生命周期问题
		VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);

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
    : TableFunction("odbc_catalog_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan,
                    ODBCBind, ODBCInitGlobalState, ODBCInitLocalState) {
	to_string = ODBCScanToString;
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
	get_bind_info = ODBCGetBindInfo;
	projection_pushdown = true;
}

} // namespace duckdb
