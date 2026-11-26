#include "duckdb.hpp"
#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "odbc_filter_pushdown.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "storage/odbc_connection_pool.hpp"
#include "storage/odbc_table_set.hpp"
#include "odbc_filter_pushdown.hpp"
#include "odbc_temporal_utils.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <thread>

namespace duckdb {

struct ODBCGlobalState;

struct ODBCLocalState : public LocalTableFunctionState {
	ODBCPoolConnection pool_connection;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<ODBCResult> result;
};

struct ODBCGlobalState : public GlobalTableFunctionState {
	explicit ODBCGlobalState(unique_ptr<ODBCResult> result_p, unique_ptr<Expression> local_filter_expr_p = nullptr,
	                         unique_ptr<ExpressionExecutor> local_filter_executor_p = nullptr)
	    : result(std::move(result_p)), local_filter_expr(std::move(local_filter_expr_p)),
	      local_filter_executor(std::move(local_filter_executor_p)) {
	}

	unique_ptr<ODBCResult> result;
	unique_ptr<Expression> local_filter_expr;
	unique_ptr<ExpressionExecutor> local_filter_executor;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static bool TryCastODBCTimestampTZVector(Vector &res_vec, Vector &output_vec, idx_t count, string &error) {
	if (res_vec.GetType().id() != LogicalTypeId::VARCHAR || output_vec.GetType().id() != LogicalTypeId::TIMESTAMP_TZ) {
		return false;
	}
	auto src = FlatVector::GetData<string_t>(res_vec);
	auto dst = FlatVector::GetData<timestamp_tz_t>(output_vec);
	for (idx_t r = 0; r < count; r++) {
		if (FlatVector::IsNull(res_vec, r)) {
			FlatVector::SetNull(output_vec, r, true);
			continue;
		}
		string raw(src[r].GetData(), src[r].GetSize());
		timestamp_tz_t parsed_tz;
		if (!ParseODBCTimestampTZText(raw, false, parsed_tz)) {
			error = StringUtil::Format("invalid timestamp with time zone format: \"%s\"", raw);
			return false;
		}
		dst[r] = parsed_tz;
	}
	error.clear();
	return true;
}

static unique_ptr<FunctionData> ODBCBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("ODBCBind");
}

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ODBCBindData>();
	// generate the SELECT statement
	string select;
	select += "SELECT ";
	vector<LogicalType> expected_types;
	expected_types.reserve(input.column_ids.size());

	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			// DuckDB requests the row-id pseudo column for projections such as
			// count(*). A bare NULL has driver-specific metadata (Db2 reports it
			// as a large character value), so use a portable typed value instead.
			select += "CAST(0 AS BIGINT)";
			expected_types.push_back(LogicalType::BIGINT);
		} else {
			auto column_id = input.column_ids[c];
			string col_name;
			if (column_id < bind_data.remote_names.size()) {
				col_name = bind_data.remote_names[column_id];
			} else {
				auto &col = bind_data.table.GetColumn(LogicalIndex(column_id));
				col_name = col.GetName();
			}
			expected_types.push_back(column_id < bind_data.types.size()
			                             ? bind_data.types[column_id]
			                             : bind_data.table.GetColumn(LogicalIndex(column_id)).GetType());
			select += ODBCUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += ODBCUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += ODBCUtils::WriteIdentifier(bind_data.table.name);
	auto &filter_names = bind_data.remote_names.empty() ? bind_data.names : bind_data.remote_names;
	auto dialect = bind_data.table.catalog.Cast<ODBCCatalog>().GetDialect();
	auto pushdown = ODBCFilterPushdown::TransformFiltersWithFallback(input.column_ids, input.filters, filter_names,
	                                                                   bind_data.remote_type_names, dialect);
	if (!pushdown.where_clause.empty()) {
		select += " WHERE " + pushdown.where_clause;
	}

	// run the query
	auto &transaction = ODBCTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();

	auto query_result = con.Query(select, bind_data.streaming, std::move(expected_types));

	// 检查查询结果是否为空
	if (!query_result) {
		throw InternalException("ODBCInitGlobalState: Query returned null result for table %s.%s",
		                        bind_data.table.schema.name.c_str(), bind_data.table.name.c_str());
	}

	unique_ptr<Expression> local_filter_expr;
	unique_ptr<ExpressionExecutor> local_filter_executor;
	if (!pushdown.unpushed_filter_columns.empty() && input.filters) {
		vector<unique_ptr<Expression>> local_exprs;
		for (auto filter_col_idx : pushdown.unpushed_filter_columns) {
			if (filter_col_idx >= input.column_ids.size()) {
				continue;
			}
			auto it = input.filters->filters.find(filter_col_idx);
			if (it == input.filters->filters.end()) {
				continue;
			}
			auto col_id = input.column_ids[filter_col_idx];
			if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			auto col_type = col_id < bind_data.types.size() ? bind_data.types[col_id]
			                                                : bind_data.table.GetColumn(LogicalIndex(col_id)).GetType();
			auto col_ref = make_uniq<BoundReferenceExpression>(col_type, filter_col_idx);
			auto expr = it->second->ToExpression(*col_ref);
			if (expr) {
				local_exprs.push_back(std::move(expr));
			}
		}
		if (local_exprs.size() == 1) {
			local_filter_expr = std::move(local_exprs[0]);
		} else if (local_exprs.size() > 1) {
			auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &expr : local_exprs) {
				conjunction->children.push_back(std::move(expr));
			}
			local_filter_expr = std::move(conjunction);
		}
		if (local_filter_expr) {
			local_filter_executor = make_uniq<ExpressionExecutor>(context, *local_filter_expr);
		}
	}

	return make_uniq<ODBCGlobalState>(std::move(query_result), std::move(local_filter_expr),
	                                  std::move(local_filter_executor));
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

	while (true) {
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
			error.clear();

			// 检查向量有效性
			if (output_vec.GetType().id() == LogicalTypeId::INVALID) {
				throw InternalException("ODBCScan: output vector %lu has invalid type", c);
			}
			if (res_vec.GetType().id() == LogicalTypeId::INVALID) {
				throw InternalException("ODBCScan: result vector %lu has invalid type", c);
			}

			if (res_vec.GetType() == output_vec.GetType()) {
				// A same-type TryCast is DuckDB's NopCast and only references the
				// source vector. Materialized ODBC chunks are owned by the function
				// state, so the output must receive an independent copy.
				VectorOperations::Copy(res_vec, output_vec, res_chunk.size(), 0, 0);
			} else if (res_vec.GetType().id() == LogicalTypeId::VARCHAR &&
			           output_vec.GetType().id() == LogicalTypeId::TIMESTAMP_TZ) {
				TryCastODBCTimestampTZVector(res_vec, output_vec, res_chunk.size(), error);
			} else if ((res_vec.GetType().id() == LogicalTypeId::TIMESTAMP &&
			            output_vec.GetType().id() == LogicalTypeId::TIMESTAMP_TZ) ||
			           (res_vec.GetType().id() == LogicalTypeId::TIMESTAMP_TZ &&
			            output_vec.GetType().id() == LogicalTypeId::TIMESTAMP)) {
				throw ConversionException(
				    "Refusing implicit session-timezone cast between TIMESTAMP and TIMESTAMP WITH TIME ZONE in ODBC "
				    "catalog scan");
			} else {
				VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
			}

			// throw an error in case of TryCast fails
			if (!error.empty()) {
				throw BinderException(error);
			}
		}
		output.SetCardinality(res_chunk.size());

		if (!gstate.local_filter_executor) {
			return;
		}
		SelectionVector sel(output.size());
		idx_t match_count = gstate.local_filter_executor->SelectExpression(output, sel);
		if (match_count == 0) {
			output.Reset();
			output.SetCardinality(0);
			continue;
		}
		if (match_count < output.size()) {
			output.Slice(sel, match_count);
			output.SetCardinality(match_count);
		}
		return;
	}
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
