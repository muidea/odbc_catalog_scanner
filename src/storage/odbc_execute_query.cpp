#include "storage/odbc_execute_query.hpp"
#include "storage/odbc_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "odbc_connection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb {

ODBCExecuteQuery::ODBCExecuteQuery(PhysicalPlan &physical_plan, LogicalOperator &op, string op_name_p,
                                     TableCatalogEntry &table, string query_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), op_name(std::move(op_name_p)),
      table(table), query(std::move(query_p)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class ODBCExecuteQueryGlobalState : public GlobalSinkState {
public:
	explicit ODBCExecuteQueryGlobalState() : affected_rows(0) {
	}

	idx_t affected_rows;
};

unique_ptr<GlobalSinkState> ODBCExecuteQuery::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ODBCExecuteQueryGlobalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType ODBCExecuteQuery::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType ODBCExecuteQuery::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ODBCExecuteQueryGlobalState>();
	auto &transaction = ODBCTransaction::Get(context, table.catalog);
	auto &connection = transaction.GetConnection();
	auto result = connection.Query(query, ODBCResultStreaming::FORCE_MATERIALIZATION);
	gstate.affected_rows = result->AffectedRows();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetDataInternal
//===--------------------------------------------------------------------===//
SourceResultType ODBCExecuteQuery::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<ODBCExecuteQueryGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.affected_rows));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string ODBCExecuteQuery::GetName() const {
	return op_name;
}

InsertionOrderPreservingMap<string> ODBCExecuteQuery::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
string ExtractFilters(PhysicalOperator &child, const string &statement) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	if (child.type == PhysicalOperatorType::FILTER) {
		auto &filter = child.Cast<PhysicalFilter>();
		auto result = ExtractFilters(child.children[0], statement);
		auto filter_str = filter.expression->ToString();
		if (result.empty()) {
			return filter_str;
		} else {
			return result + " AND " + filter_str;
		}
	} else if (child.type == PhysicalOperatorType::PROJECTION) {
		auto &proj = child.Cast<PhysicalProjection>();
		for (auto &expr : proj.select_list) {
			switch (expr->type) {
			case ExpressionType::BOUND_REF:
			case ExpressionType::BOUND_COLUMN_REF:
			case ExpressionType::VALUE_CONSTANT:
				break;
			default:
				throw NotImplementedException("Unsupported expression type in projection - only simple "
				                              "deletes/updates "
				                              "are supported in the ODBC connector");
			}
		}
		return ExtractFilters(child.children[0], statement);
	} else if (child.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = child.Cast<PhysicalTableScan>();
		if (!table_scan.table_filters) {
			return string();
		}
		string result;
		for (auto &entry : table_scan.table_filters->filters) {
			auto column_index = entry.first;
			auto &filter = entry.second;
			string column_name;
			if (column_index < table_scan.names.size()) {
				const auto col_id = table_scan.column_ids[column_index].GetPrimaryIndex();
				if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
					column_name = "rowid";
				} else {
					column_name = table_scan.names[col_id];
				}
			}
			BoundReferenceExpression bound_ref(std::move(column_name), LogicalTypeId::INVALID, 0);
			auto filter_expr = filter->ToExpression(bound_ref);
			auto filter_str = filter_expr->ToString();
			if (result.empty()) {
				result = std::move(filter_str);
			} else {
				result += " AND " + filter_str;
			}
		}
		return result;
	} else {
		throw NotImplementedException("Unsupported operator type %s in %s statement - only simple deletes "
		                              "(e.g. %s "
		                              "FROM tbl WHERE x=y) are supported in the ODBC connector",
		                              PhysicalOperatorToString(child.type), statement, statement);
	}
}

string ConstructDeleteStatement(LogicalDelete &op, PhysicalOperator &child) {
	string result = "DELETE FROM ";
	result += ODBCUtils::WriteIdentifier(op.table.schema.name);
	result += ".";
	result += ODBCUtils::WriteIdentifier(op.table.name);
	auto filters = ExtractFilters(child, "DELETE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

PhysicalOperator &ODBCCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a ODBC table");
	}
	ODBCCatalog::MaterializeODBCScans(plan);

	auto &execute = planner.Make<ODBCExecuteQuery>(op, "DELETE", op.table, ConstructDeleteStatement(op, plan));
	execute.children.push_back(plan);
	return execute;
}

string ConstructUpdateStatement(LogicalUpdate &op, PhysicalOperator &child) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	string result = "UPDATE ";
	result += ODBCUtils::WriteIdentifier(op.table.schema.name);
	result += ".";
	result += ODBCUtils::WriteIdentifier(op.table.name);
	result += " SET ";
	if (child.type != PhysicalOperatorType::PROJECTION) {
		throw NotImplementedException("ODBC Update not supported - Expected the "
		                              "child of an update to be a projection");
	}
	auto &proj = child.Cast<PhysicalProjection>();
	for (idx_t c = 0; c < op.columns.size(); c++) {
		if (c > 0) {
			result += ", ";
		}
		auto &col = op.table.GetColumn(op.table.GetColumns().PhysicalToLogical(op.columns[c]));
		result += ODBCUtils::WriteIdentifier(col.GetName());
		result += " = ";
		if (op.expressions[c]->type == ExpressionType::VALUE_DEFAULT) {
			result += "DEFAULT";
			continue;
		}
		if (op.expressions[c]->type != ExpressionType::BOUND_REF) {
			throw NotImplementedException("ODBC Update not supported - Expected a bound reference expression");
		}
		auto &ref = op.expressions[c]->Cast<BoundReferenceExpression>();
		auto &expr = proj.select_list[ref.index];
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &bound_const_expr = expr->Cast<BoundConstantExpression>();
			result += ODBCUtils::TransformConstant(bound_const_expr.value);
		} else {
			result += expr->ToString();
		}
	}
	result += " ";
	auto filters = ExtractFilters(child.children[0], "UPDATE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

PhysicalOperator &ODBCCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a ODBC table");
	}
	ODBCCatalog::MaterializeODBCScans(plan);

	auto &execute = planner.Make<ODBCExecuteQuery>(op, "UPDATE", op.table, ConstructUpdateStatement(op, plan));
	execute.children.push_back(plan);
	return execute;
}

} // namespace duckdb
