#include "storage/odbc_insert.hpp"

#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "odbc_connection.hpp"
#include "odbc_scanner.hpp"
#include "odbc_types.hpp"

#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "storage/odbc_table_entry.hpp"

namespace duckdb {

ODBCInsert::ODBCInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                         physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

ODBCInsert::ODBCInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                         unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class ODBCInsertGlobalState : public GlobalSinkState {
public:
	explicit ODBCInsertGlobalState(ClientContext &context, ODBCTableEntry &table,
	                                const vector<LogicalType> &varchar_types)
	    : table(table), insert_count(0) {
		varchar_chunk.Initialize(context, varchar_types);
	}

	ODBCTableEntry &table;
	DataChunk varchar_chunk;
	idx_t insert_count;
	string base_insert_query;
	string insert_values;
};

vector<string> GetInsertColumns(const ODBCInsert &insert, ODBCTableEntry &entry) {
	vector<string> column_names;
	auto &columns = entry.GetColumns();
	idx_t column_count;
	if (!insert.column_index_map.empty()) {
		column_count = 0;
		vector<PhysicalIndex> column_indexes;
		column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
		for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
			auto column_index = PhysicalIndex(c);
			auto mapped_index = insert.column_index_map[column_index];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// column not specified
				continue;
			}
			column_indexes[mapped_index] = column_index;
			column_count++;
		}
		for (idx_t c = 0; c < column_count; c++) {
			auto &col = columns.GetColumn(column_indexes[c]);
			column_names.push_back(col.GetName());
		}
	}
	return column_names;
}

string GetBaseInsertQuery(const ODBCTableEntry &table, const vector<string> &column_names) {
	string query;
	query += "INSERT INTO ";
	query += ODBCUtils::WriteIdentifier(table.schema.name);
	query += ".";
	query += ODBCUtils::WriteIdentifier(table.name);
	query += " ";
	if (!column_names.empty()) {
		query += "(";
		for (idx_t c = 0; c < column_names.size(); c++) {
			if (c > 0) {
				query += ", ";
			}
			query += ODBCUtils::WriteIdentifier(column_names[c]);
		}
		query += ")";
	}
	query += " VALUES ";
	return query;
}

unique_ptr<GlobalSinkState> ODBCInsert::GetGlobalSinkState(ClientContext &context) const {
	ODBCTableEntry *insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<ODBCTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<ODBCTableEntry>();
	}
	auto insert_columns = GetInsertColumns(*this, *insert_table);
	vector<LogicalType> insert_types;
	idx_t insert_column_count =
	    insert_columns.empty() ? insert_table->GetColumns().LogicalColumnCount() : insert_columns.size();
	for (idx_t c = 0; c < insert_column_count; c++) {
		insert_types.push_back(LogicalType::VARCHAR);
	}
	auto result = make_uniq<ODBCInsertGlobalState>(context, *insert_table, insert_types);
	result->base_insert_query = GetBaseInsertQuery(*insert_table, insert_columns);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static void ODBCCastBlob(const Vector &input, Vector &result, idx_t count) {
	static constexpr const char *HEX_TABLE = "0123456789ABCDEF";
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t r = 0; r < count; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		auto blob_data = const_data_ptr_cast(input_data[r].GetData());
		auto blob_size = input_data[r].GetSize();
		string result_blob = "0x";
		for (idx_t b = 0; b < blob_size; b++) {
			auto blob_entry = blob_data[b];
			auto byte_a = blob_entry >> 4;
			auto byte_b = blob_entry & 0x0F;
			result_blob += string(1, HEX_TABLE[byte_a]);
			result_blob += string(1, HEX_TABLE[byte_b]);
		}
		result_data[r] = StringVector::AddString(result, result_blob);
	}
}

SinkResultType ODBCInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	static constexpr const idx_t INSERT_FLUSH_SIZE = 8000;

	auto &gstate = input.global_state.Cast<ODBCInsertGlobalState>();
	auto &transaction = ODBCTransaction::Get(context.client, gstate.table.catalog);
	auto &con = transaction.GetConnection();
	// cast to varchar
	D_ASSERT(chunk.ColumnCount() == gstate.varchar_chunk.ColumnCount());
	chunk.Flatten();
	gstate.varchar_chunk.Reset();
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		switch (chunk.data[c].GetType().id()) {
		case LogicalTypeId::BLOB:
			ODBCCastBlob(chunk.data[c], gstate.varchar_chunk.data[c], chunk.size());
			break;
		case LogicalTypeId::TIMESTAMP_TZ: {
			Vector timestamp_vector(LogicalType::TIMESTAMP);
			timestamp_vector.Reinterpret(chunk.data[c]);
			VectorOperations::Cast(context.client, timestamp_vector, gstate.varchar_chunk.data[c], chunk.size());
			break;
		}
		default:
			VectorOperations::Cast(context.client, chunk.data[c], gstate.varchar_chunk.data[c], chunk.size());
			break;
		}
	}
	gstate.varchar_chunk.SetCardinality(chunk.size());
	// for each column type check if we need to add quotes or not
	vector<bool> add_quotes;
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		bool add_quotes_for_type;
		switch (chunk.data[c].GetType().id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BLOB:
			add_quotes_for_type = false;
			break;
		default:
			add_quotes_for_type = true;
			break;
		}
		add_quotes.push_back(add_quotes_for_type);
	}

	// generate INSERT INTO statements
	for (idx_t r = 0; r < chunk.size(); r++) {
		if (!gstate.insert_values.empty()) {
			gstate.insert_values += ", ";
		}
		gstate.insert_values += "(";
		for (idx_t c = 0; c < gstate.varchar_chunk.ColumnCount(); c++) {
			if (c > 0) {
				gstate.insert_values += ", ";
			}
			if (FlatVector::IsNull(gstate.varchar_chunk.data[c], r)) {
				gstate.insert_values += "NULL";
			} else {
				auto data = FlatVector::GetData<string_t>(gstate.varchar_chunk.data[c]);
				if (add_quotes[c]) {
					gstate.insert_values += ODBCUtils::WriteLiteral(data[r].GetString());
				} else {
					gstate.insert_values += data[r].GetString();
				}
			}
		}
		gstate.insert_values += ")";
		if (gstate.insert_values.size() >= INSERT_FLUSH_SIZE) {
			// perform the actual insert
			con.Execute(gstate.base_insert_query + gstate.insert_values);
			// reset the to-be-inserted values
			gstate.insert_values = string();
		}
	}
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType ODBCInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ODBCInsertGlobalState>();
	if (!gstate.insert_values.empty()) {
		// perform the final insert
		auto &transaction = ODBCTransaction::Get(context, gstate.table.catalog);
		auto &con = transaction.GetConnection();
		con.Execute(gstate.base_insert_query + gstate.insert_values);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetDataInternal
//===--------------------------------------------------------------------===//
SourceResultType ODBCInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<ODBCInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string ODBCInsert::GetName() const {
	return table ? "MYSQL_INSERT" : "MYSQL_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> ODBCInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
PhysicalOperator &AddCastToODBCTypes(ClientContext &context, PhysicalPlanGenerator &planner, PhysicalOperator &plan) {
	// check if we need to cast anything
	bool require_cast = false;
	ODBCTypeConfig type_config(context);
	auto &child_types = plan.GetTypes();
	for (auto &type : child_types) {
		auto odbc_type = ODBCTypes::ToODBCType(type_config, type);
		if (odbc_type != type) {
			require_cast = true;
			break;
		}
	}
	if (!require_cast) {
		return plan;
	}

	vector<LogicalType> odbc_types;
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &type = child_types[i];
		unique_ptr<Expression> expr;
		expr = make_uniq<BoundReferenceExpression>(type, i);

		auto odbc_type = ODBCTypes::ToODBCType(type_config, type);
		if (odbc_type != type) {
			// add a cast
			expr = BoundCastExpression::AddCastToType(context, std::move(expr), odbc_type);
		}
		odbc_types.push_back(std::move(odbc_type));
		select_list.push_back(std::move(expr));
	}
	// we need to cast: add casts
	auto &proj =
	    planner.Make<PhysicalProjection>(std::move(odbc_types), std::move(select_list), plan.estimated_cardinality);
	proj.children.push_back(plan);
	return proj;
}

PhysicalOperator &ODBCCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into ODBC table");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into ODBC table");
	}
	ODBCCatalog::MaterializeODBCScans(*plan);

	D_ASSERT(plan);
	auto &inner_plan = AddCastToODBCTypes(context, planner, *plan);
	auto &insert = planner.Make<ODBCInsert>(op, op.table, op.column_index_map);
	insert.children.push_back(inner_plan);
	return insert;
}

PhysicalOperator &ODBCCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	ODBCCatalog::MaterializeODBCScans(plan);
	auto &inner_plan = AddCastToODBCTypes(context, planner, plan);
	auto &insert = planner.Make<ODBCInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(inner_plan);
	return insert;
}

} // namespace duckdb
