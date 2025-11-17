#include "storage/odbc_table_set.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parser.hpp"

#include "storage/odbc_transaction.hpp"
#include "storage/odbc_schema_entry.hpp"

#include "odbc_types.hpp"

namespace duckdb {

ODBCTableSet::ODBCTableSet(ODBCSchemaEntry &schema) : ODBCInSchemaSet(schema) {
}

void ODBCTableSet::AddColumn(ClientContext &context, ODBCResult &result, ODBCTableInfo &table_info,
                               idx_t column_offset) {
	try {
		std::cout << "[DEBUG] AddColumn - Starting for column offset: " << column_offset << std::endl;
		
		ODBCTypeData type_info;
		idx_t column_index = column_offset;
		
		// Get column information with error handling
		auto column_name = result.GetString(column_index);
		std::cout << "[DEBUG] AddColumn - Column name: " << column_name << std::endl;
		
		type_info.type_name = result.GetString(column_index + 1);
		type_info.column_type = result.GetString(column_index + 2);
		std::cout << "[DEBUG] AddColumn - Type name: " << type_info.type_name << ", Column type: " << type_info.column_type << std::endl;
		
		string default_value;
		auto is_nullable = result.GetString(column_index + 4);
		std::cout << "[DEBUG] AddColumn - Is nullable: " << is_nullable << std::endl;
		
		type_info.precision = result.IsNull(column_index + 5) ? -1 : result.GetInt64(column_index + 5);
		type_info.scale = result.IsNull(column_index + 6) ? -1 : result.GetInt64(column_index + 6);
		std::cout << "[DEBUG] AddColumn - Precision: " << type_info.precision << ", Scale: " << type_info.scale << std::endl;

		// Convert to logical type
		std::cout << "[DEBUG] AddColumn - Converting to logical type..." << std::endl;
		auto column_type = ODBCTypes::TypeToLogicalType({context}, type_info);
		std::cout << "[DEBUG] AddColumn - Logical type: " << column_type.ToString() << std::endl;
		
		ColumnDefinition column(std::move(column_name), std::move(column_type));
		if (!default_value.empty()) {
			auto expressions = Parser::ParseExpressionList(default_value);
			if (expressions.empty()) {
				throw InternalException("Expression list is empty");
			}
			column.SetDefaultValue(std::move(expressions[0]));
		}
		
		auto &create_info = *table_info.create_info;
		if (is_nullable != "YES") {
			auto column_idx = create_info.columns.LogicalColumnCount();
			create_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_idx)));
		}
		
		std::cout << "[DEBUG] AddColumn - Adding column to table..." << std::endl;
		create_info.columns.AddColumn(std::move(column));
		std::cout << "[DEBUG] AddColumn - Column added successfully" << std::endl;
	} catch (const std::exception &e) {
		std::cout << "[ERROR] AddColumn - Exception: " << e.what() << std::endl;
		throw;
	} catch (...) {
		std::cout << "[ERROR] AddColumn - Unknown exception occurred" << std::endl;
		throw;
	}
}

void ODBCTableSet::LoadEntries(ClientContext &context) {
	// Debug: Print schema name being queried
	std::cout << "[DEBUG] ODBCTableSet::LoadEntries - Querying schema: " << schema.name << std::endl;

	// Use INFORMATION_SCHEMA for ODBC compatibility instead of SQL Server specific sys.* tables
	auto query = StringUtil::Replace(R"(
SELECT
    t.TABLE_NAME AS table_name,
    c.COLUMN_NAME AS column_name,
    c.DATA_TYPE AS data_type,
    c.DATA_TYPE AS column_type,
    c.COLUMN_DEFAULT AS column_default,
    c.IS_NULLABLE AS is_nullable,
    c.NUMERIC_PRECISION AS numeric_precision,
    c.NUMERIC_SCALE AS numeric_scale
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
WHERE c.TABLE_SCHEMA = ${SCHEMA_NAME} AND t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION;
)",
	                                 "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name));

	std::cout << "[DEBUG] Executing table query:\n" << query << std::endl;

	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto result = transaction.Query(query);

	vector<unique_ptr<ODBCTableInfo>> tables;
	unique_ptr<ODBCTableInfo> info;

	// Debug: Check if we got any results
	bool has_results = false;
	int row_count = 0;
	while (result->Next()) {
		try {
			has_results = true;
			row_count++;
			auto table_name = result->GetString(0);
			auto column_name = result->GetString(1);
			auto data_type = result->GetString(2);
			
			if (row_count <= 10) { // Only print first 10 rows to avoid too much output
				std::cout << "[DEBUG] Row " << row_count << ": table=" << table_name
				          << ", column=" << column_name << ", type=" << data_type << std::endl;
			}
			
			if (!info || info->GetTableName() != table_name) {
				if (info) {
					tables.push_back(std::move(info));
				}
				info = make_uniq<ODBCTableInfo>(schema, table_name);
				std::cout << "[DEBUG] Created new table info for: " << table_name << std::endl;
			}
			
			std::cout << "[DEBUG] Processing column: " << column_name << " for table: " << table_name << std::endl;
			AddColumn(context, *result, *info, 1);
			std::cout << "[DEBUG] Successfully processed column: " << column_name << std::endl;
		} catch (const std::exception &e) {
			std::cout << "[ERROR] Error processing row " << row_count << ": " << e.what() << std::endl;
			throw;
		}
	}
	
	if (info) {
		tables.push_back(std::move(info));
	}
	
	std::cout << "[DEBUG] Query result: " << row_count << " rows found, " << tables.size() << " tables created" << std::endl;
	
	// Debug output
	if (!has_results) {
		std::cout << "[DEBUG] No results from INFORMATION_SCHEMA query, trying alternative queries..." << std::endl;
		
		// Try alternative query to debug schema issue
		auto debug_query1 = StringUtil::Replace(R"(
SELECT name FROM sys.schemas ORDER BY name;
)", "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name));
		std::cout << "[DEBUG] Executing schema query:\n" << debug_query1 << std::endl;
		auto debug_result1 = transaction.Query(debug_query1);
		vector<string> available_schemas;
		while (debug_result1->Next()) {
			available_schemas.push_back(debug_result1->GetString(0));
		}
		std::cout << "[DEBUG] Available schemas: " << StringUtil::Join(available_schemas, ", ") << std::endl;
		
		// Try querying all tables without schema filter
		auto debug_query2 = R"(
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA, TABLE_NAME;
)";
		std::cout << "[DEBUG] Executing all tables query:\n" << debug_query2 << std::endl;
		auto debug_result2 = transaction.Query(debug_query2);
		vector<string> all_tables;
		while (debug_result2->Next()) {
			auto table_schema = debug_result2->GetString(0);
			auto table_name = debug_result2->GetString(1);
			auto table_type = debug_result2->GetString(2);
			all_tables.push_back(table_schema + "." + table_name + " (" + table_type + ")");
		}
		std::cout << "[DEBUG] All tables found: " << StringUtil::Join(all_tables, ", ") << std::endl;
	}
	
	for (auto &tbl_info : tables) {
		auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *tbl_info);
		CreateEntry(std::move(table_entry));
		std::cout << "[DEBUG] Created table entry: " << tbl_info->GetTableName() << std::endl;
	}
	
	std::cout << "[DEBUG] ODBCTableSet::LoadEntries completed, created " << tables.size() << " table entries" << std::endl;
}

string GetTableInfoQuery(const string &schema_name, const string &table_name) {
	return StringUtil::Replace(StringUtil::Replace(R"(
SELECT
    c.COLUMN_NAME AS column_name,
    c.DATA_TYPE AS data_type,
    c.DATA_TYPE AS column_type,
    c.COLUMN_DEFAULT AS column_default,
    c.IS_NULLABLE AS is_nullable,
    c.NUMERIC_PRECISION AS numeric_precision,
    c.NUMERIC_SCALE AS numeric_scale
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = ${SCHEMA_NAME} AND c.TABLE_NAME = ${TABLE_NAME}
ORDER BY c.ORDINAL_POSITION;
)",
	                                               "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema_name)),
	                           "${TABLE_NAME}", ODBCUtils::WriteLiteral(table_name));
}

unique_ptr<ODBCTableInfo> ODBCTableSet::GetTableInfo(ClientContext &context, ODBCSchemaEntry &schema,
                                                       const string &table_name) {
	auto &transaction = ODBCTransaction::Get(context, schema.ParentCatalog());
	auto query = GetTableInfoQuery(schema.name, table_name);
	auto result = transaction.Query(query);
	auto table_info = make_uniq<ODBCTableInfo>(schema, table_name);
	while (result->Next()) {
		AddColumn(context, *result, *table_info, 0);
	}
	return table_info;
}

optional_ptr<CatalogEntry> ODBCTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

// FIXME - this is almost entirely copied from TableCatalogEntry::ColumnsToSQL -
// should be unified
string ODBCColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key
	// columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			vector<string> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after
				// all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << ODBCUtils::WriteIdentifier(column.Name()) << " ";
		ss << ODBCTypes::TypeToString(column.Type());
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue().ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string GetODBCCreateTable(ClientContext &context, CreateTableInfo &info) {
	ODBCTypeConfig type_config(context);
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		col.SetType(ODBCTypes::ToODBCType(type_config, col.GetType()));
	}

	std::stringstream ss;
	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!info.schema.empty()) {
		ss << ODBCUtils::WriteIdentifier(info.schema);
		ss << ".";
	}
	ss << ODBCUtils::WriteIdentifier(info.table);
	ss << ODBCColumnsToSQL(info.columns, info.constraints);
	ss << ";";
	return ss.str();
}

optional_ptr<CatalogEntry> ODBCTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto create_sql = GetODBCCreateTable(context, info.Base());
	transaction.Query(create_sql);
	auto tbl_entry = make_uniq<ODBCTableEntry>(catalog, schema, info.Base());
	return CreateEntry(std::move(tbl_entry));
}

void ODBCTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	auto &transaction = ODBCTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += ODBCUtils::WriteIdentifier(info.name);
	sql += " RENAME TO ";
	sql += ODBCUtils::WriteIdentifier(info.new_table_name);
	transaction.Query(sql);
}

void ODBCTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	auto &transaction = ODBCTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += ODBCUtils::WriteIdentifier(info.name);
	sql += " RENAME COLUMN  ";
	sql += ODBCUtils::WriteIdentifier(info.old_name);
	sql += " TO ";
	sql += ODBCUtils::WriteIdentifier(info.new_name);

	transaction.Query(sql);
}

void ODBCTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	auto &transaction = ODBCTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += ODBCUtils::WriteIdentifier(info.name);
	sql += " ADD COLUMN  ";
	if (info.if_column_not_exists) {
		sql += "IF NOT EXISTS ";
	}
	sql += ODBCUtils::WriteIdentifier(info.new_column.Name());
	sql += " ";
	sql += info.new_column.Type().ToString();
	transaction.Query(sql);
}

void ODBCTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	auto &transaction = ODBCTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += ODBCUtils::WriteIdentifier(info.name);
	sql += " DROP COLUMN  ";
	if (info.if_column_exists) {
		throw NotImplementedException("DROP COLUMN IF EXISTS not supported in ODBC");
	}
	sql += ODBCUtils::WriteIdentifier(info.removed_column);
	transaction.Query(sql);
}

void ODBCTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(context, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(context, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(context, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(context, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - ODBC tables only "
		                      "support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	ClearEntries();
}

} // namespace duckdb
