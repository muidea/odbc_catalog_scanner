#include "storage/odbc_table_set.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include <algorithm>

#include "storage/odbc_transaction.hpp"
#include "storage/odbc_schema_entry.hpp"

#include "odbc_types.hpp"

namespace duckdb {

ODBCTableSet::ODBCTableSet(ODBCSchemaEntry &schema) : ODBCInSchemaSet(schema) {
}

static string NormalizeObjectName(string name) {
	name.erase(std::remove(name.begin(), name.end(), '\0'), name.end());
	StringUtil::Trim(name);
	return name;
}

void ODBCTableSet::AddColumn(ClientContext &context, ODBCResult &result, ODBCTableInfo &table_info,
                               idx_t column_offset) {
	ODBCTypeData type_info;
	idx_t column_index = column_offset;

	auto column_name = NormalizeObjectName(result.GetString(column_index));
	if (column_name.empty()) {
		column_name = "column_" + std::to_string(table_info.create_info->columns.LogicalColumnCount() + 1);
	}

	type_info.type_name = result.GetString(column_index + 1);
	type_info.column_type = result.GetString(column_index + 2);

	string default_value;
	auto is_nullable = result.GetString(column_index + 4);

	type_info.precision = result.IsNull(column_index + 5) ? -1 : result.GetInt64(column_index + 5);
	type_info.scale = result.IsNull(column_index + 6) ? -1 : result.GetInt64(column_index + 6);

	// Convert to logical type
	auto column_type = ODBCTypes::TypeToLogicalType({context}, type_info);

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

	create_info.columns.AddColumn(std::move(column));
}

void ODBCTableSet::LoadEntries(ClientContext &context) {
	// First fetch the list of base tables in this schema
	auto query = StringUtil::Replace(R"(
SELECT
    t.TABLE_NAME AS table_name
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_SCHEMA = ${SCHEMA_NAME} AND t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_NAME;
)",
	                                 "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name));

	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto result = transaction.Query(query);

	while (result->Next()) {
		auto table_name = NormalizeObjectName(result->GetString(0));
		if (table_name.empty()) {
			continue;
		}
		// Skip obviously corrupted table names to avoid failing the whole schema
		// load because of encoding issues on a single table.
		if (table_name.find('?') != string::npos) {
			continue;
		}
		try {
			auto table_info = GetTableInfo(context, schema, table_name);
			if (!table_info) {
				continue;
			}
			auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *table_info);
			CreateEntry(std::move(table_entry));
		} catch (...) {
			// Ignore per-table probe errors (e.g., 42S02 for corrupted names)
			// and continue loading remaining tables.
			continue;
		}
	}
}

string GetTableInfoQuery(const string &schema_name, const string &table_name) {
	// Use a lightweight probe query to let the ODBC driver describe the real columns.
	// This avoids relying on INFORMATION_SCHEMA.COLUMNS, which can be incomplete or
	// affected by encoding issues on some drivers/platforms.
	string sql;
	sql += "SELECT * FROM ";
	sql += ODBCUtils::WriteIdentifier(schema_name);
	sql += ".";
	sql += ODBCUtils::WriteIdentifier(table_name);
	sql += " WHERE 1=0";
	return sql;
}

unique_ptr<ODBCTableInfo> ODBCTableSet::GetTableInfo(ClientContext &context, ODBCSchemaEntry &schema,
                                                       const string &table_name) {
	auto &transaction = ODBCTransaction::Get(context, schema.ParentCatalog());
	auto query = GetTableInfoQuery(schema.name, table_name);
	auto result = transaction.Query(query);
	auto table_info = make_uniq<ODBCTableInfo>(schema, table_name);
	// The probe query returns no rows; we only need the field metadata.
	for (auto &field : result->Fields()) {
		auto column_name = NormalizeObjectName(field.name);
		if (column_name.empty()) {
			continue;
		}
		ColumnDefinition column(std::move(column_name), field.duckdb_type);
		table_info->create_info->columns.AddColumn(std::move(column));
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
