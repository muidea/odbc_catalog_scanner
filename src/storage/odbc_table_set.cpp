#include "storage/odbc_table_set.hpp"

#include "odbc_dialect.hpp"
#include "odbc_types.hpp"
#include "odbc_utils.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_transaction.hpp"

namespace duckdb {

ODBCTableSet::ODBCTableSet(ODBCSchemaEntry &schema) : ODBCInSchemaSet(schema) {
}

void ODBCTableSet::LoadEntries(ClientContext &context) {
	auto &odbc_catalog = catalog.Cast<ODBCCatalog>();
	if (odbc_catalog.GetDefaultSchema().empty() &&
	    ODBCDialectUtil::IsInternalSchema(odbc_catalog.GetDialect(), schema.name)) {
		return;
	}
	auto &connection = ODBCTransaction::Get(context, catalog).GetConnection();
	auto objects = ODBCUtils::GetCatalogObjects(connection.GetConn(), schema.name);
	unordered_set<string> created;
	for (auto &object : objects) {
		if (!created.insert(object.name).second) {
			continue;
		}
		auto table_info = GetTableInfo(context, schema, object.name, StringUtil::CIEquals(object.type, "VIEW"));
		CreateEntry(make_uniq<ODBCTableEntry>(catalog, schema, *table_info));
	}
}

static string GetTableInfoQuery(const string &schema_name, const string &table_name) {
	return "SELECT * FROM " + ODBCUtils::WriteIdentifier(schema_name) + "." + ODBCUtils::WriteIdentifier(table_name) +
	       " WHERE 1=0";
}

unique_ptr<ODBCTableInfo> ODBCTableSet::GetTableInfo(ClientContext &context, ODBCSchemaEntry &schema,
                                                     const string &table_name, bool is_view) {
	auto &transaction = ODBCTransaction::Get(context, schema.ParentCatalog());
	auto result = transaction.Query(GetTableInfoQuery(schema.name, table_name));
	struct ParsedODBCColumn {
		string name;
		string type_name;
		LogicalType type;
	};
	vector<ParsedODBCColumn> columns;
	bool has_decimal = false;
	for (auto &field : result->Fields()) {
		if (field.name.empty()) {
			continue;
		}
		if (field.duckdb_type.id() == LogicalTypeId::DECIMAL) {
			has_decimal = true;
		}
		columns.push_back({field.name, field.type_name, field.duckdb_type});
	}
	result.reset();
	if (has_decimal && schema.ParentCatalog().Cast<ODBCCatalog>().GetDialect() == ODBCDialect::HANA) {
		// HANA SQLDescribeCol reports SMALLDECIMAL and unconstrained DECIMAL (SCALE IS NULL) as DECIMAL(p,0);
		// consult SYS.TABLE_COLUMNS / SYS.VIEW_COLUMNS for exact catalog numeric types.
		auto literal = [](const string &s) { return "'" + StringUtil::Replace(s, "'", "''") + "'"; };
		string meta_sql =
		    is_view
		        ? "SELECT COLUMN_NAME FROM SYS.VIEW_COLUMNS WHERE SCHEMA_NAME = " + literal(schema.name) +
		              " AND VIEW_NAME = " + literal(table_name) +
		              " AND (DATA_TYPE_NAME = 'SMALLDECIMAL' OR (DATA_TYPE_NAME = 'DECIMAL' AND SCALE IS NULL))"
		        : "SELECT COLUMN_NAME FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME = " + literal(schema.name) +
		              " AND TABLE_NAME = " + literal(table_name) +
		              " AND (DATA_TYPE_NAME = 'SMALLDECIMAL' OR (DATA_TYPE_NAME = 'DECIMAL' AND SCALE IS NULL))";
		auto hana_cols = transaction.Query(meta_sql);
		unordered_set<string> exact_numeric_cols;
		while (hana_cols->Next()) {
			if (!hana_cols->IsNull(0)) {
				exact_numeric_cols.insert(hana_cols->GetString(0));
			}
		}
		for (auto &col : columns) {
			if (exact_numeric_cols.count(col.name)) {
				col.type = LogicalType::VARCHAR_COLLATION("odbc_exact_numeric");
			}
		}
	}
	auto table_info = make_uniq<ODBCTableInfo>(schema, table_name, is_view);
	for (auto &col : columns) {
		table_info->create_info->columns.AddColumn(ColumnDefinition(col.name, std::move(col.type)));
		table_info->remote_column_names.push_back(col.name);
		table_info->remote_type_names.push_back(col.type_name);
	}
	if (table_info->remote_column_names.empty()) {
		throw IOException("ODBC table metadata contains no readable columns");
	}
	return table_info;
}

optional_ptr<CatalogEntry> ODBCTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto &connection = ODBCTransaction::Get(context, catalog).GetConnection();
	auto objects = ODBCUtils::GetCatalogObjects(connection.GetConn(), schema.name, table_name);
	for (auto &object : objects) {
		if (StringUtil::CIEquals(object.name, table_name)) {
			auto table_info = GetTableInfo(context, schema, object.name, StringUtil::CIEquals(object.type, "VIEW"));
			auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *table_info);
			auto table_ptr = table_entry.get();
			CreateEntry(std::move(table_entry));
			return table_ptr;
		}
	}
	return nullptr;
}

} // namespace duckdb
