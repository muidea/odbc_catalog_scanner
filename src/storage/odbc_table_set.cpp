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
	auto table_info = make_uniq<ODBCTableInfo>(schema, table_name, is_view);
	for (auto &field : result->Fields()) {
		if (field.name.empty()) {
			continue;
		}
		table_info->create_info->columns.AddColumn(ColumnDefinition(field.name, field.duckdb_type));
		table_info->remote_column_names.push_back(field.name);
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
