#include "storage/odbc_schema_set.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "odbc_dialect.hpp"
#include "odbc_utils.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"

namespace duckdb {

ODBCSchemaSet::ODBCSchemaSet(Catalog &catalog) : ODBCCatalogSet(catalog) {
}

void ODBCSchemaSet::LoadEntries(ClientContext &context) {
	auto &odbc_catalog = catalog.Cast<ODBCCatalog>();
	auto &connection = ODBCTransaction::Get(context, catalog).GetConnection();
	auto default_schema_opt = odbc_catalog.GetDefaultSchema();
	string default_schema = default_schema_opt.has_value() ? default_schema_opt->GetIdentifierName() : string();
	auto objects = ODBCUtils::GetCatalogObjects(connection.GetConn(), default_schema);

	unordered_set<string> created;
	for (auto &object : objects) {
		// A configured schema is an explicit user choice.  Keep system-schema
		// filtering for broad discovery only, otherwise SYSTEM (HANA) and SYSDBA
		// (Dameng) could never be attached even when intentionally requested.
		if ((default_schema.empty() && ODBCDialectUtil::IsInternalSchema(odbc_catalog.GetDialect(), object.schema)) ||
		    !created.insert(object.schema).second) {
			continue;
		}
		CreateSchemaInfo info;
		info.SetSchema(Identifier(object.schema));
		info.internal = false;
		CreateEntry(make_uniq<ODBCSchemaEntry>(catalog, info));
	}
}

optional_ptr<CatalogEntry> ODBCSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

} // namespace duckdb
