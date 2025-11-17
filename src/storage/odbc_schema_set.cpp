#include "storage/odbc_schema_set.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

static bool ODBCSchemaIsInternal(const string &name) {
	if (name == "information_schema" || name == "performance_schema" || name == "sys") {
		return true;
	}
	return false;
}

ODBCSchemaSet::ODBCSchemaSet(Catalog &catalog) : ODBCCatalogSet(catalog) {
}

void ODBCSchemaSet::LoadEntries(ClientContext &context) {
	auto query = R"(
SELECT name AS schema_name
FROM sys.databases;
)";

	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	while (result->Next()) {
		CreateSchemaInfo info;
		info.schema = result->GetString(0);
		info.internal = ODBCSchemaIsInternal(info.schema);
		auto schema = make_uniq<ODBCSchemaEntry>(catalog, info);
		CreateEntry(std::move(schema));
	}
}

optional_ptr<CatalogEntry> ODBCSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &transaction = ODBCTransaction::Get(context, catalog);

	string create_sql = "CREATE SCHEMA " + ODBCUtils::WriteIdentifier(info.schema);
	transaction.Query(create_sql);
	auto schema_entry = make_uniq<ODBCSchemaEntry>(catalog, info);
	return CreateEntry(std::move(schema_entry));
}

} // namespace duckdb
