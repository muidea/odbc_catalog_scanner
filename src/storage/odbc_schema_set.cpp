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
	// Use standard ODBC catalog functions or INFORMATION_SCHEMA for schema enumeration
	auto query = R"(
SELECT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
ORDER BY SCHEMA_NAME;
)";

	std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - Executing schema query:\n" << query << std::endl;

	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	
	int schema_count = 0;
	vector<string> schema_names;
	while (result->Next()) {
		schema_count++;
		CreateSchemaInfo info;
		info.schema = result->GetString(0);
		info.internal = ODBCSchemaIsInternal(info.schema);
		auto schema = make_uniq<ODBCSchemaEntry>(catalog, info);
		CreateEntry(std::move(schema));
		schema_names.push_back(info.schema);
		std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - Found schema: " << info.schema
		          << " (internal: " << info.internal << ")" << std::endl;
	}
	
	std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - Found " << schema_count << " schemas" << std::endl;
	
	// Print usage information for the user
	if (!schema_names.empty()) {
		std::cout << "[INFO] Available schemas: " << StringUtil::Join(schema_names, ", ") << std::endl;
		std::cout << "[INFO] To use a specific schema, run: USE schema_name (e.g., USE dbo)" << std::endl;
	}
	
	// If no schemas found with INFORMATION_SCHEMA, try alternative methods
	if (schema_count == 0) {
		std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - No schemas found with INFORMATION_SCHEMA, trying alternative..." << std::endl;
		
		// Try SQL Server specific query as fallback
		auto fallback_query = R"(
SELECT name AS schema_name
FROM sys.databases;
)";
		std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - Trying fallback query:\n" << fallback_query << std::endl;
		auto fallback_result = transaction.Query(fallback_query);
		while (fallback_result->Next()) {
			schema_count++;
			CreateSchemaInfo info;
			info.schema = fallback_result->GetString(0);
			info.internal = ODBCSchemaIsInternal(info.schema);
			auto schema = make_uniq<ODBCSchemaEntry>(catalog, info);
			CreateEntry(std::move(schema));
			schema_names.push_back(info.schema);
			std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - Found schema (fallback): " << info.schema
			          << " (internal: " << info.internal << ")" << std::endl;
		}
		std::cout << "[DEBUG] ODBCSchemaSet::LoadEntries - Fallback found " << schema_count << " schemas" << std::endl;
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
