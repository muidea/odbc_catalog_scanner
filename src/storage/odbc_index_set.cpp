#include "storage/odbc_index_set.hpp"
#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/odbc_index_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

ODBCIndexSet::ODBCIndexSet(ODBCSchemaEntry &schema) : ODBCInSchemaSet(schema) {
}

void ODBCIndexSet::DropEntry(ClientContext &context, DropInfo &info) {
	auto entry = GetEntry(context, info.name);
	if (!entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Failed to DROP INDEX \"%s\": entry not found", info.name);
	}
	auto &odbc_index = entry->Cast<ODBCIndexEntry>();
	string drop_query = "DROP INDEX ";
	drop_query += ODBCUtils::WriteIdentifier(info.name);
	drop_query += " ON ";
	drop_query += ODBCUtils::WriteIdentifier(odbc_index.table_name);
	auto &transaction = ODBCTransaction::Get(context, catalog);
	transaction.Query(drop_query);

	EraseEntryInternal(info.name);
}

void ODBCIndexSet::LoadEntries(ClientContext &context) {
	auto query = StringUtil::Replace(R"(
SELECT DISTINCT TABLE_NAME, INDEX_NAME
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'odbcscanner';
)",
	                                 "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name));

	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	while (result->Next()) {
		auto table_name = result->GetString(0);
		auto index_name = result->GetString(1);
		CreateIndexInfo info;
		info.schema = schema.name;
		info.table = table_name;
		info.index_name = index_name;
		auto index_entry = make_uniq<ODBCIndexEntry>(catalog, schema, info, table_name);
		CreateEntry(std::move(index_entry));
	}
}

} // namespace duckdb
