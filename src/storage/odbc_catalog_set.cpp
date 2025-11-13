#include "storage/odbc_catalog_set.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/odbc_schema_entry.hpp"

namespace duckdb {

ODBCCatalogSet::ODBCCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> ODBCCatalogSet::GetEntry(ClientContext &context, const string &name) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void ODBCCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		drop_query += " IF EXISTS ";
	}
	drop_query += ODBCUtils::WriteIdentifier(info.name);
	if (info.type != CatalogType::SCHEMA_ENTRY) {
		if (info.cascade) {
			drop_query += " CASCADE";
		}
	}
	auto &transaction = ODBCTransaction::Get(context, catalog);
	transaction.Query(drop_query);

	// erase the entry from the catalog set
	EraseEntryInternal(info.name);
}

void ODBCCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void ODBCCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> ODBCCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("ODBCCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void ODBCCatalogSet::ClearEntries() {
	entries.clear();
	is_loaded = false;
}

ODBCInSchemaSet::ODBCInSchemaSet(ODBCSchemaEntry &schema) : ODBCCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> ODBCInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return ODBCCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
