//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/odbc_index_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

class ODBCIndexEntry : public IndexCatalogEntry {
public:
	ODBCIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info, string table_name);

	string table_name;

public:
	string GetSchemaName() const override;
	string GetTableName() const override;
};

} // namespace duckdb
