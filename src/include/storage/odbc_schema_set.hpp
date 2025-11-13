//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/odbc_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/odbc_catalog_set.hpp"
#include "storage/odbc_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class MySQLSchemaSet : public MySQLCatalogSet {
public:
	explicit MySQLSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
