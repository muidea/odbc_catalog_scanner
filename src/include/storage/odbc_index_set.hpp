//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/odbc_index_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/odbc_catalog_set.hpp"
#include "storage/odbc_index_entry.hpp"

namespace duckdb {
class MySQLSchemaEntry;

class MySQLIndexSet : public MySQLInSchemaSet {
public:
	MySQLIndexSet(MySQLSchemaEntry &schema);

	void DropEntry(ClientContext &context, DropInfo &info) override;

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
