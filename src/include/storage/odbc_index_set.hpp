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
class ODBCSchemaEntry;

class ODBCIndexSet : public ODBCInSchemaSet {
public:
	ODBCIndexSet(ODBCSchemaEntry &schema);

	void DropEntry(ClientContext &context, DropInfo &info) override;

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
