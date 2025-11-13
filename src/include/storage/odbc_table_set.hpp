//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/odbc_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/odbc_catalog_set.hpp"
#include "storage/odbc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class ODBCConnection;
class ODBCResult;
class ODBCSchemaEntry;

class ODBCTableSet : public ODBCInSchemaSet {
public:
	explicit ODBCTableSet(ODBCSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	static unique_ptr<ODBCTableInfo> GetTableInfo(ClientContext &context, ODBCSchemaEntry &schema,
	                                               const string &table_name);
	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(ClientContext &context, ODBCResult &result, ODBCTableInfo &table_info,
	                      idx_t column_offset = 0);
};

} // namespace duckdb
