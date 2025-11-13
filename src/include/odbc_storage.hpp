//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class ODBCStorageExtension : public StorageExtension {
public:
	ODBCStorageExtension();
};

} // namespace duckdb
