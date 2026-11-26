//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include "odbc_connection.hpp"
#include "odbc_types.hpp"
#include "odbc_utils.hpp"

namespace duckdb {
class ODBCTableEntry;
class ODBCTransaction;

struct ODBCBindData : public FunctionData {
	explicit ODBCBindData(ODBCTableEntry &table) : table(table) {
	}

	ODBCTableEntry &table;
	vector<ODBCType> odbc_types;
	vector<string> names;
	vector<string> remote_names;
	vector<string> remote_type_names;
	vector<LogicalType> types;
	string limit;
	ODBCResultStreaming streaming = ODBCResultStreaming::UNINITIALIZED;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("ODBCBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

class ODBCScanFunction : public TableFunction {
public:
	ODBCScanFunction();
};

class ODBCMetadataFunction : public TableFunction {
public:
	ODBCMetadataFunction();
};

class ODBCRelationalMetadataFunction : public TableFunction {
public:
	explicit ODBCRelationalMetadataFunction(bool vendor = false, bool scoped = false);
};

class ODBCClearCacheFunction : public TableFunction {
public:
	ODBCClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class ODBCClearCacheScopedFunction : public TableFunction {
public:
	ODBCClearCacheScopedFunction();
};

} // namespace duckdb
