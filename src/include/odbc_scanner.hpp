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

class ODBCClearCacheFunction : public TableFunction {
public:
	ODBCClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

} // namespace duckdb
