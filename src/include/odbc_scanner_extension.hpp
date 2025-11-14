#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

class ODBCScannerExtension : public Extension {
public:
	std::string Name() override {
		return "odbc_scanner";
	}
	void Load(ExtensionLoader &loader) override;
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(odbc_scanner, loader);
}
