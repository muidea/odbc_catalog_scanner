#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "odbc_scanner.hpp"
#include "odbc_storage.hpp"
#include "odbc_scanner_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_optimizer.hpp"

using namespace duckdb;

static void SetODBCDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	ODBCConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

unique_ptr<BaseSecret> CreateODBCSecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "odbc", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "driver") {
			result->secret_map["driver"] = named_param.second.ToString();
		} else if (lower_name == "server") {
			result->secret_map["server"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["database"] = named_param.second.ToString();
		} else if (lower_name == "uid") {
			result->secret_map["uid"] = named_param.second.ToString();
		} else if (lower_name == "pwd") {
			result->secret_map["pwd"] = named_param.second.ToString();
		} else if (lower_name == "port") {
			result->secret_map["port"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreateODBCSecretFunction: " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

void SetODBCSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["driver"] = LogicalType::VARCHAR;
	function.named_parameters["server"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["uid"] = LogicalType::VARCHAR;
	function.named_parameters["pwd"] = LogicalType::VARCHAR;
	function.named_parameters["database"] = LogicalType::VARCHAR;
}

static void LoadInternal(ExtensionLoader &loader) {
	// ODBC doesn't require explicit library initialization like MySQL
	// The ODBC driver manager handles initialization automatically
	ODBCClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	ODBCExecuteFunction execute_function;
	loader.RegisterFunction(execute_function);

	ODBCQueryFunction query_function;
	loader.RegisterFunction(query_function);

	SecretType secret_type;
	secret_type.name = "odbc";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction odbc_secret_function = {"odbc", "config", CreateODBCSecretFunction};
	SetODBCSecretParameters(odbc_secret_function);
	loader.RegisterFunction(odbc_secret_function);

	auto &db = loader.GetDatabaseInstance();

	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "odbc_scanner", make_shared_ptr<ODBCStorageExtension>());

	config.AddExtensionOption("odbc_experimental_filter_pushdown",
	                          "Whether or not to use filter pushdown (currently experimental)", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
	config.AddExtensionOption("odbc_debug_show_queries", "DEBUG SETTING: print all queries sent to ODBC to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetODBCDebugQueryPrint);
	config.AddExtensionOption("odbc_tinyint1_as_boolean", "Whether or not to convert TINYINT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("odbc_bit1_as_boolean", "Whether or not to convert BIT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("odbc_session_time_zone",
	                          "Value to use as a session time zone for newly opened"
	                          " connections to ODBC server",
	                          LogicalType::VARCHAR, Value(""), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("odbc_time_as_time", "Whether or not to convert ODBC's TIME columns to DuckDB's TIME",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false),
	                          ODBCClearCacheFunction::ClearCacheOnSetting);

	OptimizerExtension odbc_optimizer;
	odbc_optimizer.optimize_function = ODBCOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(odbc_optimizer));
}

void ODBCScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(odbc_scanner, loader) {
	LoadInternal(loader);
}
}
