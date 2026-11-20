#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"
#include "odbc_exact_numeric.hpp"

#include "odbc_scanner.hpp"
#include "odbc_storage.hpp"
#include "odbc_catalog_scanner_extension.hpp"
#include "odbc_dialect.hpp"

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

template <typename Fn>
static void ForEachODBCCatalog(ClientContext &context, Fn &&fn) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "odbc_catalog") {
			continue;
		}
		fn(catalog.Cast<ODBCCatalog>());
	}
}

static void SetODBCConnectionLimit(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("odbc_catalog_connection_limit can only be set globally");
	}
	auto new_max = parameter.GetValue<uint64_t>();
	if (new_max == 0) {
		throw InvalidInputException("odbc_catalog_connection_limit must be greater than zero");
	}
	ForEachODBCCatalog(context,
	                   [&](ODBCCatalog &catalog) { catalog.GetConnectionPool().SetMaximumConnections(new_max); });
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("odbc_catalog_connection_limit", parameter);
}

static void SetODBCConnectionCache(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("odbc_catalog_connection_cache can only be set globally");
	}
	ODBCConnectionPool::ODBCSetConnectionCache(context, scope, parameter);
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("odbc_catalog_connection_cache", parameter);
}

static void SetODBCPoolWaitTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("odbc_catalog_pool_wait_timeout_millis can only be set globally");
	}
	auto timeout = parameter.GetValue<uint64_t>();
	ForEachODBCCatalog(context, [&](ODBCCatalog &catalog) { catalog.GetConnectionPool().SetWaitTimeout(timeout); });
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("odbc_catalog_pool_wait_timeout_millis", parameter);
}

unique_ptr<BaseSecret> CreateODBCSecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "odbc_catalog", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "dialect") {
			auto dialect = ODBCDialectUtil::Parse(named_param.second.ToString());
			result->secret_map["dialect"] = ODBCDialectUtil::Name(dialect);
		} else if (lower_name == "driver") {
			result->secret_map["driver"] = named_param.second.ToString();
		} else if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["database"] = named_param.second.ToString();
		} else if (lower_name == "user") {
			result->secret_map["user"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
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
	function.named_parameters["dialect"] = LogicalType::VARCHAR;
	function.named_parameters["driver"] = LogicalType::VARCHAR;
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::UBIGINT;
	function.named_parameters["user"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
	function.named_parameters["database"] = LogicalType::VARCHAR;
}

namespace duckdb {
void RegisterDb2GuardedCRUD(ExtensionLoader &loader);
void RegisterDamengGuardedCRUD(ExtensionLoader &loader);
} // namespace duckdb

static void LoadInternal(ExtensionLoader &loader) {
	RegisterODBCExactNumericCollation(loader, "odbc_exact_numeric");
	RegisterDb2GuardedCRUD(loader);
	RegisterDamengGuardedCRUD(loader);
	// ODBC doesn't require explicit library initialization like MySQL
	// The ODBC driver manager handles initialization automatically
	ODBCClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	ODBCMetadataFunction metadata_function;
	loader.RegisterFunction(metadata_function);
	TableFunctionSet relational_metadata("odbc_relational_metadata");
	relational_metadata.AddFunction(ODBCRelationalMetadataFunction(true, false));
	relational_metadata.AddFunction(ODBCRelationalMetadataFunction(true, true));
	loader.RegisterFunction(relational_metadata);

	SecretType secret_type;
	secret_type.name = "odbc_catalog";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction odbc_secret_function = {"odbc_catalog", "config", CreateODBCSecretFunction};
	SetODBCSecretParameters(odbc_secret_function);
	loader.RegisterFunction(odbc_secret_function);

	auto &db = loader.GetDatabaseInstance();

	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "odbc_catalog", make_shared_ptr<ODBCStorageExtension>());

	config.AddExtensionOption("odbc_catalog_debug_show_queries",
	                          "DEBUG SETTING: print all queries sent to ODBC catalog to stdout", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false), SetODBCDebugQueryPrint);
	config.AddExtensionOption("odbc_catalog_tinyint1_as_boolean",
	                          "Whether or not to convert TINYINT(1) columns to BOOLEAN", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("odbc_catalog_bit1_as_boolean", "Whether or not to convert BIT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("odbc_catalog_time_as_time",
	                          "Whether or not to convert ODBC catalog TIME columns to DuckDB TIME",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("odbc_catalog_connection_limit", "Maximum number of connections per ODBC catalog catalog",
	                          LogicalType::UBIGINT, Value::UBIGINT(ODBCConnectionPool::DEFAULT_MAX_CONNECTIONS),
	                          SetODBCConnectionLimit);
	config.AddExtensionOption("odbc_catalog_connection_cache", "Whether or not to cache idle ODBC catalog connections",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), SetODBCConnectionCache);
	config.AddExtensionOption("odbc_catalog_pool_wait_timeout_millis",
	                          "Maximum time to wait for an available ODBC catalog connection", LogicalType::UBIGINT,
	                          Value::UBIGINT(30000), SetODBCPoolWaitTimeout);

	OptimizerExtension odbc_optimizer;
	odbc_optimizer.optimize_function = ODBCOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(odbc_optimizer));
}

void ODBCCatalogScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(odbc_catalog_scanner, loader) {
	LoadInternal(loader);
}
}
