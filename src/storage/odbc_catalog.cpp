#include "storage/odbc_catalog.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "odbc_connection.hpp"
#include "odbc_scanner.hpp"
#include "odbc_types.hpp"
#include "storage/odbc_schema_entry.hpp"

namespace duckdb {

ODBCCatalog::ODBCCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                         string default_schema_p, ODBCDialect dialect_p, AccessMode access_mode)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      access_mode(access_mode), schemas(*this), default_schema(std::move(default_schema_p)), dialect(dialect_p),
      connection_pool(*this) {
	StringUtil::Trim(default_schema);
	Value connection_limit;
	auto &db_instance = db_p.GetDatabase();
	if (db_instance.TryGetCurrentSetting(Identifier("odbc_catalog_connection_limit"), connection_limit)) {
		connection_pool.SetMaximumConnections(UBigIntValue::Get(connection_limit));
	}
	Value wait_timeout;
	if (db_instance.TryGetCurrentSetting(Identifier("odbc_catalog_pool_wait_timeout_millis"), wait_timeout)) {
		connection_pool.SetWaitTimeout(UBigIntValue::Get(wait_timeout));
	}
	ODBCTypeConfig type_config(db_p.GetDatabase(), dialect);
	ODBCConnection::Open(type_config, connection_string);
}

ODBCCatalog::~ODBCCatalog() = default;

static unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	return secret_manager.GetSecretByName(transaction, secret_name, "local_file");
}

static string GetSecretString(const KeyValueSecret &secret, const string &name) {
	auto value = secret.TryGetValue(Identifier(name));
	return value.IsNull() ? string() : value.ToString();
}

string ODBCCatalog::GetConnectionString(ClientContext &context, const string &secret_name, ODBCDialect dialect) {
	auto secret_entry = GetSecret(context, secret_name);
	if (!secret_entry) {
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	const auto &secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	if (ODBCDialectUtil::Parse(GetSecretString(secret, "dialect")) != dialect) {
		throw BinderException("ODBC catalog SECRET dialect does not match ATTACH DIALECT");
	}

	ODBCCatalogConnectionOptions options;
	options.driver = GetSecretString(secret, "driver");
	options.host = GetSecretString(secret, "host");
	options.database = GetSecretString(secret, "database");
	options.user = GetSecretString(secret, "user");
	options.password = GetSecretString(secret, "password");
	try {
		auto port = std::stoull(GetSecretString(secret, "port"));
		if (port > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("ODBC catalog SECRET PORT is out of range");
		}
		options.port = static_cast<uint32_t>(port);
	} catch (const InvalidInputException &) {
		throw;
	} catch (...) {
		throw InvalidInputException("ODBC catalog SECRET PORT must be an integer");
	}
	return ODBCDialectUtil::BuildConnectionString(dialect, options);
}

void ODBCCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> ODBCCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

void ODBCCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

void ODBCCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<ODBCSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> ODBCCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException(
			    "ODBC catalog has no default schema; specify SCHEMA in ATTACH or qualify the table");
		}
		schema_name = default_schema;
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool ODBCCatalog::InMemory() {
	return false;
}

string ODBCCatalog::GetDBPath() {
	return attach_path;
}

bool ODBCCatalog::IsODBCScan(const string &name) {
	return name == "odbc_catalog_scan";
}

void ODBCCatalog::MaterializeODBCScans(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = op.Cast<PhysicalTableScan>();
		if (IsODBCScan(table_scan.function.GetName().GetIdentifierName())) {
			table_scan.bind_data->Cast<ODBCBindData>().streaming = ODBCResultStreaming::FORCE_MATERIALIZATION;
		}
	}
	for (auto &child : op.children) {
		MaterializeODBCScans(child);
	}
}

DatabaseSize ODBCCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

void ODBCCatalog::ClearCache() {
	schemas.ClearEntries();
}

PhysicalOperator &ODBCCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

PhysicalOperator &ODBCCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

PhysicalOperator &ODBCCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

PhysicalOperator &ODBCCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

unique_ptr<LogicalOperator> ODBCCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw PermissionException("odbc_catalog_scanner catalogs are read-only");
}

} // namespace duckdb
