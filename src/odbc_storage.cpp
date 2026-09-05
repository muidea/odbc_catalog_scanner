#include "duckdb.hpp"

#include "odbc_storage.hpp"
#include "storage/odbc_catalog.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/odbc_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> ODBCAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                      AttachedDatabase &db, const string &name, AttachInfo &info,
                                      AttachOptions &attach_options) {
	auto &config = DBConfig::GetConfig(context);
	if (!Settings::Get<EnableExternalAccessSetting>(config)) {
		throw PermissionException("Attaching ODBC catalog databases is disabled through configuration");
	}
	// check if we have a secret provided
	string secret_name;
	string schema_name;
	string dialect_name;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else if (lower_name == "schema") {
			schema_name = entry.second.ToString();
		} else if (lower_name == "dialect") {
			dialect_name = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for ODBC catalog attach: %s", entry.first);
		}
	}
	if (attach_options.access_mode != AccessMode::READ_ONLY) {
		throw BinderException("odbc_catalog_scanner is read-only; add READ_ONLY to the ATTACH options");
	}
	if (secret_name.empty()) {
		throw BinderException("odbc_catalog attach requires a named SECRET");
	}
	if (dialect_name.empty()) {
		throw BinderException("odbc_catalog attach requires DIALECT 'hana', 'dameng', or 'db2'");
	}
	if (!info.path.empty()) {
		throw BinderException("odbc_catalog attach does not accept a connection string; use a named SECRET");
	}

	auto dialect = ODBCDialectUtil::Parse(dialect_name);
	auto connection_string = ODBCCatalog::GetConnectionString(context, secret_name, dialect);
	return make_uniq<ODBCCatalog>(db, std::move(connection_string), string(), std::move(schema_name), dialect,
	                              attach_options.access_mode);
}

static unique_ptr<TransactionManager> ODBCCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                   AttachedDatabase &db, Catalog &catalog) {
	auto &odbc_catalog = catalog.Cast<ODBCCatalog>();
	return make_uniq<ODBCTransactionManager>(db, odbc_catalog);
}

ODBCStorageExtension::ODBCStorageExtension() {
	attach = ODBCAttach;
	create_transaction_manager = ODBCCreateTransactionManager;
}

} // namespace duckdb
