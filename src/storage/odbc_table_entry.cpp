#include "storage/odbc_catalog.hpp"
#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_table_entry.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "odbc_scanner.hpp"

namespace duckdb {

static bool TableIsInternal(const SchemaCatalogEntry &schema, const string &name) {
	if (schema.name != "odbc") {
		return false;
	}
	// this is a list of all system tables
	// https://dev.odbc.com/doc/refman/8.0/en/system-schema.html#system-schema-data-dictionary-tables
	unordered_set<string> system_tables {"audit_log_filter",
	                                     "audit_log_user",
	                                     "columns_priv",
	                                     "component",
	                                     "db",
	                                     "default_roles",
	                                     "engine_cost",
	                                     "event",
	                                     "events",
	                                     "firewall_group_allowlist",
	                                     "firewall_groups",
	                                     "firewall_memebership",
	                                     "firewall_users",
	                                     "firewall_whitelist",
	                                     "func",
	                                     "general_log",
	                                     "global_grants",
	                                     "gtid_executed",
	                                     "help_category",
	                                     "help_keyword",
	                                     "help_relation",
	                                     "help_topic",
	                                     "innodb_index_stats",
	                                     "innodb_table_stats",
	                                     "innodb_dynamic_metadata",
	                                     "ndb_binlog_index",
	                                     "password_history",
	                                     "parameters",
	                                     "plugin",
	                                     "procs_priv",
	                                     "proxies_priv",
	                                     "replication_asynchronous_connection_failover",
	                                     "replication_asynchronous_connection_failover_managed",
	                                     "replication_group_configuration_version",
	                                     "replication_group_member_actions",
	                                     "role_edges",
	                                     "routines",
	                                     "server_cost",
	                                     "servers",
	                                     "slave_master_info",
	                                     "slave_relay_log_info",
	                                     "slave_worker_info",
	                                     "slow_log",
	                                     "tables_priv",
	                                     "time_zone",
	                                     "time_zone_leap_second",
	                                     "time_zone_name",
	                                     "time_zone_transition",
	                                     "time_zone_transition_type",
	                                     "user"};
	return system_tables.find(name) != system_tables.end();
}

ODBCTableEntry::ODBCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = TableIsInternal(schema, name);
}

ODBCTableEntry::ODBCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, ODBCTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info), remote_column_names(info.remote_column_names) {
	this->internal = TableIsInternal(schema, name);
}

unique_ptr<BaseStatistics> ODBCTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void ODBCTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                            ClientContext &) {
}

TableFunction ODBCTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<ODBCBindData>(*this);
	idx_t column_index = 0;
	for (auto &col : columns.Logical()) {
		result->types.push_back(col.GetType());
		result->names.push_back(col.GetName());
		if (column_index < remote_column_names.size()) {
			result->remote_names.push_back(remote_column_names[column_index]);
		} else {
			result->remote_names.push_back(col.GetName());
		}
		column_index++;
	}

	bind_data = std::move(result);

	auto function = ODBCScanFunction();
	Value filter_pushdown;
	if (context.TryGetCurrentSetting("odbc_experimental_filter_pushdown", filter_pushdown)) {
		function.filter_pushdown = BooleanValue::Get(filter_pushdown);
	}
	return function;
}

TableStorageInfo ODBCTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<ODBCTransaction>();
	auto &db = transaction.GetConnection();
	TableStorageInfo result;
	result.cardinality = 0;
	result.index_info = db.GetIndexInfo(name);
	return result;
}

} // namespace duckdb
