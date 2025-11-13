#include "storage/odbc_transaction.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "odbc_result.hpp"
#include "odbc_types.hpp"
#include "storage/odbc_catalog.hpp"

namespace duckdb {

ODBCTransaction::ODBCTransaction(ODBCCatalog &odbc_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context),
      connection(ODBCConnection::Open(ODBCTypeConfig(context), odbc_catalog.connection_string)),
      access_mode(odbc_catalog.access_mode) {
	string time_zone;
	{
		Value odbc_session_time_zone;
		if (context.TryGetCurrentSetting("odbc_session_time_zone", odbc_session_time_zone)) {
			time_zone = odbc_session_time_zone.ToString();
		}
	}
	if (!time_zone.empty()) {
		connection.Execute("SET TIME_ZONE = '" + time_zone + "'");
	}
}

ODBCTransaction::~ODBCTransaction() = default;

void ODBCTransaction::Start() {
	transaction_state = ODBCTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void ODBCTransaction::Commit() {
	if (transaction_state == ODBCTransactionState::TRANSACTION_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_FINISHED;
		connection.Execute("COMMIT");
	}
}
void ODBCTransaction::Rollback() {
	if (transaction_state == ODBCTransactionState::TRANSACTION_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_FINISHED;
		connection.Execute("ROLLBACK");
	}
}

ODBCConnection &ODBCTransaction::GetConnection() {
	if (transaction_state == ODBCTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_STARTED;
		string query = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			query += " READ ONLY";
		}
		connection.Execute(query);
	}
	return connection;
}

unique_ptr<ODBCResult> ODBCTransaction::Query(const string &query) {
	if (transaction_state == ODBCTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_STARTED;
		string transaction_start = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		connection.Execute(transaction_start);
		return connection.Query(query, ODBCResultStreaming::FORCE_MATERIALIZATION);
	}
	return connection.Query(query, ODBCResultStreaming::FORCE_MATERIALIZATION);
}

ODBCTransaction &ODBCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<ODBCTransaction>();
}

} // namespace duckdb
