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
		// Don't explicitly commit - let the ODBC driver handle transaction management
		// connection.Execute("COMMIT");
	}
}
void ODBCTransaction::Rollback() {
	if (transaction_state == ODBCTransactionState::TRANSACTION_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_FINISHED;
		// Don't explicitly rollback - let the ODBC driver handle transaction management
		// connection.Execute("ROLLBACK");
	}
}

ODBCConnection &ODBCTransaction::GetConnection() {
	// For ODBC connections, we rely on the driver's default transaction management
	// Many ODBC drivers (including SQL Server) handle transactions automatically
	if (transaction_state == ODBCTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_STARTED;
		// Don't explicitly start a transaction - let the ODBC driver handle it
	}
	return connection;
}

unique_ptr<ODBCResult> ODBCTransaction::Query(const string &query) {
	// For ODBC connections, we rely on the driver's default transaction management
	if (transaction_state == ODBCTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = ODBCTransactionState::TRANSACTION_STARTED;
		// Don't explicitly start a transaction - let the ODBC driver handle it
	}
	return connection.Query(query, ODBCResultStreaming::FORCE_MATERIALIZATION);
}

ODBCTransaction &ODBCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<ODBCTransaction>();
}

} // namespace duckdb
