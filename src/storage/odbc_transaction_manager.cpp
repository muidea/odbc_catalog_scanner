#include "storage/odbc_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

ODBCTransactionManager::ODBCTransactionManager(AttachedDatabase &db_p, ODBCCatalog &odbc_catalog)
    : TransactionManager(db_p), odbc_catalog(odbc_catalog) {
}

Transaction &ODBCTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<ODBCTransaction>(odbc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData ODBCTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &odbc_transaction = transaction.Cast<ODBCTransaction>();
	odbc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void ODBCTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &odbc_transaction = transaction.Cast<ODBCTransaction>();
	try {
		odbc_transaction.Rollback();
	} catch (...) {
		// Ignore exceptions during rollback - transaction is being rolled back anyway
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void ODBCTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = ODBCTransaction::Get(context, db.GetCatalog());
	auto &db = transaction.GetConnection();
	db.Execute("CHECKPOINT");
}

} // namespace duckdb
