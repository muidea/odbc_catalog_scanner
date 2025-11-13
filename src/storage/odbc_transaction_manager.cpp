#include "storage/odbc_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

MySQLTransactionManager::MySQLTransactionManager(AttachedDatabase &db_p, MySQLCatalog &odbc_catalog)
    : TransactionManager(db_p), odbc_catalog(odbc_catalog) {
}

Transaction &MySQLTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<MySQLTransaction>(odbc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData MySQLTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &odbc_transaction = transaction.Cast<MySQLTransaction>();
	odbc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void MySQLTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &odbc_transaction = transaction.Cast<MySQLTransaction>();
	try {
		odbc_transaction.Rollback();
	} catch (...) {
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void MySQLTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = MySQLTransaction::Get(context, db.GetCatalog());
	auto &db = transaction.GetConnection();
	db.Execute("CHECKPOINT");
}

} // namespace duckdb
