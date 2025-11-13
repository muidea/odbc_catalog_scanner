//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/odbc_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"

namespace duckdb {

class MySQLTransactionManager : public TransactionManager {
public:
	MySQLTransactionManager(AttachedDatabase &db_p, MySQLCatalog &odbc_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	MySQLCatalog &odbc_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<MySQLTransaction>> transactions;
};

} // namespace duckdb
