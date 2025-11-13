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

class ODBCTransactionManager : public TransactionManager {
public:
	ODBCTransactionManager(AttachedDatabase &db_p, ODBCCatalog &odbc_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	ODBCCatalog &odbc_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<ODBCTransaction>> transactions;
};

} // namespace duckdb
