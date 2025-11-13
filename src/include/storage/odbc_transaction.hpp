//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/odbc_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "odbc_connection.hpp"

namespace duckdb {
class MySQLCatalog;
class MySQLSchemaEntry;
class MySQLTableEntry;

enum class MySQLTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class MySQLTransaction : public Transaction {
public:
	MySQLTransaction(MySQLCatalog &odbc_catalog, TransactionManager &manager, ClientContext &context);
	~MySQLTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	MySQLConnection &GetConnection();
	unique_ptr<MySQLResult> Query(const string &query);
	static MySQLTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	MySQLConnection connection;
	MySQLTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
