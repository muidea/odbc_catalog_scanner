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
class ODBCCatalog;
class ODBCSchemaEntry;
class ODBCTableEntry;

enum class ODBCTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class ODBCTransaction : public Transaction {
public:
	ODBCTransaction(ODBCCatalog &odbc_catalog, TransactionManager &manager, ClientContext &context);
	~ODBCTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	ODBCConnection &GetConnection();
	unique_ptr<ODBCResult> Query(const string &query);
	static ODBCTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	ODBCConnection connection;
	ODBCTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
