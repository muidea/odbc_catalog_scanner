#include "odbc_connection.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "odbc_parameter.hpp"
#include "odbc_types.hpp"

namespace duckdb {

static bool debug_odbc_print_queries = true;

ODBCConnection::ODBCConnection(shared_ptr<OwnedODBCConnection> connection_p, const std::string &dsn_p,
                                 ODBCTypeConfig type_config_p)
    : connection(std::move(connection_p)), dsn(std::move(dsn_p)), type_config(std::move(type_config_p)) {
}

ODBCConnection::~ODBCConnection() {
	Close();
}

ODBCConnection::ODBCConnection(ODBCConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(type_config, other.type_config);
}

ODBCConnection &ODBCConnection::operator=(ODBCConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(type_config, other.type_config);
	return *this;
}

ODBCConnection ODBCConnection::Open(ODBCTypeConfig type_config, const string &connection_string) {
	auto connection = make_shared_ptr<OwnedODBCConnection>(ODBCUtils::Connect(connection_string));
	return ODBCConnection(std::move(connection), connection_string, std::move(type_config));
}

idx_t ODBCConnection::ODBCExecute(SQLHSTMT stmt, const string &query, vector<Value> params, bool streaming) {
	if (ODBCConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	// Debug: Print query and parameters
	std::cout << "[DEBUG] ODBCExecute - Query: " << query << std::endl;
	if (!params.empty()) {
		std::cout << "[DEBUG] ODBCExecute - Parameters count: " << params.size() << std::endl;
		for (size_t i = 0; i < params.size(); i++) {
			std::cout << "[DEBUG] ODBCExecute - Param " << i << ": " << params[i].ToString() << std::endl;
		}
	}

	lock_guard<mutex> l(query_lock);

	SQLRETURN retcode;

	if (!stmt) { // basic interface
		auto hdbc = GetConn();
		SQLHSTMT hstmt;
		retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
		if (!SQL_SUCCEEDED(retcode)) {
			std::cout << "[DEBUG] ODBCExecute - Failed to allocate statement handle" << std::endl;
			throw IOException("Failed to allocate statement handle for ODBC query \"%s\"\n", query.c_str());
		}

		std::cout << "[DEBUG] ODBCExecute - Executing direct query" << std::endl;
		retcode = SQLExecDirect(hstmt, (SQLCHAR*)query.c_str(), SQL_NTS);
		if (!SQL_SUCCEEDED(retcode)) {
			std::cout << "[DEBUG] ODBCExecute - SQLExecDirect failed" << std::endl;
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
			throw IOException("Failed to run query \"%s\"\n", query.c_str());
		}
		
		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		std::cout << "[DEBUG] ODBCExecute - Basic interface query completed" << std::endl;
		return 0;
	}

	// prepared statement interface

	std::cout << "[DEBUG] ODBCExecute - Preparing statement" << std::endl;
	retcode = SQLPrepare(stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "[DEBUG] ODBCExecute - SQLPrepare failed" << std::endl;
		throw IOException("Failed to prepare ODBC query \"%s\"\n", query.c_str());
	}

	vector<ODBCParameter> odbc_params;
	if (params.size() > 0) {
		SQLSMALLINT param_count;
		retcode = SQLNumParams(stmt, &param_count);
		if (!SQL_SUCCEEDED(retcode)) {
			std::cout << "[DEBUG] ODBCExecute - SQLNumParams failed" << std::endl;
			throw IOException("Failed to get parameter count for ODBC query \"%s\"\n", query.c_str());
		}
		
		std::cout << "[DEBUG] ODBCExecute - Parameter count: " << param_count << std::endl;
		if (param_count != params.size()) {
			std::cout << "[DEBUG] ODBCExecute - Parameter count mismatch: expected " << param_count
			          << ", got " << params.size() << std::endl;
			throw IOException(
			    "Incorrect query parameters count specified, expected: %d, actual: %zu, ODBC query \"%s\"\n",
			    param_count, params.size(), query.c_str());
		}
		
		odbc_params.reserve(params.size());
		for (Value &dp : params) {
			ODBCParameter mp(query, std::move(dp));
			odbc_params.emplace_back(std::move(mp));
			ODBCParameter &mp_ref = odbc_params.back();
			SQLRETURN bind_ret = mp_ref.BindParameter(stmt, odbc_params.size());
			if (!SQL_SUCCEEDED(bind_ret)) {
				std::cout << "[DEBUG] ODBCExecute - BindParameter failed for parameter " << odbc_params.size() << std::endl;
				throw IOException("Failed to bind parameter %zu for ODBC query \"%s\"\n",
				                  odbc_params.size(), query.c_str());
			}
		}
		std::cout << "[DEBUG] ODBCExecute - All parameters bound successfully" << std::endl;
	}

	std::cout << "[DEBUG] ODBCExecute - Executing prepared statement" << std::endl;
	retcode = SQLExecute(stmt);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "[DEBUG] ODBCExecute - SQLExecute failed" << std::endl;
		throw IOException("Failed to execute ODBC query \"%s\"\n", query.c_str());
	}

	SQLLEN affected_rows = 0;
	retcode = SQLRowCount(stmt, &affected_rows);
	if (!SQL_SUCCEEDED(retcode)) {
		// For SELECT queries, SQLRowCount should return -1
		affected_rows = -1;
		std::cout << "[DEBUG] ODBCExecute - SQLRowCount failed, assuming SELECT query (affected_rows = -1)" << std::endl;
	} else {
		std::cout << "[DEBUG] ODBCExecute - Affected rows: " << affected_rows << std::endl;
	}

	// For ODBC, we need to be more careful:
	// - SELECT queries should return -1 (no affected rows)
	// - DML queries (INSERT, UPDATE, DELETE) return >= 0
	// But some ODBC drivers might return 0 for SELECT queries, so we need to check column count
	SQLSMALLINT num_cols = 0;
	SQLRETURN col_ret = SQLNumResultCols(stmt, &num_cols);
	if (SQL_SUCCEEDED(col_ret) && num_cols > 0) {
		// This is a SELECT query, force affected_rows to -1
		affected_rows = -1;
		std::cout << "[DEBUG] ODBCExecute - Detected SELECT query (has " << num_cols << " columns), setting affected_rows = -1" << std::endl;
	}

	// Return -1 for SELECT queries (no affected rows), otherwise return the actual count
	return affected_rows >= 0 ? static_cast<idx_t>(affected_rows) : static_cast<idx_t>(-1);
}

unique_ptr<ODBCResult> ODBCConnection::QueryInternal(const string &query, vector<Value> params,
                                                       ODBCResultStreaming streaming,
                                                       ODBCConnectorInterface con_interface) {
	std::cout << "[DEBUG] QueryInternal - Starting query: " << query << std::endl;
	
	auto hdbc = GetConn();
	bool result_streaming = streaming == ODBCResultStreaming::ALLOW_STREAMING;
	bool basic_interface = con_interface == ODBCConnectorInterface::BASIC;

	if (basic_interface) {
		std::cout << "[DEBUG] QueryInternal - Using basic interface" << std::endl;
		ODBCExecute(nullptr, query, params, result_streaming);
		return unique_ptr<ODBCResult>(nullptr);
	}

	std::cout << "[DEBUG] QueryInternal - Using prepared statement interface" << std::endl;
	SQLHSTMT hstmt;
	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "[DEBUG] QueryInternal - Failed to allocate statement handle" << std::endl;
		throw IOException("Failed to initialize ODBC query \"%s\"\n", query.c_str());
	}
	
	idx_t affected_rows = ODBCExecute(hstmt, query, params, result_streaming);
	
	// Get number of result columns
	SQLSMALLINT num_cols = 0;
	retcode = SQLNumResultCols(hstmt, &num_cols);
	if (!SQL_SUCCEEDED(retcode)) {
		num_cols = 0;
		std::cout << "[DEBUG] QueryInternal - SQLNumResultCols failed, num_cols = 0" << std::endl;
	} else {
		std::cout << "[DEBUG] QueryInternal - Number of result columns: " << num_cols << std::endl;
	}
	
	// For SELECT queries, affected_rows will be -1, but we still need to process the result set
	if (affected_rows == static_cast<idx_t>(-1) && num_cols == 0) {
		// Try to get column count again - some drivers might not set it immediately
		retcode = SQLNumResultCols(hstmt, &num_cols);
		if (SQL_SUCCEEDED(retcode)) {
			std::cout << "[DEBUG] QueryInternal - Retry SQLNumResultCols succeeded, num_cols = " << num_cols << std::endl;
		}
	}
	
	std::cout << "[DEBUG] QueryInternal - Creating ODBCResult with affected_rows=" << affected_rows
	          << ", num_cols=" << num_cols << std::endl;
	return make_uniq<ODBCResult>(query, ODBCStatementPtr(hstmt, ODBCStatementDelete), type_config, affected_rows, num_cols);
}

unique_ptr<ODBCResult> ODBCConnection::Query(const string &query, ODBCResultStreaming streaming) {
	return Query(query, vector<Value>(), streaming);
}

unique_ptr<ODBCResult> ODBCConnection::Query(const string &query, vector<Value> params,
                                               ODBCResultStreaming streaming) {
	return QueryInternal(query, params, streaming, ODBCConnectorInterface::PREPARED_STATEMENT);
}

void ODBCConnection::Execute(const string &query) {
	Execute(query, vector<Value>());
}

void ODBCConnection::Execute(const string &query, vector<Value> params) {
	ODBCConnectorInterface con_interface =
	    params.size() > 0 ? ODBCConnectorInterface::PREPARED_STATEMENT : ODBCConnectorInterface::BASIC;
	QueryInternal(query, std::move(params), ODBCResultStreaming::FORCE_MATERIALIZATION, con_interface);
}

bool ODBCConnection::IsOpen() {
	return connection.get();
}

void ODBCConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

vector<IndexInfo> ODBCConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void ODBCConnection::DebugSetPrintQueries(bool print) {
	debug_odbc_print_queries = print;
}

bool ODBCConnection::DebugPrintQueries() {
	return debug_odbc_print_queries;
}

} // namespace duckdb
