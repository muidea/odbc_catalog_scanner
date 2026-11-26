#include "query_odbc_source_error.hpp"
#include "odbc_connection.hpp"
#include "odbc_handle_guard.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "odbc_parameter.hpp"
#include "odbc_types.hpp"
#include "encoding_utils.hpp"
namespace duckdb {

static bool debug_odbc_print_queries = false;

static SQLRETURN SQLExecDirectWithFallback(SQLHSTMT stmt, const string &query) {
#ifdef _WIN32
	SQLRETURN retcode = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
	if (SQL_SUCCEEDED(retcode)) {
		return retcode;
	}
	string query_gbk = UTF8ToGBK(query);
	if (query_gbk != query) {
		SQLFreeStmt(stmt, SQL_CLOSE);
		retcode = SQLExecDirect(stmt, (SQLCHAR *)query_gbk.c_str(), SQL_NTS);
	}
	return retcode;
#else
	return SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
#endif
}

static SQLRETURN SQLPrepareWithFallback(SQLHSTMT stmt, const string &query) {
#ifdef _WIN32
	SQLRETURN retcode = SQLPrepare(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
	if (SQL_SUCCEEDED(retcode)) {
		return retcode;
	}
	string query_gbk = UTF8ToGBK(query);
	if (query_gbk != query) {
		SQLFreeStmt(stmt, SQL_CLOSE);
		retcode = SQLPrepare(stmt, (SQLCHAR *)query_gbk.c_str(), SQL_NTS);
	}
	return retcode;
#else
	return SQLPrepare(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
#endif
}

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

shared_ptr<ODBCConnection> ODBCConnection::Open(ODBCTypeConfig type_config, const string &connection_string,
                                                uint32_t login_timeout) {
	ODBCConnectGuard handles;
	handles.connection = ODBCUtils::Connect(connection_string, handles.environment, login_timeout);
	auto owned_connection = make_shared_ptr<OwnedODBCConnection>();
	owned_connection->connection = handles.Release(owned_connection->environment);
	return make_shared_ptr<ODBCConnection>(std::move(owned_connection), connection_string, std::move(type_config));
}

idx_t ODBCConnection::ODBCExecute(SQLHSTMT stmt, const string &query, vector<Value> params, bool streaming) {
	if (ODBCConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	lock_guard<std::recursive_mutex> l(query_lock);

	SQLRETURN retcode;

	if (!stmt) { // basic interface
		auto hdbc = GetConn();
		ODBCStatementPtr statement(nullptr, ODBCStatementDelete);
		SQLHSTMT hstmt = SQL_NULL_HSTMT;
		retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
		statement.reset(hstmt);
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_DBC, hdbc, GetConn(), "source_prepare", query);
		}

		retcode = SQLExecDirectWithFallback(statement.get(), query);
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_STMT, statement.get(), GetConn(), "source_execute", query);
		}

		return 0;
	}

	// prepared statement interface
	// 对于预编译语句接口，调用者负责清理语句句柄
	// 我们只需要确保在失败时抛出适当的异常

	if (params.empty()) {
		retcode = SQLExecDirectWithFallback(stmt, query);
	} else {
		retcode = SQLPrepareWithFallback(stmt, query);
	}
	if (!SQL_SUCCEEDED(retcode)) {
		ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), params.empty() ? "source_execute" : "source_prepare",
		                    query);
	}

	vector<ODBCParameter> odbc_params;
	if (params.empty()) {
		SQLLEN affected_rows = 0;
		retcode = SQLRowCount(stmt, &affected_rows);
		if (!SQL_SUCCEEDED(retcode)) {
			affected_rows = -1;
		}
		SQLSMALLINT num_cols = 0;
		SQLRETURN col_ret = SQLNumResultCols(stmt, &num_cols);
		if (SQL_SUCCEEDED(col_ret) && num_cols > 0) {
			affected_rows = -1;
		}
		return affected_rows >= 0 ? static_cast<idx_t>(affected_rows) : static_cast<idx_t>(-1);
	}
	if (params.size() > 0) {
		SQLSMALLINT param_count;
		retcode = SQLNumParams(stmt, &param_count);
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_describe", query);
		}

		if (param_count != params.size()) {
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
				ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_bind", query);
			}
		}
	}

	retcode = SQLExecute(stmt);
	if (!SQL_SUCCEEDED(retcode)) {
		ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_execute", query);
	}

	SQLLEN affected_rows = 0;
	retcode = SQLRowCount(stmt, &affected_rows);
	if (!SQL_SUCCEEDED(retcode)) {
		// For SELECT queries, SQLRowCount should return -1
		affected_rows = -1;
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
	}

	// Return -1 for SELECT queries (no affected rows), otherwise return the actual count
	return affected_rows >= 0 ? static_cast<idx_t>(affected_rows) : static_cast<idx_t>(-1);
}

unique_ptr<ODBCResult> ODBCConnection::QueryInternal(const string &query, vector<Value> params,
                                                     ODBCResultStreaming streaming,
                                                     ODBCConnectorInterface con_interface,
                                                     vector<LogicalType> expected_types) {

	// 检查连接是否繁忙
	bool expected = false;
	if (!connection_busy.compare_exchange_strong(expected, true)) {
		throw IOException("ODBC connection is busy executing another query \"%s\"\n", query.c_str());
	}

	// Keep all operations after claiming busy inside the catch-all, including
	// health checks, locking, shared_from_this and result/owner construction.
	try {
		if (!CheckConnectionHealth()) {
			throw IOException("ODBC connection is not healthy for query \"%s\"\n", query.c_str());
		}

		lock_guard<std::recursive_mutex> l(query_lock);
		auto hdbc = GetConn();
		if (!hdbc) {
			throw IOException("ODBC connection is not open for query \"%s\"\n", query.c_str());
		}

		SQLINTEGER conn_attr = SQL_CD_FALSE;
		SQLRETURN status_ret = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &conn_attr, 0, nullptr);
		if (SQL_SUCCEEDED(status_ret) && conn_attr == SQL_CD_TRUE) {
			throw IOException("ODBC connection is dead for query \"%s\"\n", query.c_str());
		}

		bool result_streaming = streaming == ODBCResultStreaming::ALLOW_STREAMING;
		if (con_interface == ODBCConnectorInterface::BASIC) {
			ODBCExecute(nullptr, query, params, result_streaming);
			ReleaseConnection();
			return unique_ptr<ODBCResult>(nullptr);
		}

		// Construct the owner before allocating a handle. From allocation until
		// transfer to ODBCResult there is exactly one statement owner.
		ODBCStatementPtr statement(nullptr, ODBCStatementDelete);
		SQLHSTMT hstmt = SQL_NULL_HSTMT;
		SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
		statement.reset(hstmt);
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_DBC, hdbc, GetConn(), "source_prepare", query);
		}

		idx_t affected_rows = ODBCExecute(statement.get(), query, params, result_streaming);
		SQLSMALLINT num_cols = 0;
		retcode = SQLNumResultCols(statement.get(), &num_cols);
		if (!SQL_SUCCEEDED(retcode)) {
			num_cols = 0;
		}
		if (affected_rows == static_cast<idx_t>(-1) && num_cols == 0) {
			retcode = SQLNumResultCols(statement.get(), &num_cols);
		}

		auto self = shared_from_this();
		return make_uniq<ODBCResult>(query, std::move(statement), type_config, affected_rows, num_cols, std::move(self),
		                             streaming, std::move(expected_types));
	} catch (...) {
		// Failed construction leaves busy cleanup here, not in ODBCResult, so
		// another query cannot acquire busy between two releases.
		ReleaseConnection();
		throw;
	}
}

unique_ptr<ODBCResult> ODBCConnection::Query(const string &query, ODBCResultStreaming streaming,
                                             vector<LogicalType> expected_types) {
	return Query(query, vector<Value>(), streaming, std::move(expected_types));
}

unique_ptr<ODBCResult> ODBCConnection::Query(const string &query, vector<Value> params, ODBCResultStreaming streaming,
                                             vector<LogicalType> expected_types) {
	return QueryInternal(query, params, streaming, ODBCConnectorInterface::PREPARED_STATEMENT,
	                     std::move(expected_types));
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

bool ODBCConnection::CheckConnectionHealth() {
	if (!IsOpen()) {
		return false;
	}

	auto hdbc = GetConn();
	SQLINTEGER conn_attr = SQL_CD_FALSE;
	SQLRETURN retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &conn_attr, 0, nullptr);

	if (!SQL_SUCCEEDED(retcode)) {
		// SQL_ATTR_CONNECTION_DEAD is optional in some ODBC drivers.
		return true;
	}

	return (conn_attr == SQL_CD_FALSE);
}

void ODBCConnection::DebugSetPrintQueries(bool print) {
	debug_odbc_print_queries = print;
}

bool ODBCConnection::DebugPrintQueries() {
	return debug_odbc_print_queries;
}

void ODBCConnection::ReleaseConnection() {
	// 使用原子操作确保幂等性和线程安全
	bool expected = true;
	connection_busy.compare_exchange_strong(expected, false);
}

} // namespace duckdb
