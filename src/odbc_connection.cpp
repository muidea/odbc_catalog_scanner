#include "odbc_connection.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "odbc_parameter.hpp"
#include "odbc_types.hpp"
#include <thread>
#include <memory>

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

shared_ptr<ODBCConnection> ODBCConnection::Open(ODBCTypeConfig type_config, const string &connection_string) {
	auto owned_connection = make_shared_ptr<OwnedODBCConnection>(ODBCUtils::Connect(connection_string));
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
		SQLHSTMT hstmt = SQL_NULL_HSTMT;
		retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
		if (!SQL_SUCCEEDED(retcode)) {
			throw IOException("Failed to allocate statement handle for ODBC query \"%s\"\n", query.c_str());
		}

		try {
			retcode = SQLExecDirect(hstmt, (SQLCHAR*)query.c_str(), SQL_NTS);
			if (!SQL_SUCCEEDED(retcode)) {
				throw IOException("Failed to run query \"%s\"\n", query.c_str());
			}
		} catch (const std::exception &e) {
			// 确保语句句柄被清理
			if (hstmt != SQL_NULL_HSTMT) {
				SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
			}
			throw;
		}
		
		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		return 0;
	}

	// prepared statement interface
	// 对于预编译语句接口，调用者负责清理语句句柄
	// 我们只需要确保在失败时抛出适当的异常

	retcode = SQLPrepare(stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
	if (!SQL_SUCCEEDED(retcode)) {
		// 获取详细的ODBC错误信息
		SQLCHAR sqlstate[6] = {0};
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
		SQLINTEGER native_error = 0;
		SQLSMALLINT message_length = 0;
		
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, &native_error,
		              message, SQL_MAX_MESSAGE_LENGTH, &message_length);
		
		throw IOException("Failed to prepare ODBC query \"%s\": SQLState=%s, NativeError=%d, Message=%s\n",
		                  query.c_str(), reinterpret_cast<const char*>(sqlstate), native_error, reinterpret_cast<const char*>(message));
	}

	vector<ODBCParameter> odbc_params;
	if (params.size() > 0) {
		SQLSMALLINT param_count;
		retcode = SQLNumParams(stmt, &param_count);
		if (!SQL_SUCCEEDED(retcode)) {
			// 获取详细的ODBC错误信息
			SQLCHAR sqlstate[6] = {0};
			SQLCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
			SQLINTEGER native_error = 0;
			SQLSMALLINT message_length = 0;
			
			SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, &native_error,
			              message, SQL_MAX_MESSAGE_LENGTH, &message_length);
			
			throw IOException("Failed to get parameter count for ODBC query \"%s\": SQLState=%s, NativeError=%d, Message=%s\n",
			                  query.c_str(), reinterpret_cast<const char*>(sqlstate), native_error, reinterpret_cast<const char*>(message));
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
				throw IOException("Failed to bind parameter %zu for ODBC query \"%s\"\n",
				                  odbc_params.size(), query.c_str());
			}
		}
	}

	retcode = SQLExecute(stmt);
	if (!SQL_SUCCEEDED(retcode)) {
		// 获取详细的ODBC错误信息
		SQLCHAR sqlstate[6] = {0};
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
		SQLINTEGER native_error = 0;
		SQLSMALLINT message_length = 0;
		
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, &native_error,
		              message, SQL_MAX_MESSAGE_LENGTH, &message_length);
		
		throw IOException("Failed to execute ODBC query \"%s\": SQLState=%s, NativeError=%d, Message=%s\n",
		                  query.c_str(), reinterpret_cast<const char*>(sqlstate), native_error, reinterpret_cast<const char*>(message));
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
                                                        ODBCConnectorInterface con_interface) {
	
	// 检查连接是否繁忙
	bool expected = false;
	if (!connection_busy.compare_exchange_strong(expected, true)) {
		throw IOException("ODBC connection is busy executing another query \"%s\"\n", query.c_str());
	}
	
	// 检查连接健康状态
	if (!CheckConnectionHealth()) {
		ReleaseConnection();
		throw IOException("ODBC connection is not healthy for query \"%s\"\n", query.c_str());
	}
	
	// 检查是否有挂起的结果集
	if (HasPendingResults()) {
		ResetConnectionState();
		
		// 再次检查
		if (HasPendingResults()) {
			ReleaseConnection();
			throw IOException("ODBC connection has pending results that cannot be cleared for query \"%s\"\n", query.c_str());
		}
	}
	
	// 注意：我们不使用ConnectionBusyGuard，因为连接需要保持繁忙直到ODBCResult被销毁
	// 连接繁忙状态由ODBCResult在析构时释放
	
	// 使用递归互斥锁保护连接操作，防止多个查询同时使用同一个连接
	// 使用 recursive_mutex 允许同一线程重入，避免死锁
	lock_guard<std::recursive_mutex> l(query_lock);
	
	auto hdbc = GetConn();
	
	// 检查连接是否有效
	if (!hdbc) {
		ReleaseConnection();
		throw IOException("ODBC connection is not open for query \"%s\"\n", query.c_str());
	}
	
	// 检查连接状态
	SQLINTEGER conn_attr = SQL_CD_FALSE;
	SQLRETURN status_ret = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &conn_attr, 0, nullptr);
	if (SQL_SUCCEEDED(status_ret) && conn_attr == SQL_CD_TRUE) {
		ReleaseConnection();
		throw IOException("ODBC connection is dead for query \"%s\"\n", query.c_str());
	}
	
	bool result_streaming = streaming == ODBCResultStreaming::ALLOW_STREAMING;
	bool basic_interface = con_interface == ODBCConnectorInterface::BASIC;

	if (basic_interface) {
		ODBCExecute(nullptr, query, params, result_streaming);
		// BASIC接口不创建ODBCResult，需要立即释放连接忙状态
		ReleaseConnection();
		return unique_ptr<ODBCResult>(nullptr);
	}
	
	SQLHSTMT hstmt = SQL_NULL_HSTMT;
	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if (!SQL_SUCCEEDED(retcode)) {
		// 释放连接忙状态
		ReleaseConnection();
		throw IOException("Failed to initialize ODBC query \"%s\"\n", query.c_str());
	}
	
	idx_t affected_rows = 0;
	try {
		affected_rows = ODBCExecute(hstmt, query, params, result_streaming);
	} catch (const std::exception &e) {
		// 执行失败，清理语句句柄
		if (hstmt != SQL_NULL_HSTMT) {
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		}
		// 释放连接忙状态
		ReleaseConnection();
		throw;
	}
	
	// Get number of result columns
	SQLSMALLINT num_cols = 0;
	retcode = SQLNumResultCols(hstmt, &num_cols);
	if (!SQL_SUCCEEDED(retcode)) {
		num_cols = 0;
	}
	
	// For SELECT queries, affected_rows will be -1, but we still need to process the result set
	if (affected_rows == static_cast<idx_t>(-1) && num_cols == 0) {
		// Try to get column count again - some drivers might not set it immediately
		retcode = SQLNumResultCols(hstmt, &num_cols);
	}
	
	try {
		auto result = make_uniq<ODBCResult>(query, ODBCStatementPtr(hstmt, ODBCStatementDelete), type_config,
		                                    affected_rows, num_cols, shared_from_this(), streaming);
		return result;
	} catch (const std::exception &e) {
		// ODBCResult构造函数失败，清理语句句柄
		if (hstmt != SQL_NULL_HSTMT) {
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		}
		// 释放连接忙状态
		ReleaseConnection();
		throw IOException("Failed to create ODBC result for query \"%s\": %s\n", query.c_str(), e.what());
	}
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

bool ODBCConnection::CheckConnectionHealth() {
	if (!IsOpen()) {
		return false;
	}
	
	auto hdbc = GetConn();
	SQLINTEGER conn_attr = SQL_CD_FALSE;
	SQLRETURN retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &conn_attr, 0, nullptr);
	
	if (!SQL_SUCCEEDED(retcode)) {
		return false;
	}
	
	return (conn_attr == SQL_CD_FALSE);
}

void ODBCConnection::ResetConnectionState() {
	if (!IsOpen()) {
		return;
	}
	
	auto hdbc = GetConn();
	
	// 尝试回滚任何未提交的事务
	SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
	
	// 对于FreeTDS驱动，可能需要更激进的清理
	// 尝试多次分配和释放语句句柄，清理可能残留的状态
	for (int i = 0; i < 3; i++) {
		SQLHSTMT hstmt = SQL_NULL_HSTMT;
		SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
		
		if (SQL_SUCCEEDED(retcode)) {
			// 尝试取消任何挂起的操作
			SQLCancel(hstmt);
			
			// 尝试关闭游标
			SQLCloseCursor(hstmt);
			
			// 尝试使用SQLFreeStmt with SQL_CLOSE
			SQLFreeStmt(hstmt, SQL_CLOSE);
			
			// 尝试使用SQLFreeStmt with SQL_DROP
			SQLFreeStmt(hstmt, SQL_DROP);
			
			// 如果SQL_DROP失败，使用SQLFreeHandle
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
			
			// 短暂延迟，让驱动处理清理
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	
	// 对于某些驱动，可能需要重置连接属性
	// 尝试设置连接属性为默认值
	SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
	
	// 尝试设置连接超时
	SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)5, 0);
	
	// 尝试设置登录超时
	SQLSetConnectAttr(hdbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
	
	// 尝试重置连接状态
	SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, (SQLPOINTER)SQL_CD_FALSE, 0);
	
	// 尝试设置访问模式
	SQLSetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_WRITE, 0);
}

bool ODBCConnection::HasPendingResults() {
	if (!IsOpen()) {
		return false;
	}
	
	// 检查连接是否有挂起的结果集
	// 这是一个启发式检查，不是100%准确
	auto hdbc = GetConn();
	SQLHSTMT hstmt = SQL_NULL_HSTMT;
	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	
	if (!SQL_SUCCEEDED(retcode)) {
		// 无法分配语句句柄，可能连接有问题
		return false;
	}
	
	// 首先检查是否有活动的结果集
	SQLSMALLINT num_cols = 0;
	retcode = SQLNumResultCols(hstmt, &num_cols);
	
	if (SQL_SUCCEEDED(retcode) && num_cols > 0) {
		// 有活动的结果集
		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		return true;
	}
	
	// 对于FreeTDS驱动，使用更简单的方法
	// 尝试获取行计数
	SQLLEN row_count = 0;
	retcode = SQLRowCount(hstmt, &row_count);
	
	// 如果row_count是-1（SELECT查询）但我们已经检查过没有列，可能有问题
	if (SQL_SUCCEEDED(retcode) && row_count == -1 && num_cols == 0) {
		// 这可能表示有挂起的结果集
		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		return true;
	}
	
	// 最后尝试执行一个简单的查询作为后备检查
	SQLCHAR test_query[] = "SELECT 1";
	retcode = SQLExecDirect(hstmt, test_query, SQL_NTS);
	
	// 如果查询失败，可能是有挂起的结果集或连接有问题
	// 但为了兼容性，我们只在特定错误码时返回true
	if (!SQL_SUCCEEDED(retcode)) {
		// 检查是否是"连接繁忙"或"语句句柄已在使用中"的错误
		SQLCHAR sqlstate[6] = {0};
		SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate, nullptr, nullptr, 0, nullptr);
		
		// 常见的"连接繁忙"或"语句在使用中"的错误码
		// HY000: 一般错误
		// 24000: 无效的游标状态
		// HY010: 函数序列错误
		// 08S01: 通信链接失败
		// 空字符串也可能表示有挂起的结果
		const char* sqlstate_str = reinterpret_cast<const char*>(sqlstate);
		if (strcmp(sqlstate_str, "HY000") == 0 || strcmp(sqlstate_str, "24000") == 0 ||
		    strcmp(sqlstate_str, "HY010") == 0 || strcmp(sqlstate_str, "08S01") == 0 ||
		    sqlstate_str[0] == '\0') {  // 空字符串也可能表示有问题
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
			return true;
		}
	}
	
	SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
	return false;
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

void ODBCConnection::ReleaseConnection() {
	// 使用原子操作确保幂等性和线程安全
	bool expected = true;
	connection_busy.compare_exchange_strong(expected, false);
}

} // namespace duckdb
