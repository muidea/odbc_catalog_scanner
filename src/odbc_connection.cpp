#include "odbc_connection.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "odbc_parameter.hpp"
#include "odbc_types.hpp"

namespace duckdb {

static bool debug_odbc_print_queries = false;

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

	lock_guard<mutex> l(query_lock);
	SQLRETURN ret;

	if (!stmt) { // basic interface - direct execution
		auto con = GetConn();
		SQLHSTMT hstmt;
		ret = SQLAllocHandle(SQL_HANDLE_STMT, con, &hstmt);
		if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
			throw IOException("Failed to allocate statement handle for query \"%s\"", query.c_str());
		}

		ret = SQLExecDirect(hstmt, (SQLCHAR*)query.c_str(), SQL_NTS);
		if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
			SQLCHAR sqlstate[6];
			SQLINTEGER native_error;
			SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
			SQLSMALLINT text_length;
			
			SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate, &native_error,
				message_text, SQL_MAX_MESSAGE_LENGTH, &text_length);
			
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
			throw IOException("Failed to run query \"%s\": %s", query.c_str(), (char*)message_text);
		}

		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		return 0;
	}

	// prepared statement interface
	ret = SQLPrepare(stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLCHAR sqlstate[6];
		SQLINTEGER native_error;
		SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
		SQLSMALLINT text_length;
		
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, &native_error,
			message_text, SQL_MAX_MESSAGE_LENGTH, &text_length);
		
		throw IOException("Failed to prepare ODBC query \"%s\": %s", query.c_str(), (char*)message_text);
	}

	// TODO: Implement parameter binding for ODBC
	// This is a complex task that requires mapping DuckDB Value types to ODBC SQL types
	// For now, we'll skip parameter binding and assume queries don't have parameters
	if (params.size() > 0) {
		throw IOException("Parameter binding not yet implemented for ODBC queries");
	}

	ret = SQLExecute(stmt);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLCHAR sqlstate[6];
		SQLINTEGER native_error;
		SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
		SQLSMALLINT text_length;
		
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, &native_error,
			message_text, SQL_MAX_MESSAGE_LENGTH, &text_length);
		
		throw IOException("Failed to execute ODBC query \"%s\": %s", query.c_str(), (char*)message_text);
	}

	SQLLEN affected_rows = 0;
	ret = SQLRowCount(stmt, &affected_rows);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		affected_rows = 0;
	}

	if (!streaming && affected_rows == 0) {
		// For SELECT queries, we need to fetch the result set
		// This is handled by the ODBCResult class
	}

	return affected_rows >= 0 ? static_cast<idx_t>(affected_rows) : 0;
}

unique_ptr<ODBCResult> ODBCConnection::QueryInternal(const string &query, vector<Value> params,
                                                       ODBCResultStreaming streaming,
                                                       ODBCConnectorInterface con_interface) {
	auto con = GetConn();
	bool result_streaming = streaming == ODBCResultStreaming::ALLOW_STREAMING;
	bool basic_interface = con_interface == ODBCConnectorInterface::BASIC;

	if (basic_interface) {
		ODBCExecute(nullptr, query, params, result_streaming);
		return unique_ptr<ODBCResult>(nullptr);
	}

	SQLHSTMT stmt;
	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		throw IOException("Failed to allocate statement handle for query \"%s\"", query.c_str());
	}

	auto stmt_ptr = ODBCStatementPtr(stmt, ODBCStatementDelete);
	idx_t affected_rows = ODBCExecute(stmt_ptr.get(), query, params, result_streaming);
	return make_uniq<ODBCResult>(query, std::move(stmt_ptr), type_config, affected_rows);
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
