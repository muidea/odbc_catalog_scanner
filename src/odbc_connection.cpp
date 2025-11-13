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

idx_t ODBCConnection::ODBCExecute(MYSQL_STMT *stmt, const string &query, vector<Value> params, bool streaming) {
	if (ODBCConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	lock_guard<mutex> l(query_lock);

	if (!stmt) { // basic interface
		auto con = GetConn();
		int res_query = odbc_real_query(con, query.c_str(), query.size());
		if (res_query != 0) {
			throw IOException("Failed to run query \"%s\": %s\n", query.c_str(), odbc_error(con));
		}
		auto result = ODBCResultPtr(odbc_store_result(con), ODBCResultDelete);
		return 0;
	}

	// prepared statement interface

	int res_prepare = odbc_stmt_prepare(stmt, query.c_str(), query.size());
	if (res_prepare != 0) {
		throw IOException("Failed to prepare ODBC query \"%s\": %s\n", query.c_str(), odbc_stmt_error(stmt));
	}

	vector<ODBCParameter> odbc_params;
	vector<MYSQL_BIND> binds;
	if (params.size() > 0) {
		size_t expected_count = odbc_stmt_param_count(stmt);
		if (expected_count != params.size()) {
			throw IOException(
			    "Incorrect query parameters count specified, expected: %zu, actual: %zu, ODBC query \"%s\": %s\n",
			    expected_count, params.size(), query.c_str(), odbc_stmt_error(stmt));
		}
		odbc_params.reserve(params.size());
		binds.reserve(params.size());
		for (Value &dp : params) {
			ODBCParameter mp(query, std::move(dp));
			odbc_params.emplace_back(std::move(mp));
			ODBCParameter &mp_ref = odbc_params.back();
			binds.push_back(mp_ref.CreateBind());
		}
		auto res_bind = odbc_stmt_bind_param(stmt, binds.data());
		if (res_bind != 0) {
			throw IOException("Failed to bind parameters, count: %zu, ODBC query \"%s\": %s\n", binds.size(),
			                  query.c_str(), odbc_stmt_error(stmt));
		}
	}

	int res_exec = odbc_stmt_execute(stmt);
	if (res_exec != 0) {
		throw IOException("Failed to execute ODBC query \"%s\": %s\n", query.c_str(), odbc_stmt_error(stmt));
	}

	idx_t affected_rows = odbc_stmt_affected_rows(stmt);

	if (!streaming && affected_rows == static_cast<idx_t>(-1)) {
		bool btrue = true;
		auto res_attr = odbc_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &btrue);
		if (res_attr != 0) {
			throw IOException("Failed to set STMT_ATTR_UPDATE_MAX_LENGTH for ODBC query \"%s\": %s\n", query.c_str(),
			                  odbc_stmt_error(stmt));
		}

		int res_store = odbc_stmt_store_result(stmt);
		if (res_store != 0) {
			throw IOException("Failed to store result for ODBC query \"%s\": %s\n", query.c_str(),
			                  odbc_stmt_error(stmt));
		}
	}

	return affected_rows;
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

	auto stmt = ODBCStatementPtr(odbc_stmt_init(con), ODBCStatementDelete);
	if (!stmt) {
		throw IOException("Failed to initialize ODBC query \"%s\": %s\n", query.c_str(), odbc_error(con));
	}
	idx_t affected_rows = ODBCExecute(stmt.get(), query, params, result_streaming);
	return make_uniq<ODBCResult>(query, std::move(stmt), type_config, affected_rows);
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
