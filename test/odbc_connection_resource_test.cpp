#include "odbc_utils.hpp"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#ifndef _WIN32
#include <unistd.h>
#endif

using namespace duckdb;

namespace {
int environments = 0;
int connections = 0;
bool fail_attribute = false;
bool fail_login_timeout = false;
bool login_timeout_seen = false;
bool fail_driver = false;
bool throw_connect = false;
string temporary_path;
void Require(bool value, const char *message) {
	if (!value)
		throw std::runtime_error(message);
}
void Empty() {
	Require(environments == 0 && connections == 0, "native connection handles leaked");
}
void Fail(const string &dsn, uint32_t timeout = 0) {
	bool threw = false;
	SQLHENV env = SQL_NULL_HENV;
	try {
		ODBCUtils::Connect(dsn, env, timeout);
	} catch (const std::exception &) {
		threw = true;
	}
	Require(threw, "expected Connect failure");
	Require(env == SQL_NULL_HENV, "failed Connect transferred environment");
	Empty();
}
} // namespace

extern "C" {
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE, SQLHANDLE *output) {
	*output = new int(type);
	if (type == SQL_HANDLE_ENV)
		environments++;
	else if (type == SQL_HANDLE_DBC)
		connections++;
	else
		throw std::runtime_error("unexpected handle type");
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
	Require(handle && *static_cast<int *>(handle) == type, "double free or wrong handle type");
	if (type == SQL_HANDLE_ENV) {
		Require(connections == 0, "environment freed before connection");
		environments--;
	} else
		connections--;
	delete static_cast<int *>(handle);
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLDisconnect(SQLHDBC) {
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER) {
	return SQL_SUCCESS;
}
SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER) {
	if (attribute == SQL_LOGIN_TIMEOUT) {
		Require(reinterpret_cast<uintptr_t>(value) == 5, "guarded login budget lost");
		login_timeout_seen = true;
		return fail_login_timeout ? SQL_ERROR : SQL_SUCCESS;
	}
	return fail_attribute ? SQL_ERROR : SQL_SUCCESS;
}
SQLRETURN SQL_API SQLDriverConnect(SQLHDBC, SQLHWND, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *,
                                   SQLUSMALLINT) {
	return fail_driver ? SQL_ERROR : SQL_SUCCESS;
}
SQLRETURN SQL_API SQLConnect(SQLHDBC, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT) {
	auto path = std::getenv("ODBCINI");
	Require(path != nullptr, "temporary ODBCINI not set");
	temporary_path = path;
#ifndef _WIN32
	Require(access(path, F_OK) == 0, "temporary ini not present during SQLConnect");
#endif
	if (throw_connect)
		throw std::runtime_error("injected SQLConnect exception");
	return fail_driver ? SQL_ERROR : SQL_SUCCESS;
}
SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *state, SQLINTEGER *native,
                                SQLCHAR *message, SQLSMALLINT capacity, SQLSMALLINT *length) {
	std::memcpy(state, "HY000", 6);
	*native = 0;
	*length = 0;
	if (message && capacity)
		message[0] = 0;
	return SQL_SUCCESS;
}
}

int main() {
	try {
		Fail("PORT=70000;");
		Fail("broken");
		fail_attribute = true;
		Fail("DRIVER=test;SERVER=local;");
		fail_attribute = false;
		fail_driver = true;
		Fail("DRIVER=test;SERVER=local;");
		fail_driver = false;
#ifndef _WIN32
		// 只使用本测试创建的环境值，不读取真实连接配置。
		setenv("ODBCINI", "/tmp/odbc-closeout-test-original", 1);
		throw_connect = true;
		Fail("DRIVER=test;SERVER=local;UID=test;PWD={x}}};");
		Require(string(std::getenv("ODBCINI")) == "/tmp/odbc-closeout-test-original", "ODBCINI not restored");
		Require(!temporary_path.empty() && access(temporary_path.c_str(), F_OK) != 0, "temporary ini leaked");
		throw_connect = false;
		Fail("DRIVER={test\ninvalid};SERVER=local;UID=test;PWD={x}}};");
		unsetenv("ODBCINI");
#endif
		fail_login_timeout = true;
		Fail("DRIVER=test;SERVER=local;", 5);
		fail_login_timeout = false;
		Require(login_timeout_seen, "guarded connect omitted login timeout");
		SQLHENV env = SQL_NULL_HENV;
		auto connection = ODBCUtils::Connect("DRIVER=test;SERVER=local;", env, 5);
		Require(environments == 1 && connections == 1, "success transfer invalid");
		SQLDisconnect(connection);
		SQLFreeHandle(SQL_HANDLE_DBC, connection);
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		Empty();
		std::cout << "PASS: Connect parse/options/driver/temporary-DSN failures and successful transfer\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "FAIL: " << error.what() << '\n';
		return 1;
	}
}
