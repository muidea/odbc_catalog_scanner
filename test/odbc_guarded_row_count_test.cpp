#include "odbc_guarded_row_count.hpp"

#include <iostream>
#include <limits>
#include <stdexcept>

using namespace duckdb;

static void Require(bool ok) {
	if (!ok) {
		throw std::runtime_error("row-count ABI or unknown-count proof failed");
	}
}

static void Narrow(int32_t x, int32_t y, bool valid) {
	SQLLEN first, second;
	std::memset(&first, 0xa5, sizeof(first));
	std::memset(&second, 0x5a, sizeof(second));
	std::memcpy(&first, &x, sizeof(x));
	std::memcpy(&second, &y, sizeof(y));
	int64_t result = -99;
	Require(DecodeGuardedODBCRowCount(first, second, result) == valid);
	if (valid) {
		Require(result == x);
	}
}

int main() {
	try {
		for (int32_t value : {0, 1, std::numeric_limits<int32_t>::max()}) {
			Narrow(value, value, true);
		}
		Narrow(-1, -1, false);
		Narrow(-4, -4, false);
		Narrow(1, 2, false);
		for (SQLLEN value : {SQLLEN(0), SQLLEN(1), std::numeric_limits<SQLLEN>::max()}) {
			int64_t result = -99;
			Require(DecodeGuardedODBCRowCount(value, value, result) && result == value);
		}
		int64_t result = -99;
		Require(!DecodeGuardedODBCRowCount(-1, -1, result));
		SQLLEN first, second;
		std::memset(&first, 0xa5, sizeof(first));
		std::memset(&second, 0x5a, sizeof(second));
		Require(!DecodeGuardedODBCRowCount(first, second, result));
		std::cout << "PASS: 32/64-bit exact/zero/unknown/inconsistent/unwritten row counts\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << "FAIL: " << error.what() << '\n';
		return 1;
	}
}
