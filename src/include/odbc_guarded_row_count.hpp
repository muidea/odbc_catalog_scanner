//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_guarded_row_count.hpp
//
//===----------------------------------------------------------------------===//
#pragma once

#include "odbc_types.hpp"
#include <cstring>

namespace duckdb {

// Some CLI drivers expose 32-bit SQLLEN through a 64-bit driver manager.
// Two identical row-count reads with different padding prove the write width;
// a zero-initialized buffer alone would mistake 32-bit -1 for 4294967295.
inline bool DecodeGuardedODBCRowCount(SQLLEN first, SQLLEN second, int64_t &result) {
	if (sizeof(SQLLEN) == 8) {
		auto a = reinterpret_cast<const unsigned char *>(&first);
		auto b = reinterpret_cast<const unsigned char *>(&second);
		bool narrow = true;
		for (idx_t i = 4; i < 8; i++) {
			narrow = narrow && a[i] == 0xa5 && b[i] == 0x5a;
		}
		if (narrow) {
			int32_t x, y;
			std::memcpy(&x, a, sizeof(x));
			std::memcpy(&y, b, sizeof(y));
			if (x != y || x < 0) {
				return false;
			}
			result = x;
			return true;
		}
	}
	if (first != second || first < 0) {
		return false;
	}
	result = first;
	return true;
}

} // namespace duckdb
