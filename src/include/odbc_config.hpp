//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_config.hpp
//
// ODBC configuration header to handle platform-specific includes
//
//===----------------------------------------------------------------------===//

#pragma once

// Platform-specific ODBC configuration
// This header must be included BEFORE any other ODBC-related headers

#ifdef _WIN32
	// On Windows, we must include windows.h before ODBC headers
	// to ensure proper type definitions
	#ifndef WIN32_LEAN_AND_MEAN
		#define WIN32_LEAN_AND_MEAN
	#endif
	#ifndef NOMINMAX
		#define NOMINMAX
	#endif
	#include <windows.h>
#endif

// Now include ODBC headers in the correct order
#include <sql.h>
#include <sqlext.h>
