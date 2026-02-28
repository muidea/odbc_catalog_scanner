#include "include/odbc_result.hpp"
#include "odbc_connection.hpp"
#include "storage/odbc_connection_pool.hpp"

#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "odbc_types.hpp"
#include <sql.h>
#include <sqlext.h>
#include <cstdint>
#include <string>
#include <cstring>
#include <climits>
#include <cstdio>
#include <cmath>
#include <thread>

namespace duckdb {

static const size_t DEFAULT_BIND_BUFFER_SIZE = 64;

static size_t GetBufferSize(SQLSMALLINT odbc_type, SQLULEN column_size, const ODBCTypeConfig &type_config) {
	switch (odbc_type) {
	case SQL_TINYINT:
	case SQL_BIT:
		return sizeof(int8_t);
	case SQL_SMALLINT:
		return sizeof(int16_t);
	case SQL_INTEGER:
		return sizeof(int32_t);
	case SQL_REAL:
		return sizeof(float);
	case SQL_FLOAT:
	case SQL_DOUBLE:
		return sizeof(double);
	case SQL_BIGINT:
		return sizeof(int64_t);
	case SQL_TYPE_DATE:
	case SQL_TYPE_TIME:
	case SQL_TYPE_TIMESTAMP:
		return sizeof(SQL_TIMESTAMP_STRUCT);
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		return 64; // Large enough for decimal strings
	default:
		return column_size > 0 ? column_size : DEFAULT_BIND_BUFFER_SIZE;
	}
}

ODBCField::ODBCField(void *field_info, LogicalType duckdb_type_p, const ODBCTypeConfig &type_config)
    : duckdb_type(std::move(duckdb_type_p)) {
	// For ODBC, we need to extract field information differently
	// This will be implemented in the ReadFields function
	bind_buffer.resize(DEFAULT_BIND_BUFFER_SIZE);
}

void ODBCField::ResetBind() {
	this->bind_is_null = false;
	this->bind_length = 0;
	this->bind_error = false;
}

static vector<ODBCField> ReadFields(const std::string &query, SQLHSTMT stmt, const ODBCTypeConfig &type_config) {
	vector<ODBCField> fields;
	SQLSMALLINT num_cols;
	SQLRETURN retcode = SQLNumResultCols(stmt, &num_cols);
	
	if (!SQL_SUCCEEDED(retcode) || num_cols == 0) {
		return fields;
	}

	fields.reserve(num_cols);
	for (SQLSMALLINT i = 0; i < num_cols; i++) {
		SQLCHAR col_name[256];
		SQLSMALLINT name_len, data_type, decimal_digits, nullable;
		SQLULEN column_size;
		
		retcode = SQLDescribeCol(stmt, i + 1, col_name, sizeof(col_name), &name_len,
		                        &data_type, &column_size, &decimal_digits, &nullable);
		
		if (!SQL_SUCCEEDED(retcode)) {
			throw IOException("Failed to describe column %d for ODBC query \"%s\"\n", i + 1, query.c_str());
		}
		
		// Convert ODBC type to logical type
		LogicalType ltype = ODBCTypes::ODBCToLogicalType(type_config, data_type, column_size, decimal_digits);
		
		// Create field with placeholder information
		ODBCField field(nullptr, std::move(ltype), type_config);
		field.name = string(reinterpret_cast<char*>(col_name), name_len);
		field.odbc_type = data_type;
		
		// Resize buffer based on ODBC type
		size_t buf_size = GetBufferSize(data_type, column_size, type_config);
		field.bind_buffer.resize(buf_size);
		
		fields.emplace_back(std::move(field));
	}
	return fields;
}

static void InitBinds(vector<ODBCField> &fields, SQLHSTMT stmt) {
	for (SQLSMALLINT i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];
		
		SQLSMALLINT c_type = ODBCTypes::GetCType(f.odbc_type);
		
		SQLRETURN retcode = SQLBindCol(stmt, i + 1,
		                              c_type,
		                              f.bind_buffer.data(),
		                              f.bind_buffer.size(),
		                              &f.bind_length);
		
		if (!SQL_SUCCEEDED(retcode)) {
			throw IOException("Failed to bind column %d for ODBC query\n", i + 1);
		}
	}
}

static vector<LogicalType> CreateChunkTypes(vector<ODBCField> &fields, const ODBCTypeConfig &type_config) {
	vector<LogicalType> ltypes;
	for (idx_t c = 0; c < fields.size(); c++) {
		ODBCField &f = fields[c];
		LogicalType &lt = f.duckdb_type;
		
		// For ODBC, we use the already converted logical types
		// Additional type adjustments can be made here if needed
		ltypes.push_back(lt);
	}
	return ltypes;
}

ODBCResult::ODBCResult(const std::string &query_p, ODBCStatementPtr stmt_p, ODBCTypeConfig type_config_p,
                          idx_t affected_rows_p, SQLSMALLINT num_cols_p, shared_ptr<ODBCConnection> connection_p)
    : query(query_p), stmt(std::move(stmt_p)), type_config(std::move(type_config_p)), affected_rows(affected_rows_p), connection(std::move(connection_p)) {
	
	// 验证语句句柄
	if (!stmt) {
		throw InternalException("ODBCResult: Statement handle is null for query \"%s\"\n", query_p.c_str());
	}
	
	// For SELECT queries, affected_rows should be -1 and we should have columns
	// For DML queries, affected_rows will be >= 0 and we may not have columns
	if (affected_rows != static_cast<idx_t>(-1)) {
		// This is a DML query (INSERT, UPDATE, DELETE) that doesn't return a result set
		// 对于DML查询，也需要释放连接繁忙状态
		if (connection && !connection_released) {
			connection->ReleaseConnection();
			connection_released = true;
		}
		return;
	}
	
	// For SELECT queries, we need to read the fields and initialize the data chunk
	try {
		this->fields = ReadFields(query, stmt.get(), type_config);
		
		if (fields.empty()) {
			// If ReadFields returned empty, try to get column count again
			SQLSMALLINT num_cols;
			SQLRETURN retcode = SQLNumResultCols(stmt.get(), &num_cols);
			
			if (SQL_SUCCEEDED(retcode) && num_cols > 0) {
				this->fields = ReadFields(query, stmt.get(), type_config);
			}
		}
		
		if (!fields.empty()) {
			// 初始化列绑定
			InitBinds(fields, stmt.get());
			
			// 创建逻辑类型并初始化数据块
			auto ltypes = CreateChunkTypes(fields, type_config);
			this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);
		}
		
		// 查询已成功执行，语句句柄已分配，现在可以释放连接繁忙状态
		// 连接可以用于其他查询（使用不同的语句句柄）
		if (connection && !connection_released) {
			connection->ReleaseConnection();
			connection_released = true;
		}
	} catch (const std::exception &e) {
		throw;
	}
}

ODBCResult::~ODBCResult() {
	CleanupODBCState();
	
	// 连接已经在构造函数中释放，这里只需要记录日志
	// 如果由于异常等原因连接未被释放，则尝试释放
	if (connection && !connection_released) {
		connection->ReleaseConnection();
	}
}

void ODBCResult::CleanupODBCState() {
	// 确保所有ODBC结果都被消费完，避免连接状态残留
	if (stmt) {
		// 对于FreeTDS驱动，需要更彻底的清理
		// 首先检查是否有未消费的结果集
		SQLSMALLINT num_cols = 0;
		SQLRETURN retcode = SQLNumResultCols(stmt.get(), &num_cols);
		
		if (SQL_SUCCEEDED(retcode) && num_cols > 0) {
			// 有未消费的结果集，尝试消费所有行
			// 这对于FreeTDS驱动特别重要，否则连接会保持繁忙状态
			while (true) {
				retcode = SQLFetch(stmt.get());
				if (retcode == SQL_NO_DATA || !SQL_SUCCEEDED(retcode)) {
					break;
				}
			}
		}
		
		// 尝试关闭游标
		SQLCloseCursor(stmt.get());
		
		// 尝试取消任何挂起的操作
		SQLCancel(stmt.get());
		
		// 对于某些驱动，可能需要多次关闭游标
		SQLCloseCursor(stmt.get());
		
		// 检查语句状态
		SQLLEN row_count = 0;
		SQLRowCount(stmt.get(), &row_count);
		
		// 让ODBCStatementDelete自定义删除器处理最终的清理
		// 它会调用SQLFreeStmt(stmt, SQL_CLOSE)和SQLFreeHandle
	}
}

void ODBCResult::HandleTruncatedData() {
	// For ODBC, truncated data handling is different
	// We rely on SQLGetData for large data or use larger buffers
	// This is a simplified implementation
	for (size_t i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];
		
		// Skip NULL values (bind_length == -1)
		if (f.bind_length == SQL_NULL_DATA) {
			continue;
		}
		
		// Only handle truncation for non-NULL data
		if (f.bind_length == SQL_NO_TOTAL || f.bind_length > f.bind_buffer.size()) {
			// Data was truncated, we need to handle this case
			// For now, we'll just resize the buffer and re-bind
			size_t new_size = f.bind_length > 0 ? f.bind_length : f.bind_buffer.size() * 2;
			f.bind_buffer.resize(new_size);
			
			SQLRETURN retcode = SQLBindCol(stmt.get(), i + 1,
			                              ODBCTypes::GetCType(f.odbc_type),
			                              f.bind_buffer.data(),
			                              f.bind_buffer.size(),
			                              &f.bind_length);
			
			if (!SQL_SUCCEEDED(retcode)) {
				throw IOException("Failed to re-bind column %zu for ODBC query \"%s\"\n", i, query.c_str());
			}
		}
	}
}

DataChunk &ODBCResult::NextChunk() {
	
	this->data_chunk.Reset();
	this->row_idx = 0;

	idx_t r = 0;
	for (; r < STANDARD_VECTOR_SIZE; r++) {
		if (!FetchNext()) {
			break;
		}

		WriteToChunk(r);
	}

	this->data_chunk.SetCardinality(r);
	
	return this->data_chunk;
}

bool ODBCResult::FetchNext() {
	if (fields.empty()) {
		return false;
	}
	
	for (auto &f : fields) {
		f.ResetBind();
	}

	SQLRETURN retcode = SQLFetch(stmt.get());

	if (retcode == SQL_ERROR) {
		throw IOException("Failed to fetch result row for ODBC query \"%s\"\n", query.c_str());
	}

	if (retcode == SQL_NO_DATA) {
		return false;
	}

	HandleTruncatedData();
	
	for (size_t i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];
	}

	bool result = (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO);
	return result;
}

bool ODBCResult::Next() {
	if (row_idx < data_chunk.size() - 1) {
		row_idx += 1;
		return true;
	}
	
	NextChunk();
	row_idx = 0;
	bool result = data_chunk.size() > 0;
	return result;
}

void ODBCResult::CheckColumnIdx(idx_t col) {
	if (col >= data_chunk.ColumnCount()) {
		throw IOException("Column: %zu out of range of field count: %zu, ODBC query \"%s\"\n", col,
		                  data_chunk.ColumnCount(), query.c_str());
	}
}

void ODBCResult::CheckNotNull(idx_t col) {
	if (IsNull(col)) {
		throw InternalException("Get called for a NULL value, column: %zu, ODBC query \"%s\"\n", col, query.c_str());
	}
}

string ODBCResult::GetString(idx_t col) {
	CheckNotNull(col);
	ODBCField &f = fields[col];
	if (f.duckdb_type.id() == LogicalTypeId::VARCHAR || f.duckdb_type.id() == LogicalTypeId::BLOB) {
		Vector &vec = data_chunk.data[col];
		string_t *data = FlatVector::GetData<string_t>(vec);
		string_t &st = data[row_idx];
		return string(st.GetData(), st.GetSize());
	}
	throw InternalException("Get called for a String type, actual type: \"%s\", column: %zu, ODBC query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int32_t ODBCResult::GetInt32(idx_t col) {
	CheckNotNull(col);
	ODBCField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	if (f.duckdb_type.id() == LogicalTypeId::INTEGER) {
		int32_t *data = FlatVector::GetData<int32_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UINTEGER) {
		uint32_t *data = FlatVector::GetData<uint32_t>(vec);
		return static_cast<int32_t>(data[row_idx]);
	}
	throw InternalException("Get called for an Int32 type, actual type: \"%s\", column: %zu, ODBC query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int64_t ODBCResult::GetInt64(idx_t col) {
	CheckNotNull(col);
	ODBCField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	
	// Handle various integer types that can be converted to int64_t
	switch (f.duckdb_type.id()) {
	case LogicalTypeId::TINYINT: {
		int8_t *data = FlatVector::GetData<int8_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::UTINYINT: {
		uint8_t *data = FlatVector::GetData<uint8_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::SMALLINT: {
		int16_t *data = FlatVector::GetData<int16_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::USMALLINT: {
		uint16_t *data = FlatVector::GetData<uint16_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::INTEGER: {
		int32_t *data = FlatVector::GetData<int32_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::UINTEGER: {
		uint32_t *data = FlatVector::GetData<uint32_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::BIGINT: {
		int64_t *data = FlatVector::GetData<int64_t>(vec);
		return data[row_idx];
	}
	case LogicalTypeId::UBIGINT: {
		uint64_t *data = FlatVector::GetData<uint64_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::DOUBLE: {
		if (vec.GetType().id() == LogicalTypeId::DOUBLE) {
			double *data = FlatVector::GetData<double>(vec);
			return static_cast<int64_t>(data[row_idx]);
		} else if (vec.GetType().id() == LogicalTypeId::VARCHAR) {
			string_t *data = FlatVector::GetData<string_t>(vec);
			string_t st = data[row_idx];
			return atoll(st.GetData());
		}
		break;
	}
	default:
		break;
	}
	
	throw InternalException("Get called for an Int64 type, actual type: \"%s\", column: %zu, ODBC query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

bool ODBCResult::IsNull(idx_t col) {
	CheckColumnIdx(col);
	Vector &vec = data_chunk.data[col];
	return FlatVector::IsNull(vec, row_idx);
}

idx_t ODBCResult::AffectedRows() {
	if (affected_rows == idx_t(-1)) {
		throw InternalException("ODBCResult::AffectedRows called for result "
		                        "that didn't affect any rows, query \"%s\"\n",
		                        query.c_str());
	}
	return affected_rows;
}

const vector<ODBCField> &ODBCResult::Fields() {
	return fields;
}

// ODBC TIME values use SQL_TIMESTAMP_STRUCT
static void WriteTimeAsString(ODBCField &f, Vector &vec, idx_t row) {
	
	SQL_TIMESTAMP_STRUCT *ts = reinterpret_cast<SQL_TIMESTAMP_STRUCT *>(f.bind_buffer.data());
	if (ts->hour == 0 && ts->minute == 0 && ts->second == 0) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
	string time_str = Time::ToString(tm);
	string_t st(time_str.c_str(), time_str.length());
	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

static void WriteDecimal(ODBCField &f, Vector &vec, idx_t row) {
	if (f.bind_length == SQL_NULL_DATA) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	
	// ODBC decimal values are typically passed as strings
	// We need to parse the string and convert to appropriate decimal value
	string decimal_str(f.bind_buffer.data(), f.bind_length);
	
	// Get the vector's type
	auto &vec_type = vec.GetType();
	
	// Get decimal precision and scale from the logical type
	uint8_t precision = DecimalType::GetWidth(vec_type);
	uint8_t scale = DecimalType::GetScale(vec_type);
	
	
	try {
		// Parse the decimal string
		size_t dot_pos = decimal_str.find('.');
		string int_part, frac_part;
		
		if (dot_pos != string::npos) {
			int_part = decimal_str.substr(0, dot_pos);
			frac_part = decimal_str.substr(dot_pos + 1);
		} else {
			int_part = decimal_str;
			frac_part = "0";
		}
		
		// Pad fractional part to the expected scale
		if (frac_part.length() < scale) {
			frac_part.append(scale - frac_part.length(), '0');
		} else if (frac_part.length() > scale) {
			// Truncate if fractional part is longer than scale
			frac_part = frac_part.substr(0, scale);
		}
		
		
		// Combine integer and fractional parts
		string scaled_str = int_part + frac_part;
		
		// Determine which integer type to use based on precision
		// DECIMAL precision determines the internal storage type:
		// - precision <= 18: INT64
		// - precision <= 38: INT128 (hugeint_t)
		// - precision > 38: HUGEINT (not supported by ODBC)
		
		if (precision <= 18) {
			// Use INT64 for DECIMAL with precision <= 18
			try {
				int64_t scaled_val = std::stoll(scaled_str);
				auto data = FlatVector::GetData<int64_t>(vec);
				data[row] = scaled_val;
				return;
			} catch (const std::exception &e) {
				throw;
			}
		} else if (precision <= 38) {
			// Use INT128 (hugeint_t) for DECIMAL with precision <= 38
			try {
				// For hugeint_t, we need to parse the string differently
				// Since std::stoll doesn't support 128-bit integers
				// We'll use string parsing for now
				hugeint_t huge_val;
				
				// Simple string to hugeint conversion
				// For now, we'll handle values that fit in 64-bit
				// For larger values, we'd need a proper big integer parser
				try {
					// Try to parse as int64 first
					int64_t int64_val = std::stoll(scaled_str);
					huge_val.lower = int64_val;
					// Sign extend for negative numbers
					if (int64_val < 0) {
						huge_val.upper = -1;
					} else {
						huge_val.upper = 0;
					}
				} catch (const std::out_of_range &e) {
					// Value doesn't fit in int64, need proper hugeint parsing
					// For now, we'll throw an error
					throw InternalException("Decimal value too large for int64, needs proper hugeint parsing: %s", 
						scaled_str.c_str());
				}
				
				auto data = FlatVector::GetData<hugeint_t>(vec);
				data[row] = huge_val;
				return;
			} catch (const std::exception &e) {
				throw;
			}
		} else {
			throw InternalException("Unsupported DECIMAL precision %u for column %s (max supported: 38)", 
				precision, f.name.c_str());
		}
		
	} catch (const std::exception &e) {
		// Cannot fall back to string for DECIMAL type
		throw InternalException("Failed to parse decimal value '%s' for column %s: %s", 
			string(f.bind_buffer.data(), f.bind_length).c_str(), f.name.c_str(), e.what());
	}
}

static void WriteString(ODBCField &f, Vector &vec, idx_t row) {
	
	// WriteString should only be called for VARCHAR or BLOB types
	// If called for other types, it's a bug in our code
	D_ASSERT(vec.GetType().id() == LogicalTypeId::VARCHAR || vec.GetType().id() == LogicalTypeId::BLOB);
	
	if (f.odbc_type == SQL_TYPE_TIME) {
		WriteTimeAsString(f, vec, row);
		return;
	}

	if (f.bind_buffer.empty()) {
		throw InternalException("Empty bind buffer in WriteString");
	}
	
	if (f.bind_length < 0) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	
	if (f.bind_length > f.bind_buffer.size()) {
		// Truncate to buffer size
		f.bind_length = f.bind_buffer.size();
	}
	
	// Handle numeric types that should be converted to string
	// Check for standard ODBC numeric types and also handle potential SQL Server specific codes
	bool is_numeric_type = 
		f.odbc_type == SQL_INTEGER || f.odbc_type == SQL_BIGINT || 
		f.odbc_type == SQL_SMALLINT || f.odbc_type == SQL_TINYINT ||
		f.odbc_type == SQL_REAL || f.odbc_type == SQL_FLOAT || f.odbc_type == SQL_DOUBLE ||
		f.odbc_type == SQL_DECIMAL || f.odbc_type == SQL_NUMERIC;
	
	// Also check for common SQL Server integer type codes that might not match standard ODBC
	// SQL Server might use different type codes for integers
	if (is_numeric_type || 
	    (f.odbc_type >= -10 && f.odbc_type <= -5) || // Common SQL Server type code range
	    f.odbc_type == 4 || f.odbc_type == 5 || f.odbc_type == -6 || f.odbc_type == -7) { // Additional SQL Server codes
		// Convert numeric value to string
		// First check if we have valid data
		if (f.bind_length == SQL_NULL_DATA) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		// Convert numeric value to string
		// First check if we have valid data
		if (f.bind_length == SQL_NULL_DATA) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		
		// Determine actual data size based on ODBC type
		size_t data_size = 0;
		switch (f.odbc_type) {
		case SQL_TINYINT:
			data_size = sizeof(int8_t);
			break;
		case SQL_SMALLINT:
			data_size = sizeof(int16_t);
			break;
		case SQL_INTEGER:
			data_size = sizeof(int32_t);
			break;
		case SQL_BIGINT:
			data_size = sizeof(int64_t);
			break;
		case SQL_REAL:
			data_size = sizeof(float);
			break;
		case SQL_FLOAT:
		case SQL_DOUBLE:
			data_size = sizeof(double);
			break;
		default:
			data_size = f.bind_length;
			break;
		}
		
		// Ensure we have enough data
		if (f.bind_buffer.size() >= data_size) {
			string str_val;
			switch (f.odbc_type) {
			case SQL_TINYINT: {
				int8_t val = *reinterpret_cast<int8_t*>(f.bind_buffer.data());
				str_val = std::to_string(val);
				break;
			}
			case SQL_SMALLINT: {
				int16_t val = *reinterpret_cast<int16_t*>(f.bind_buffer.data());
				str_val = std::to_string(val);
				break;
			}
			case SQL_INTEGER: {
				int32_t val = *reinterpret_cast<int32_t*>(f.bind_buffer.data());
				str_val = std::to_string(val);
				break;
			}
			case SQL_BIGINT: {
				int64_t val = *reinterpret_cast<int64_t*>(f.bind_buffer.data());
				str_val = std::to_string(val);
				break;
			}
			case SQL_REAL: {
				float val = *reinterpret_cast<float*>(f.bind_buffer.data());
				str_val = std::to_string(val);
				break;
			}
			case SQL_FLOAT:
			case SQL_DOUBLE: {
				double val = *reinterpret_cast<double*>(f.bind_buffer.data());
				str_val = std::to_string(val);
				break;
			}
			default:
				// Use raw string data
				str_val = string(f.bind_buffer.data(), f.bind_length);
				break;
			}
			
			string_t st(str_val.c_str(), str_val.length());
			auto data = FlatVector::GetData<string_t>(vec);
			data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
			return;
		}
	}
	
	string_t st(f.bind_buffer.data(), f.bind_length);
	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

static void WriteBool(ODBCField &f, Vector &vec, idx_t row) {
	
	D_ASSERT(f.bind_buffer.size() >= sizeof(int8_t));
	auto data = FlatVector::GetData<bool>(vec);
	if (f.odbc_type == SQL_TINYINT || f.odbc_type == SQL_BIT) {
		int8_t val = *reinterpret_cast<int8_t *>(f.bind_buffer.data());
		data[row] = val != 0;
	}
	if (f.bind_length == 0) {
		throw BinderException("Failed to cast ODBC boolean - expected 1 byte "
		                      "element but got element of size %d\n",
		                      f.bind_length);
	}
	// For ODBC, boolean values can be represented in various ways
	// We check the first character for common boolean representations
	char first = f.bind_buffer[0];
	if (first == '\0' || first == '0' || first == 'f' || first == 'F' || first == '-') {
		data[row] = false;
	} else {
		data[row] = true;
	}
}

template <typename NUM_TYPE>
static void WriteNumber(ODBCField &f, Vector &vec, idx_t row) {
	
	D_ASSERT(f.bind_buffer.size() >= sizeof(NUM_TYPE));
	NUM_TYPE num = *reinterpret_cast<NUM_TYPE *>(f.bind_buffer.data());
	
	
	auto data = FlatVector::GetData<NUM_TYPE>(vec);
	data[row] = num;
}

static void WriteDateTime(ODBCField &f, Vector &vec, idx_t row) {
	
	D_ASSERT(f.bind_buffer.size() >= sizeof(SQL_TIMESTAMP_STRUCT));
	SQL_TIMESTAMP_STRUCT *ts = reinterpret_cast<SQL_TIMESTAMP_STRUCT *>(f.bind_buffer.data());
	
	// Check for null/zero values
	if ((ts->year == 0 && ts->month == 0 && ts->day == 0 && ts->hour == 0 && ts->minute == 0 && ts->second == 0 &&
	     ts->fraction == 0) ||
	    (f.odbc_type == SQL_TYPE_TIME && ts->hour == 0 && ts->minute == 0 && ts->second == 0) ||
	    (f.odbc_type == SQL_TYPE_DATE && ts->year == 0 && ts->month == 0 && ts->day == 0)) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	switch (vec.GetType().id()) {
	case LogicalTypeId::DATE: {
		date_t val = Date::FromDate(ts->year, ts->month, ts->day);
		date_t *data = FlatVector::GetData<date_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIME: {
		if (ts->hour < 0 || ts->hour > 24 ||
		    (ts->hour == 24 && (ts->minute > 0 || ts->second > 0 || ts->fraction > 0))) {
			throw BinderException("time field value out of range, hour value: " + std::to_string(ts->hour));
		}
		dtime_t val = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		dtime_t *data = FlatVector::GetData<dtime_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		date_t dt = Date::FromDate(ts->year, ts->month, ts->day);
		dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		timestamp_t *data = FlatVector::GetData<timestamp_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		// For ODBC, timezone information is not directly available in SQL_TIMESTAMP_STRUCT
		// We assume local time and convert accordingly
		date_t dt = Date::FromDate(ts->year, ts->month, ts->day);
		dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		timestamp_tz_t *data = FlatVector::GetData<timestamp_tz_t>(vec);
		data[row] = timestamp_tz_t(val.value);
		break;
	}
	default:
		throw InternalException("Unsupported date/time type: " + vec.GetType().ToString());
	}
}

void ODBCResult::WriteToChunk(idx_t row) {
	for (idx_t col = 0; col < data_chunk.ColumnCount(); col++) {
		if (col >= fields.size()) {
			throw InternalException("Column index out of bounds in WriteToChunk");
		}
		
		ODBCField &f = fields[col];
		Vector &vec = data_chunk.data[col];

		if (f.bind_is_null) {
			continue;
		}
		
		// Handle type mismatches - when logical type doesn't match ODBC type
		// This can happen with SQL Server returning unexpected type codes
		if (vec.GetType().id() == LogicalTypeId::VARCHAR) {
			// Vector expects VARCHAR, but ODBC type might be numeric
			// Convert numeric types to string
			if (f.odbc_type == SQL_INTEGER || f.odbc_type == SQL_BIGINT || 
			    f.odbc_type == SQL_SMALLINT || f.odbc_type == SQL_TINYINT ||
			    f.odbc_type == SQL_DECIMAL || f.odbc_type == SQL_NUMERIC ||
			    f.odbc_type == SQL_REAL || f.odbc_type == SQL_FLOAT || f.odbc_type == SQL_DOUBLE) {
				WriteString(f, vec, row);
				continue;
			}
		}
		
		// Also handle the reverse: numeric vector with non-numeric ODBC type
		if ((vec.GetType().id() == LogicalTypeId::BIGINT || vec.GetType().id() == LogicalTypeId::INTEGER ||
		     vec.GetType().id() == LogicalTypeId::SMALLINT || vec.GetType().id() == LogicalTypeId::TINYINT) &&
		    !(f.odbc_type == SQL_INTEGER || f.odbc_type == SQL_BIGINT || 
		      f.odbc_type == SQL_SMALLINT || f.odbc_type == SQL_TINYINT)) {
			WriteString(f, vec, row);
			continue;
		}
		
		if (vec.GetType().id() == LogicalTypeId::BIGINT && f.odbc_type == SQL_INTEGER) {
			// Handle SQL_INTEGER data for BIGINT vector
			WriteNumber<int64_t>(f, vec, row);
			continue;
		}
		
		// More comprehensive type mismatch handling
		if ((vec.GetType().id() == LogicalTypeId::BIGINT && (f.odbc_type == SQL_INTEGER || f.odbc_type == SQL_SMALLINT || f.odbc_type == SQL_TINYINT)) ||
		    (vec.GetType().id() == LogicalTypeId::INTEGER && (f.odbc_type == SQL_BIGINT || f.odbc_type == SQL_SMALLINT || f.odbc_type == SQL_TINYINT)) ||
		    (vec.GetType().id() == LogicalTypeId::SMALLINT && (f.odbc_type == SQL_BIGINT || f.odbc_type == SQL_INTEGER || f.odbc_type == SQL_TINYINT)) ||
		    (vec.GetType().id() == LogicalTypeId::TINYINT && (f.odbc_type == SQL_BIGINT || f.odbc_type == SQL_INTEGER || f.odbc_type == SQL_SMALLINT))) {
			// Handle integer type mismatches
			switch (vec.GetType().id()) {
			case LogicalTypeId::BIGINT:
				WriteNumber<int64_t>(f, vec, row);
				break;
			case LogicalTypeId::INTEGER:
				WriteNumber<int32_t>(f, vec, row);
				break;
			case LogicalTypeId::SMALLINT:
				WriteNumber<int16_t>(f, vec, row);
				break;
			case LogicalTypeId::TINYINT:
				WriteNumber<int8_t>(f, vec, row);
				break;
			default:
				WriteString(f, vec, row);
				break;
			}
			continue;
		}
		
		
		switch (vec.GetType().id()) {
		case LogicalTypeId::BOOLEAN: {
			WriteBool(f, vec, row);
			break;
		}
		case LogicalTypeId::TINYINT: {
			WriteNumber<int8_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::UTINYINT: {
			WriteNumber<uint8_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			WriteNumber<int16_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::USMALLINT: {
			WriteNumber<uint16_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::INTEGER: {
			if (f.odbc_type == SQL_BIGINT) {
				// SQL_BIGINT is 64-bit, but we have INTEGER vector (32-bit)
				// Need to handle 64-bit to 32-bit conversion with bounds checking
				if (f.bind_buffer.size() >= sizeof(int64_t)) {
					int64_t val = *reinterpret_cast<int64_t*>(f.bind_buffer.data());
					// Check for overflow
					if (val < INT32_MIN || val > INT32_MAX) {
						// Convert to string instead
						string str_val = std::to_string(val);
						string_t st(str_val.c_str(), str_val.length());
						auto data = FlatVector::GetData<string_t>(vec);
						data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
					} else {
						int32_t *data = FlatVector::GetData<int32_t>(vec);
						data[row] = static_cast<int32_t>(val);
					}
				} else {
					WriteNumber<int32_t>(f, vec, row);
				}
			} else {
				WriteNumber<int32_t>(f, vec, row);
			}
			break;
		}
		case LogicalTypeId::UINTEGER: {
			WriteNumber<uint32_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::BIGINT: {
			if (f.odbc_type == SQL_INTEGER) {
				// SQL_INTEGER is 32-bit, but we have BIGINT vector (64-bit)
				// Need to handle 32-bit to 64-bit conversion
				if (f.bind_buffer.size() >= sizeof(int32_t)) {
					int32_t val = *reinterpret_cast<int32_t*>(f.bind_buffer.data());
					int64_t *data = FlatVector::GetData<int64_t>(vec);
					data[row] = static_cast<int64_t>(val);
				} else {
					WriteNumber<int64_t>(f, vec, row);
				}
			} else {
				WriteNumber<int64_t>(f, vec, row);
			}
			break;
		}
		case LogicalTypeId::UBIGINT: {
			WriteNumber<uint64_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::FLOAT: {
			WriteNumber<float>(f, vec, row);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			if (f.odbc_type == SQL_DOUBLE || f.odbc_type == SQL_FLOAT) {
				WriteNumber<double>(f, vec, row);
			} else {
				WriteString(f, vec, row);
			}
			break;
		}
		case LogicalTypeId::VARCHAR: {
			WriteString(f, vec, row);
			break;
		}
		case LogicalTypeId::BLOB: {
			WriteString(f, vec, row);
			break;
		}
		case LogicalTypeId::DECIMAL: {
			// DECIMAL types are typically passed as strings in ODBC
			// We need to parse the string and convert to appropriate decimal value
			WriteDecimal(f, vec, row);
			break;
		}
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			WriteDateTime(f, vec, row);
			break;
		}
		default: {
			WriteString(f, vec, row);
		}
		}
	}
}



} // namespace duckdb
