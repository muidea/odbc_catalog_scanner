#include "include/odbc_result.hpp"
#include "odbc_connection.hpp"
#include "storage/odbc_connection_pool.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "odbc_types.hpp"
#include <cstdint>
#include <string>
#include <cstring>
#include <climits>
#include <cstdio>
#include <cmath>
#include <thread>

namespace duckdb {

static const size_t DEFAULT_BIND_BUFFER_SIZE = 64;
static const size_t MAX_BIND_BUFFER_SIZE = 65536;

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
		return 64;
	default:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE) {
			return static_cast<size_t>(column_size);
		}
		return DEFAULT_BIND_BUFFER_SIZE;
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
                       idx_t affected_rows_p, SQLSMALLINT num_cols_p, shared_ptr<ODBCConnection> connection_p,
                       ODBCResultStreaming streaming_p)
    : query(query_p), stmt(std::move(stmt_p)), type_config(std::move(type_config_p)), affected_rows(affected_rows_p),
      connection(std::move(connection_p)),
      is_streaming(streaming_p == ODBCResultStreaming::ALLOW_STREAMING) {

	if (!stmt) {
		throw InternalException("ODBCResult: Statement handle is null for query \"%s\"\n", query_p.c_str());
	}

	if (affected_rows != static_cast<idx_t>(-1)) {
		if (connection && !connection_released) {
			connection->ReleaseConnection();
			connection_released = true;
		}
		return;
	}

	try {
		this->fields = ReadFields(query, stmt.get(), type_config);

		if (fields.empty()) {
			SQLSMALLINT num_cols;
			SQLRETURN retcode = SQLNumResultCols(stmt.get(), &num_cols);
			if (SQL_SUCCEEDED(retcode) && num_cols > 0) {
				this->fields = ReadFields(query, stmt.get(), type_config);
			}
		}

		if (!fields.empty()) {
			InitBinds(fields, stmt.get());
			auto ltypes = CreateChunkTypes(fields, type_config);
			this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);

			if (!is_streaming) {
				ReadAllData();
			}
		}

		if (!is_streaming) {
			if (connection && !connection_released) {
				connection->ReleaseConnection();
				connection_released = true;
			}
		}
	} catch (const std::exception &e) {
		if (connection && !connection_released) {
			connection->ReleaseConnection();
			connection_released = true;
		}
		throw;
	}
}

void ODBCResult::ReadAllData() {
	// 清空现有的缓存
	cached_chunks.clear();
	
	// 如果没有字段，说明是空结果集
	if (fields.empty()) {
		return;
	}
	
	// 保存原始的data_chunk状态
	DataChunk original_chunk;
	original_chunk.Initialize(Allocator::DefaultAllocator(), data_chunk.GetTypes());
	
	// 循环读取所有数据，直到没有更多行
	while (true) {
		// 创建一个新的数据块
		auto chunk = make_uniq<DataChunk>();
		auto ltypes = CreateChunkTypes(fields, type_config);
		chunk->Initialize(Allocator::DefaultAllocator(), ltypes);
		
		// 临时设置data_chunk指向当前chunk，以便WriteToChunk正确写入
		data_chunk.Reference(*chunk);
		
		idx_t r = 0;
		for (; r < STANDARD_VECTOR_SIZE; r++) {
			if (!FetchNext()) {
				break;
			}
			
			WriteToChunk(r);
		}
		
		// 恢复原始的data_chunk
		data_chunk.Reference(original_chunk);
		
		if (r == 0) {
			// 没有读取到任何数据，结束循环
			break;
		}
		
		// 设置数据块大小并添加到缓存
		chunk->SetCardinality(r);
		cached_chunks.push_back(std::move(chunk));
		
		// 如果读取的行数少于标准向量大小，说明已经读取完所有数据
		if (r < STANDARD_VECTOR_SIZE) {
			break;
		}
	}
	
	// 重置当前读取位置
	current_chunk_idx = 0;
}

ODBCResult::~ODBCResult() {
	if (stmt) {
		if (is_streaming && !stream_exhausted) {
			SQLCancel(static_cast<SQLHSTMT>(stmt.get()));
		}
		stmt.reset();
	}

	if (connection && !connection_released) {
		connection->ReleaseConnection();
	}
}

void ODBCResult::HandleTruncatedData() {
	for (size_t i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];

		if (f.bind_length == SQL_NULL_DATA) {
			continue;
		}

		bool truncated = (f.bind_length == SQL_NO_TOTAL) ||
		                 (f.bind_length > 0 && static_cast<size_t>(f.bind_length) > f.bind_buffer.size());
		if (!truncated) {
			continue;
		}

		SQLSMALLINT c_type = ODBCTypes::GetCType(f.odbc_type);

		// Determine the full data length; double the buffer when length is unknown
		size_t total_len;
		if (f.bind_length != SQL_NO_TOTAL && f.bind_length > 0) {
			total_len = static_cast<size_t>(f.bind_length);
		} else {
			total_len = f.bind_buffer.size() * 2;
		}

		// Cap at a reasonable maximum to prevent OOM
		if (total_len > MAX_BIND_BUFFER_SIZE * 16) {
			total_len = MAX_BIND_BUFFER_SIZE * 16;
		}

		// For character data, retrieve remaining bytes via SQLGetData.
		// The bind buffer already contains the first portion (before truncation).
		if (c_type == SQL_C_CHAR || c_type == SQL_C_BINARY) {
			size_t already_have = f.bind_buffer.size();
			if (c_type == SQL_C_CHAR && already_have > 0) {
				already_have--; // null terminator doesn't count
			}

			f.bind_buffer.resize(total_len + 1);

			SQLLEN remaining_len = 0;
			SQLRETURN retcode = SQLGetData(stmt.get(), static_cast<SQLUSMALLINT>(i + 1), c_type,
			                               f.bind_buffer.data() + already_have,
			                               static_cast<SQLLEN>(f.bind_buffer.size() - already_have),
			                               &remaining_len);

			if (SQL_SUCCEEDED(retcode) && remaining_len >= 0) {
				f.bind_length = static_cast<SQLLEN>(already_have) + remaining_len;
			} else {
				// SQLGetData failed; keep what we have, clamp length to buffer
				f.bind_length = static_cast<SQLLEN>(already_have);
			}
		} else {
			// Non-character types: just resize and re-bind for next fetch
			f.bind_buffer.resize(total_len);
			SQLBindCol(stmt.get(), static_cast<SQLUSMALLINT>(i + 1), c_type, f.bind_buffer.data(),
			           static_cast<SQLLEN>(f.bind_buffer.size()), &f.bind_length);
		}
	}
}

DataChunk &ODBCResult::NextChunk() {
	if (is_streaming) {
		data_chunk.Reset();

		if (stream_exhausted) {
			data_chunk.SetCardinality(0);
			return data_chunk;
		}

		idx_t r = 0;
		for (; r < STANDARD_VECTOR_SIZE; r++) {
			if (!FetchNext()) {
				stream_exhausted = true;
				break;
			}
			WriteToChunk(r);
		}

		data_chunk.SetCardinality(r);

		if (stream_exhausted && connection && !connection_released) {
			connection->ReleaseConnection();
			connection_released = true;
		}

		return data_chunk;
	}

	// Materialized mode: return from cache
	if (!cached_chunks.empty()) {
		if (current_chunk_idx < cached_chunks.size()) {
			data_chunk.Reference(*cached_chunks[current_chunk_idx]);
			current_chunk_idx++;
			return data_chunk;
		} else {
			data_chunk.SetCardinality(0);
			return data_chunk;
		}
	}

	data_chunk.SetCardinality(0);
	return data_chunk;
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
		// 获取详细的ODBC错误信息
		SQLCHAR sqlstate[6] = {0};
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
		SQLINTEGER native_error = 0;
		SQLSMALLINT message_length = 0;
		
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, sqlstate, &native_error,
		              message, SQL_MAX_MESSAGE_LENGTH, &message_length);
		
		throw IOException("Failed to fetch result row for ODBC query \"%s\": SQLState=%s, NativeError=%d, Message=%s\n",
		                  query.c_str(), reinterpret_cast<const char*>(sqlstate), native_error, reinterpret_cast<const char*>(message));
	}

	if (retcode == SQL_NO_DATA) {
		return false;
	}

	HandleTruncatedData();
	
	for (size_t i = 0; i < fields.size(); i++) {
		fields[i].bind_is_null = (fields[i].bind_length == SQL_NULL_DATA);
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

	if (f.bind_length < 0 || static_cast<size_t>(f.bind_length) > f.bind_buffer.size()) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	string decimal_str(f.bind_buffer.data(), static_cast<size_t>(f.bind_length));

	auto &vec_type = vec.GetType();
	uint8_t precision = DecimalType::GetWidth(vec_type);
	uint8_t scale = DecimalType::GetScale(vec_type);

	try {
		size_t dot_pos = decimal_str.find('.');
		string int_part, frac_part;

		if (dot_pos != string::npos) {
			int_part = decimal_str.substr(0, dot_pos);
			frac_part = decimal_str.substr(dot_pos + 1);
		} else {
			int_part = decimal_str;
		}

		if (frac_part.length() < scale) {
			frac_part.append(scale - frac_part.length(), '0');
		} else if (frac_part.length() > scale) {
			frac_part = frac_part.substr(0, scale);
		}

		string scaled_str = int_part + frac_part;

		// DuckDB DECIMAL internal storage depends on precision:
		//   1-4   → int16_t
		//   5-9   → int32_t
		//   10-18 → int64_t
		//   19-38 → hugeint_t
		if (precision <= 4) {
			FlatVector::GetData<int16_t>(vec)[row] = static_cast<int16_t>(std::stoi(scaled_str));
		} else if (precision <= 9) {
			FlatVector::GetData<int32_t>(vec)[row] = static_cast<int32_t>(std::stol(scaled_str));
		} else if (precision <= 18) {
			FlatVector::GetData<int64_t>(vec)[row] = std::stoll(scaled_str);
		} else if (precision <= 38) {
			hugeint_t huge_val;
			if (!Hugeint::TryConvert(scaled_str.c_str(), huge_val)) {
				throw InternalException("Decimal value out of hugeint range: %s", scaled_str.c_str());
			}
			FlatVector::GetData<hugeint_t>(vec)[row] = huge_val;
		} else {
			throw InternalException("Unsupported DECIMAL precision %u for column %s (max: 38)", precision,
			                        f.name.c_str());
		}
	} catch (const std::exception &e) {
		throw InternalException("Failed to parse decimal value '%s' for column %s: %s", decimal_str.c_str(),
		                        f.name.c_str(), e.what());
	}
}

static void WriteString(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(vec.GetType().id() == LogicalTypeId::VARCHAR || vec.GetType().id() == LogicalTypeId::BLOB);

	if (f.odbc_type == SQL_TYPE_TIME) {
		WriteTimeAsString(f, vec, row);
		return;
	}

	if (f.bind_buffer.empty()) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	if (f.bind_length < 0) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	if (static_cast<size_t>(f.bind_length) > f.bind_buffer.size()) {
		f.bind_length = static_cast<SQLLEN>(f.bind_buffer.size());
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

		if (f.bind_length == SQL_NULL_DATA) {
			FlatVector::SetNull(vec, row, true);
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
