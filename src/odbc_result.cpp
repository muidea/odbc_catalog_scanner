#include "odbc_result.hpp"
#include <iostream>
#include "odbc_connection.hpp"

#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "odbc_types.hpp"
#include <sql.h>
#include <sqlext.h>

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
	std::cout << "[DEBUG] ReadFields - Starting" << std::endl;
	vector<ODBCField> fields;
	SQLSMALLINT num_cols;
	SQLRETURN retcode = SQLNumResultCols(stmt, &num_cols);
	std::cout << "[DEBUG] ReadFields - SQLNumResultCols returned: " << retcode << ", num_cols: " << num_cols << std::endl;
	
	if (!SQL_SUCCEEDED(retcode) || num_cols == 0) {
		std::cout << "[DEBUG] ReadFields - No columns found or error" << std::endl;
		return fields;
	}

	fields.reserve(num_cols);
	for (SQLSMALLINT i = 0; i < num_cols; i++) {
		std::cout << "[DEBUG] ReadFields - Processing column " << i + 1 << std::endl;
		SQLCHAR col_name[256];
		SQLSMALLINT name_len, data_type, decimal_digits, nullable;
		SQLULEN column_size;
		
		retcode = SQLDescribeCol(stmt, i + 1, col_name, sizeof(col_name), &name_len,
		                        &data_type, &column_size, &decimal_digits, &nullable);
		std::cout << "[DEBUG] ReadFields - SQLDescribeCol for column " << i + 1 << " returned: " << retcode << std::endl;
		
		if (!SQL_SUCCEEDED(retcode)) {
			std::cout << "[ERROR] ReadFields - Failed to describe column " << i + 1 << std::endl;
			throw IOException("Failed to describe column %d for ODBC query \"%s\"\n", i + 1, query.c_str());
		}
		
		std::cout << "[DEBUG] ReadFields - Column " << i + 1 << ": name=" << col_name
		          << ", type=" << data_type << ", size=" << column_size << std::endl;
		
		// Convert ODBC type to logical type
		LogicalType ltype = ODBCTypes::ODBCToLogicalType(type_config, data_type, column_size, decimal_digits);
		std::cout << "[DEBUG] ReadFields - Converted to logical type: " << ltype.ToString() << std::endl;
		
		// Create field with placeholder information
		ODBCField field(nullptr, std::move(ltype), type_config);
		field.name = string(reinterpret_cast<char*>(col_name), name_len);
		field.odbc_type = data_type;
		
		// Resize buffer based on ODBC type
		size_t buf_size = GetBufferSize(data_type, column_size, type_config);
		field.bind_buffer.resize(buf_size);
		std::cout << "[DEBUG] ReadFields - Buffer size: " << buf_size << std::endl;
		
		fields.emplace_back(std::move(field));
		std::cout << "[DEBUG] ReadFields - Successfully processed column " << i + 1 << std::endl;
	}

	std::cout << "[DEBUG] ReadFields - Completed, returning " << fields.size() << " fields" << std::endl;
	return fields;
}

static void InitBinds(vector<ODBCField> &fields, SQLHSTMT stmt) {
	for (SQLSMALLINT i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];
		
		SQLRETURN retcode = SQLBindCol(stmt, i + 1,
		                              ODBCTypes::GetCType(f.odbc_type),
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
                         idx_t affected_rows_p, SQLSMALLINT num_cols_p)
    : query(query_p), stmt(std::move(stmt_p)), type_config(std::move(type_config_p)), affected_rows(affected_rows_p) {
	std::cout << "[DEBUG] ODBCResult constructor - Starting" << std::endl;
	std::cout << "[DEBUG] ODBCResult constructor - stmt pointer: " << stmt.get() << std::endl;
	std::cout << "[DEBUG] ODBCResult constructor - Starting" << std::endl;
	
	// For SELECT queries, affected_rows should be -1 and we should have columns
	// For DML queries, affected_rows will be >= 0 and we may not have columns
	if (affected_rows != static_cast<idx_t>(-1)) {
		std::cout << "[DEBUG] ODBCResult constructor - DML query, no result set" << std::endl;
		// This is a DML query (INSERT, UPDATE, DELETE) that doesn't return a result set
		return;
	}
	
	std::cout << "[DEBUG] ODBCResult constructor - SELECT query, reading fields" << std::endl;
	
	// For SELECT queries, we need to read the fields and initialize the data chunk
	try {
		this->fields = ReadFields(query, stmt.get(), type_config);
		std::cout << "[DEBUG] ODBCResult constructor - ReadFields returned " << fields.size() << " fields" << std::endl;
		
		if (fields.empty()) {
			std::cout << "[DEBUG] ODBCResult constructor - No fields found, trying to get column count again" << std::endl;
			// If ReadFields returned empty, try to get column count again
			SQLSMALLINT num_cols;
			SQLRETURN retcode = SQLNumResultCols(stmt.get(), &num_cols);
			std::cout << "[DEBUG] ODBCResult constructor - SQLNumResultCols returned: " << retcode << ", num_cols: " << num_cols << std::endl;
			if (SQL_SUCCEEDED(retcode) && num_cols > 0) {
				this->fields = ReadFields(query, stmt.get(), type_config);
				std::cout << "[DEBUG] ODBCResult constructor - Second ReadFields returned " << fields.size() << " fields" << std::endl;
			}
		}
		
		if (!fields.empty()) {
			std::cout << "[DEBUG] ODBCResult constructor - Initializing binds and data chunk" << std::endl;
			InitBinds(fields, stmt.get());
			auto ltypes = CreateChunkTypes(fields, type_config);
			this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);
			std::cout << "[DEBUG] ODBCResult constructor - Data chunk initialized with " << ltypes.size() << " columns" << std::endl;
		} else {
			std::cout << "[DEBUG] ODBCResult constructor - No fields to process" << std::endl;
		}
	} catch (const std::exception &e) {
		std::cout << "[ERROR] ODBCResult constructor - Exception: " << e.what() << std::endl;
		throw;
	} catch (...) {
		std::cout << "[ERROR] ODBCResult constructor - Unknown exception" << std::endl;
		throw;
	}
	
	std::cout << "[DEBUG] ODBCResult constructor - Completed successfully" << std::endl;
}

void ODBCResult::HandleTruncatedData() {
	std::cout << "[DEBUG] ODBCResult::HandleTruncatedData() - Starting" << std::endl;
	// For ODBC, truncated data handling is different
	// We rely on SQLGetData for large data or use larger buffers
	// This is a simplified implementation
	for (size_t i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];
		
		std::cout << "[DEBUG] HandleTruncatedData - Column " << i << ": bind_length=" << f.bind_length
		          << ", buffer_size=" << f.bind_buffer.size() << std::endl;
		
		// Skip NULL values (bind_length == -1)
		if (f.bind_length == SQL_NULL_DATA) {
			std::cout << "[DEBUG] HandleTruncatedData - Column " << i << " is NULL, skipping" << std::endl;
			continue;
		}
		
		// Only handle truncation for non-NULL data
		if (f.bind_length == SQL_NO_TOTAL || f.bind_length > f.bind_buffer.size()) {
			std::cout << "[DEBUG] HandleTruncatedData - Column " << i << " data truncated, handling..." << std::endl;
			// Data was truncated, we need to handle this case
			// For now, we'll just resize the buffer and re-bind
			size_t new_size = f.bind_length > 0 ? f.bind_length : f.bind_buffer.size() * 2;
			std::cout << "[DEBUG] HandleTruncatedData - Resizing buffer from " << f.bind_buffer.size()
			          << " to " << new_size << std::endl;
			f.bind_buffer.resize(new_size);
			
			SQLRETURN retcode = SQLBindCol(stmt.get(), i + 1,
			                              ODBCTypes::GetCType(f.odbc_type),
			                              f.bind_buffer.data(),
			                              f.bind_buffer.size(),
			                              &f.bind_length);
			
			if (!SQL_SUCCEEDED(retcode)) {
				std::cout << "[ERROR] HandleTruncatedData - Failed to re-bind column " << i << std::endl;
				throw IOException("Failed to re-bind column %zu for ODBC query \"%s\"\n", i, query.c_str());
			}
			std::cout << "[DEBUG] HandleTruncatedData - Column " << i << " re-bound successfully" << std::endl;
		}
	}
	std::cout << "[DEBUG] ODBCResult::HandleTruncatedData() - Completed" << std::endl;
}

DataChunk &ODBCResult::NextChunk() {
	std::cout << "[DEBUG] ODBCResult::NextChunk() - Starting" << std::endl;
	this->data_chunk.Reset();
	this->row_idx = 0;

	idx_t r = 0;
	for (; r < STANDARD_VECTOR_SIZE; r++) {
		std::cout << "[DEBUG] ODBCResult::NextChunk() - Fetching row " << r << std::endl;
		if (!FetchNext()) {
			std::cout << "[DEBUG] ODBCResult::NextChunk() - No more data at row " << r << std::endl;
			break;
		}

		std::cout << "[DEBUG] ODBCResult::NextChunk() - Writing row " << r << " to chunk" << std::endl;
		WriteToChunk(r);
	}

	this->data_chunk.SetCardinality(r);
	std::cout << "[DEBUG] ODBCResult::NextChunk() - Completed, cardinality: " << r << std::endl;
	return this->data_chunk;
}

bool ODBCResult::FetchNext() {
	std::cout << "[DEBUG] ODBCResult::FetchNext() - Starting, fields.size(): " << fields.size() << std::endl;
	
	if (fields.empty()) {
		std::cout << "[DEBUG] ODBCResult::FetchNext() - No fields, returning false" << std::endl;
		return false;
	}
	
	for (auto &f : fields) {
#ifdef DEBUG
		std::memset(f.bind_buffer.data(), '\0', f.bind_buffer.size());
#endif
		f.ResetBind();
	}

	std::cout << "[DEBUG] ODBCResult::FetchNext() - Calling SQLFetch on stmt: " << stmt.get() << std::endl;
	SQLRETURN retcode = SQLFetch(stmt.get());
	std::cout << "[DEBUG] ODBCResult::FetchNext() - SQLFetch returned: " << retcode << std::endl;

	if (retcode == SQL_ERROR) {
		std::cout << "[ERROR] ODBCResult::FetchNext() - SQLFetch failed" << std::endl;
		throw IOException("Failed to fetch result row for ODBC query \"%s\"\n", query.c_str());
	}

	if (retcode == SQL_NO_DATA) {
		std::cout << "[DEBUG] ODBCResult::FetchNext() - No more data (SQL_NO_DATA)" << std::endl;
		return false;
	}

	std::cout << "[DEBUG] ODBCResult::FetchNext() - Handling truncated data" << std::endl;
	HandleTruncatedData();

	bool result = (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO);
	std::cout << "[DEBUG] ODBCResult::FetchNext() - Returning: " << result << std::endl;
	return result;
}

bool ODBCResult::Next() {
	std::cout << "[DEBUG] ODBCResult::Next() - Starting, row_idx: " << row_idx << ", data_chunk.size(): " << data_chunk.size() << std::endl;
	
	if (row_idx < data_chunk.size() - 1) {
		row_idx += 1;
		std::cout << "[DEBUG] ODBCResult::Next() - Incrementing row_idx to: " << row_idx << std::endl;
		return true;
	}
	
	std::cout << "[DEBUG] ODBCResult::Next() - Calling NextChunk()" << std::endl;
	NextChunk();
	row_idx = 0;
	bool result = data_chunk.size() > 0;
	std::cout << "[DEBUG] ODBCResult::Next() - Returning: " << result << std::endl;
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

static void WriteString(ODBCField &f, Vector &vec, idx_t row) {
	std::cout << "[DEBUG] WriteString - Starting, field: " << f.name
	          << ", odbc_type: " << f.odbc_type
	          << ", bind_buffer.size(): " << f.bind_buffer.size()
	          << ", bind_length: " << f.bind_length << std::endl;
	
	if (f.odbc_type == SQL_TYPE_TIME) {
		std::cout << "[DEBUG] WriteString - Writing time as string" << std::endl;
		WriteTimeAsString(f, vec, row);
		return;
	}

	if (f.bind_buffer.empty()) {
		std::cout << "[ERROR] WriteString - bind_buffer is empty for field: " << f.name << std::endl;
		throw InternalException("Empty bind buffer in WriteString");
	}
	
	if (f.bind_length < 0) {
		std::cout << "[DEBUG] WriteString - bind_length is negative (" << f.bind_length << "), treating as NULL" << std::endl;
		FlatVector::SetNull(vec, row, true);
		return;
	}
	
	if (f.bind_length > f.bind_buffer.size()) {
		std::cout << "[WARNING] WriteString - bind_length (" << f.bind_length
		          << ") exceeds buffer size (" << f.bind_buffer.size()
		          << ") for field: " << f.name << std::endl;
		// Truncate to buffer size
		f.bind_length = f.bind_buffer.size();
	}
	
	std::cout << "[DEBUG] WriteString - Creating string_t with length: " << f.bind_length << std::endl;
	string_t st(f.bind_buffer.data(), f.bind_length);
	auto data = FlatVector::GetData<string_t>(vec);
	std::cout << "[DEBUG] WriteString - Adding string to vector" << std::endl;
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
	std::cout << "[DEBUG] WriteString - Completed" << std::endl;
}

static void WriteBool(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(int8_t));
	auto data = FlatVector::GetData<bool>(vec);
	if (f.odbc_type == SQL_TINYINT || f.odbc_type == SQL_BIT) {
		int8_t val = *reinterpret_cast<int8_t *>(f.bind_buffer.data());
		data[row] = val != 0;
		return;
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
	std::cout << "[DEBUG] ODBCResult::WriteToChunk() - Starting, row: " << row
	          << ", data_chunk.ColumnCount(): " << data_chunk.ColumnCount()
	          << ", fields.size(): " << fields.size() << std::endl;
	
	for (idx_t col = 0; col < data_chunk.ColumnCount(); col++) {
		std::cout << "[DEBUG] ODBCResult::WriteToChunk() - Processing column " << col << std::endl;
		
		if (col >= fields.size()) {
			std::cout << "[ERROR] ODBCResult::WriteToChunk() - Column index " << col << " out of bounds, fields.size(): " << fields.size() << std::endl;
			throw InternalException("Column index out of bounds in WriteToChunk");
		}
		
		ODBCField &f = fields[col];
		Vector &vec = data_chunk.data[col];

		std::cout << "[DEBUG] ODBCResult::WriteToChunk() - Column " << col << ": name=" << f.name
		          << ", odbc_type=" << f.odbc_type << ", duckdb_type=" << f.duckdb_type.ToString()
		          << ", bind_is_null=" << f.bind_is_null << std::endl;

		if (f.bind_is_null) {
			std::cout << "[DEBUG] ODBCResult::WriteToChunk() - Setting column " << col << " to NULL" << std::endl;
			FlatVector::SetNull(vec, row, true);
			continue;
		}

		std::cout << "[DEBUG] ODBCResult::WriteToChunk() - Writing column " << col << " data" << std::endl;
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
			WriteNumber<int32_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::UINTEGER: {
			WriteNumber<uint32_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::BIGINT: {
			WriteNumber<int64_t>(f, vec, row);
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
