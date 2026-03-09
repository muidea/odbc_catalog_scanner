#include "include/odbc_result.hpp"
#include "odbc_connection.hpp"
#include "storage/odbc_connection_pool.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "odbc_types.hpp"
#include "../include/encoding_utils.hpp"
#include <cstdint>
#include <string>
#include <cstring>
#include <climits>
#include <cstdio>
#include <cmath>
#include <thread>
#include <codecvt>
#include <locale>

namespace duckdb {

static const size_t DEFAULT_BIND_BUFFER_SIZE = 256;
static const size_t MAX_BIND_BUFFER_SIZE = 65536;
static const size_t DEFAULT_WIDE_BIND_BUFFER_CHARS = 256;

static string UTF16UnitsToUTF8(const SQLWCHAR *data, size_t code_units) {
	if (!data || code_units == 0) {
		return string();
	}
	std::u16string utf16;
	utf16.reserve(code_units);
	for (size_t i = 0; i < code_units; i++) {
		utf16.push_back(static_cast<char16_t>(data[i]));
	}
	std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
	return converter.to_bytes(utf16);
}

static size_t GetBufferSize(SQLSMALLINT odbc_type, SQLULEN column_size, const ODBCTypeConfig &type_config) {
	switch (odbc_type) {
	case SQL_TINYINT:
		return sizeof(uint8_t);
	case SQL_BIT:
		return sizeof(uint8_t);
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
	case SQL_DATE:
		return sizeof(SQL_TIMESTAMP_STRUCT);
	case SQL_TYPE_TIME:
	case SQL_TIME:
		return sizeof(SQL_TIMESTAMP_STRUCT);
	case SQL_TYPE_TIMESTAMP:
	case SQL_TIMESTAMP:
		return sizeof(SQL_TIMESTAMP_STRUCT);
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		return 64;
	case SQL_GUID:
		return 16;
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE) {
			return std::max(static_cast<size_t>(column_size + 1), static_cast<size_t>(256));
		}
		return 256;
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE / sizeof(SQLWCHAR)) {
			return std::max(static_cast<size_t>((column_size + 1) * sizeof(SQLWCHAR)),
			                static_cast<size_t>(DEFAULT_WIDE_BIND_BUFFER_CHARS * sizeof(SQLWCHAR)));
		}
		return DEFAULT_WIDE_BIND_BUFFER_CHARS * sizeof(SQLWCHAR);
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE) {
			return static_cast<size_t>(column_size);
		}
		return 256;
	default:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE) {
			return static_cast<size_t>(column_size + 1);
		}
		return 256;
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
		
		// Validate column name
		if (name_len <= 0 || name_len >= sizeof(col_name)) {
			throw IOException("Invalid column name length %d for column %d in ODBC query \"%s\"\n", 
			                  name_len, i + 1, query.c_str());
		}
		
		// Ensure col_name is null-terminated
		col_name[name_len] = '\0';
		col_name[sizeof(col_name) - 1] = '\0';
		
		// Convert ODBC type to logical type
		LogicalType ltype = ODBCTypes::ODBCToLogicalType(type_config, data_type, column_size, decimal_digits);
		
		// Create field with placeholder information
		ODBCField field(nullptr, std::move(ltype), type_config);
		
		// Read column name and validate
		string col_name_str(reinterpret_cast<char*>(col_name), name_len);
		
		// Convert GBK to UTF-8 on Windows
		col_name_str = GBKToUTF8(col_name_str);
		
		// Check if column name is empty or contains null characters - if so, use placeholder name
		if (col_name_str.empty() || col_name_str.find('\0') != string::npos) {
			// Use placeholder name to avoid catalog errors
			col_name_str = "column_" + std::to_string(i + 1);
		}
		
		field.name = col_name_str;
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
	cached_chunks.clear();

	if (fields.empty()) {
		return;
	}

	auto ltypes = CreateChunkTypes(fields, type_config);

	while (true) {
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::DefaultAllocator(), ltypes);
		data_chunk.Reference(*chunk);

		idx_t r = 0;
		for (; r < STANDARD_VECTOR_SIZE; r++) {
			if (!FetchNext()) {
				break;
			}
			WriteToChunk(r);
		}

		if (r == 0) {
			break;
		}

		chunk->SetCardinality(r);
		cached_chunks.push_back(std::move(chunk));

		if (r < STANDARD_VECTOR_SIZE) {
			break;
		}
	}

	data_chunk.SetCardinality(0);
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
		if (c_type == SQL_C_CHAR || c_type == SQL_C_BINARY || c_type == SQL_C_WCHAR) {
			size_t already_have = f.bind_buffer.size();
			if ((c_type == SQL_C_CHAR || c_type == SQL_C_WCHAR) && already_have > 0) {
				already_have--; // null terminator doesn't count
			}

			size_t resize_target = total_len + 1;
			if (c_type == SQL_C_WCHAR) {
				resize_target = total_len + sizeof(SQLWCHAR);
			}
			f.bind_buffer.resize(resize_target);

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

static void WriteTimeAsString(ODBCField &f, Vector &vec, idx_t row) {
	SQLSMALLINT c_type = ODBCTypes::GetCType(f.odbc_type);

	if (c_type == SQL_C_CHAR) {
		size_t len = (f.bind_length >= 0 && static_cast<size_t>(f.bind_length) <= f.bind_buffer.size())
		                 ? static_cast<size_t>(f.bind_length)
		                 : 0;
		if (len == 0) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		string time_str(f.bind_buffer.data(), len);
		auto data = FlatVector::GetData<string_t>(vec);
		data[row] = StringVector::AddStringOrBlob(vec, time_str);
		return;
	}

	if (f.bind_buffer.size() < sizeof(SQL_TIMESTAMP_STRUCT)) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	SQL_TIMESTAMP_STRUCT *ts = reinterpret_cast<SQL_TIMESTAMP_STRUCT *>(f.bind_buffer.data());
	if (ts->hour == 0 && ts->minute == 0 && ts->second == 0 && ts->fraction == 0) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
	string time_str = Time::ToString(tm);
	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, time_str);
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

static void WriteNumericAsString(ODBCField &f, Vector &vec, idx_t row) {
	string str_val;
	switch (f.odbc_type) {
	case SQL_TINYINT: {
		uint8_t val = *reinterpret_cast<uint8_t *>(f.bind_buffer.data());
		str_val = std::to_string(val);
		break;
	}
	case SQL_SMALLINT: {
		int16_t val = *reinterpret_cast<int16_t *>(f.bind_buffer.data());
		str_val = std::to_string(val);
		break;
	}
	case SQL_INTEGER: {
		int32_t val = *reinterpret_cast<int32_t *>(f.bind_buffer.data());
		str_val = std::to_string(val);
		break;
	}
	case SQL_BIGINT: {
		int64_t val = *reinterpret_cast<int64_t *>(f.bind_buffer.data());
		str_val = std::to_string(val);
		break;
	}
	case SQL_REAL: {
		float val = *reinterpret_cast<float *>(f.bind_buffer.data());
		str_val = std::to_string(val);
		break;
	}
	case SQL_FLOAT:
	case SQL_DOUBLE: {
		double val = *reinterpret_cast<double *>(f.bind_buffer.data());
		str_val = std::to_string(val);
		break;
	}
	case SQL_BIT: {
		uint8_t val = *reinterpret_cast<uint8_t *>(f.bind_buffer.data());
		str_val = val ? "true" : "false";
		break;
	}
	default:
		str_val = string(f.bind_buffer.data(), std::max(static_cast<SQLLEN>(0), f.bind_length));
		break;
	}

	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, str_val);
}

static bool IsNumericODBCType(SQLSMALLINT odbc_type) {
	return odbc_type == SQL_INTEGER || odbc_type == SQL_BIGINT || odbc_type == SQL_SMALLINT ||
	       odbc_type == SQL_TINYINT || odbc_type == SQL_REAL || odbc_type == SQL_FLOAT || odbc_type == SQL_DOUBLE ||
	       odbc_type == SQL_BIT;
}

static void WriteString(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(vec.GetType().id() == LogicalTypeId::VARCHAR || vec.GetType().id() == LogicalTypeId::BLOB);

	if (f.odbc_type == SQL_TYPE_TIME || f.odbc_type == SQL_TIME) {
		WriteTimeAsString(f, vec, row);
		return;
	}

	if (f.bind_buffer.empty() || f.bind_length < 0) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	if (IsNumericODBCType(f.odbc_type)) {
		WriteNumericAsString(f, vec, row);
		return;
	}

	SQLSMALLINT c_type = ODBCTypes::GetCType(f.odbc_type);
	if (c_type == SQL_C_WCHAR) {
		if (f.bind_buffer.size() < sizeof(SQLWCHAR) || f.bind_length <= 0) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		size_t byte_len = static_cast<size_t>(f.bind_length);
		if (byte_len > f.bind_buffer.size()) {
			byte_len = f.bind_buffer.size();
		}
		size_t unit_len = byte_len / sizeof(SQLWCHAR);
		auto *wdata = reinterpret_cast<const SQLWCHAR *>(f.bind_buffer.data());
		while (unit_len > 0 && wdata[unit_len - 1] == 0) {
			unit_len--;
		}
		if (unit_len == 0) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		string result_str = UTF16UnitsToUTF8(wdata, unit_len);
		auto data = FlatVector::GetData<string_t>(vec);
		data[row] = StringVector::AddStringOrBlob(vec, result_str);
		return;
	}

	// SQL_GUID bound as SQL_C_BINARY: format the 16-byte GUID as a string
	if (f.odbc_type == SQL_GUID) {
		if (f.bind_length >= 16) {
			auto *b = reinterpret_cast<const uint8_t *>(f.bind_buffer.data());
			char guid_str[37];
			snprintf(guid_str, sizeof(guid_str),
			         "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X",
			         b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6],
			         b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
			auto data = FlatVector::GetData<string_t>(vec);
			data[row] = StringVector::AddStringOrBlob(vec, string(guid_str, 36));
		} else {
			FlatVector::SetNull(vec, row, true);
		}
		return;
	}

	size_t actual_len;
	if (f.bind_length >= 0 && static_cast<size_t>(f.bind_length) <= f.bind_buffer.size()) {
		actual_len = static_cast<size_t>(f.bind_length);
	} else if (f.bind_length > 0) {
		actual_len = f.bind_buffer.size();
	} else {
		actual_len = 0;
	}

	// Strip all trailing null bytes
	while (actual_len > 0 && f.bind_buffer[actual_len - 1] == '\0') {
		actual_len--;
	}

	// For character (non-BLOB) data, truncate at the first embedded null byte.
	// This protects against drivers that report UTF-16 byte counts for
	// SQL_C_CHAR bindings, which would leave embedded null bytes.
	if (vec.GetType().id() == LogicalTypeId::VARCHAR && actual_len > 0) {
		auto *null_pos = static_cast<const char *>(memchr(f.bind_buffer.data(), '\0', actual_len));
		if (null_pos) {
			actual_len = static_cast<size_t>(null_pos - f.bind_buffer.data());
		}
	}

	string raw_str(f.bind_buffer.data(), actual_len);
	string result_str = GBKToUTF8(raw_str);

	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, result_str);
}

static void WriteBool(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(!f.bind_buffer.empty());
	auto data = FlatVector::GetData<bool>(vec);

	if (f.odbc_type == SQL_BIT || f.odbc_type == SQL_TINYINT) {
		uint8_t val = *reinterpret_cast<uint8_t *>(f.bind_buffer.data());
		data[row] = (val != 0);
		return;
	}

	if (f.bind_length <= 0) {
		data[row] = false;
		return;
	}

	char first = f.bind_buffer[0];
	data[row] = (first != '\0' && first != '0' && first != 'f' && first != 'F');
}

template <typename NUM_TYPE>
static void WriteNumber(ODBCField &f, Vector &vec, idx_t row) {
	
	D_ASSERT(f.bind_buffer.size() >= sizeof(NUM_TYPE));
	NUM_TYPE num = *reinterpret_cast<NUM_TYPE *>(f.bind_buffer.data());
	
	
	auto data = FlatVector::GetData<NUM_TYPE>(vec);
	data[row] = num;
}

static void WriteDateTime(ODBCField &f, Vector &vec, idx_t row) {
	SQLSMALLINT c_type = ODBCTypes::GetCType(f.odbc_type);

	// SQL_SS_TIME2 / SQL_SS_TIMESTAMPOFFSET / SQL_GUID(time) are bound as SQL_C_CHAR
	if (c_type == SQL_C_CHAR) {
		size_t len = (f.bind_length >= 0 && static_cast<size_t>(f.bind_length) <= f.bind_buffer.size())
		                 ? static_cast<size_t>(f.bind_length)
		                 : 0;
		if (len == 0) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		string str_val(f.bind_buffer.data(), len);
		switch (vec.GetType().id()) {
		case LogicalTypeId::TIME: {
			dtime_t val;
			idx_t pos;
			if (Time::TryConvertTime(str_val.c_str(), len, pos, val)) {
				FlatVector::GetData<dtime_t>(vec)[row] = val;
			} else {
				FlatVector::SetNull(vec, row, true);
			}
			return;
		}
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			timestamp_t val;
			bool use_offset = (vec.GetType().id() == LogicalTypeId::TIMESTAMP_TZ);
			if (Timestamp::TryConvertTimestamp(str_val.c_str(), len, val, use_offset) ==
			    TimestampCastResult::SUCCESS) {
				if (use_offset) {
					FlatVector::GetData<timestamp_tz_t>(vec)[row] = timestamp_tz_t(val.value);
				} else {
					FlatVector::GetData<timestamp_t>(vec)[row] = val;
				}
			} else {
				FlatVector::SetNull(vec, row, true);
			}
			return;
		}
		case LogicalTypeId::DATE: {
			date_t val;
			idx_t pos;
			bool special = false;
			if (Date::TryConvertDate(str_val.c_str(), len, pos, val, special) == DateCastResult::SUCCESS) {
				FlatVector::GetData<date_t>(vec)[row] = val;
			} else {
				FlatVector::SetNull(vec, row, true);
			}
			return;
		}
		default:
			throw InternalException("WriteDateTime: unexpected vector type %s for SQL_C_CHAR bound column %s",
			                        vec.GetType().ToString(), f.name);
		}
	}

	if (f.bind_buffer.size() < sizeof(SQL_TIMESTAMP_STRUCT)) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	SQL_TIMESTAMP_STRUCT *ts = reinterpret_cast<SQL_TIMESTAMP_STRUCT *>(f.bind_buffer.data());

	bool is_zero = (ts->year == 0 && ts->month == 0 && ts->day == 0 && ts->hour == 0 && ts->minute == 0 &&
	                ts->second == 0 && ts->fraction == 0);
	if (is_zero) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	switch (vec.GetType().id()) {
	case LogicalTypeId::DATE: {
		date_t val = Date::FromDate(ts->year, ts->month, ts->day);
		FlatVector::GetData<date_t>(vec)[row] = val;
		break;
	}
	case LogicalTypeId::TIME: {
		dtime_t val = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		FlatVector::GetData<dtime_t>(vec)[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		date_t dt = Date::FromDate(ts->year, ts->month, ts->day);
		dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		FlatVector::GetData<timestamp_t>(vec)[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		date_t dt = Date::FromDate(ts->year, ts->month, ts->day);
		dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		FlatVector::GetData<timestamp_tz_t>(vec)[row] = timestamp_tz_t(val.value);
		break;
	}
	default:
		throw InternalException("WriteDateTime: unexpected vector type %s for timestamp-struct bound column %s",
		                        vec.GetType().ToString(), f.name);
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
			} else if (f.odbc_type == SQL_REAL) {
				D_ASSERT(f.bind_buffer.size() >= sizeof(float));
				float float_val = *reinterpret_cast<float *>(f.bind_buffer.data());
				FlatVector::GetData<double>(vec)[row] = static_cast<double>(float_val);
			} else {
				size_t len = (f.bind_length >= 0 && static_cast<size_t>(f.bind_length) <= f.bind_buffer.size())
				                 ? static_cast<size_t>(f.bind_length)
				                 : 0;
				if (len == 0) {
					FlatVector::SetNull(vec, row, true);
				} else {
					string str_val(f.bind_buffer.data(), len);
					try {
						double val = std::stod(str_val);
						FlatVector::GetData<double>(vec)[row] = val;
					} catch (...) {
						FlatVector::SetNull(vec, row, true);
					}
				}
			}
			break;
		}
		case LogicalTypeId::DECIMAL: {
			WriteDecimal(f, vec, row);
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
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			WriteDateTime(f, vec, row);
			break;
		}
		default:
			throw InternalException("WriteToChunk: unsupported vector type %s for column %s",
			                        vec.GetType().ToString(), f.name);
		}
	}
}



} // namespace duckdb
