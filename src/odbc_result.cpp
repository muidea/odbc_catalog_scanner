#include "query_odbc_source_error.hpp"
#include "include/odbc_result.hpp"
#include "odbc_connection.hpp"
#include "storage/odbc_connection_pool.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/function/cast/default_casts.hpp"
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
#include <codecvt>
#include <locale>

namespace duckdb {

static const size_t DEFAULT_BIND_BUFFER_SIZE = 64;
static const size_t MAX_BIND_BUFFER_SIZE = 65536;

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

static size_t GetBufferSize(SQLSMALLINT c_type, SQLULEN column_size) {
	switch (c_type) {
	case SQL_C_STINYINT:
	case SQL_C_UTINYINT:
	case SQL_C_BIT:
		return sizeof(int8_t);
	case SQL_C_SSHORT:
	case SQL_C_USHORT:
		return sizeof(int16_t);
	case SQL_C_SLONG:
	case SQL_C_ULONG:
		return sizeof(int32_t);
	case SQL_C_FLOAT:
		return sizeof(float);
	case SQL_C_DOUBLE:
		return sizeof(double);
	case SQL_C_SBIGINT:
	case SQL_C_UBIGINT:
		return sizeof(int64_t);
	case SQL_C_TYPE_DATE:
		return sizeof(SQL_DATE_STRUCT);
	case SQL_C_TYPE_TIME:
		return sizeof(SQL_TIME_STRUCT);
	case SQL_C_TYPE_TIMESTAMP:
		return sizeof(SQL_TIMESTAMP_STRUCT);
	case SQL_C_WCHAR:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE / sizeof(SQLWCHAR)) {
			return static_cast<size_t>((column_size + 1) * sizeof(SQLWCHAR));
		}
		return DEFAULT_BIND_BUFFER_SIZE * sizeof(SQLWCHAR);
	case SQL_C_CHAR:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE) {
			return static_cast<size_t>(column_size + 1);
		}
		return DEFAULT_BIND_BUFFER_SIZE;
	default:
		if (column_size > 0 && column_size <= MAX_BIND_BUFFER_SIZE) {
			return static_cast<size_t>(column_size);
		}
		return DEFAULT_BIND_BUFFER_SIZE;
	}
}

static size_t GetBoundPayloadCapacity(const ODBCField &field) {
	auto capacity = field.bind_buffer.size();
	if (field.c_type == SQL_C_CHAR && capacity > 0) {
		return capacity - 1;
	}
	if (field.c_type == SQL_C_WCHAR && capacity >= sizeof(SQLWCHAR)) {
		return capacity - sizeof(SQLWCHAR);
	}
	return capacity;
}

ODBCField::ODBCField(void *field_info, LogicalType duckdb_type_p, const ODBCTypeConfig &type_config)
    : duckdb_type(std::move(duckdb_type_p)) {
	// For ODBC, we need to extract field information differently
	// This will be implemented in the ReadFields function
	bind_buffer.resize(DEFAULT_BIND_BUFFER_SIZE);
}

// A CLI driver may write a 32-bit SQLLEN through the driver manager's
// 64-bit slot. Poison padding proves that width without mistaking narrow -1
// (NULL) for a multi-gigabyte positive value. Never accept an unwritten slot.
static SQLLEN UnwrittenIndicator() {
	SQLLEN value;
	std::memset(&value, 0xa5, sizeof(value));
	return value;
}

static SQLLEN DecodeIndicator(SQLLEN value) {
	if (value == UnwrittenIndicator()) {
		throw IOException("ODBC driver did not write a result length indicator");
	}
	if (sizeof(SQLLEN) == 8) {
		auto bytes = reinterpret_cast<const unsigned char *>(&value);
		bool narrow = true;
		for (idx_t i = 4; i < 8; i++) {
			narrow = narrow && bytes[i] == 0xa5;
		}
		if (narrow) {
			int32_t low;
			std::memcpy(&low, bytes, sizeof(low));
			value = low;
		}
	}
	if (value < 0 && value != SQL_NULL_DATA && value != SQL_NO_TOTAL) {
		throw IOException("ODBC driver returned an invalid result length indicator");
	}
	return value;
}

void ODBCField::ResetBind() {
	this->bind_is_null = false;
	this->bind_length = UnwrittenIndicator();
	this->bind_error = false;
	this->value_buffer.clear();
}

const char *ODBCField::ValueData() const {
	return value_buffer.empty() ? bind_buffer.data() : value_buffer.data();
}

size_t ODBCField::ValueBufferSize() const {
	return value_buffer.empty() ? bind_buffer.size() : value_buffer.size();
}

static bool TransferNumericAsText(const string &type_name) {
	auto normalized_type_name = StringUtil::Lower(type_name);
	StringUtil::Trim(normalized_type_name);
	return normalized_type_name == "decimal" || normalized_type_name == "numeric" ||
	       normalized_type_name == "smalldecimal" || normalized_type_name == "decfloat" ||
	       normalized_type_name == "money" || normalized_type_name == "smallmoney";
}

static vector<ODBCField> ReadFields(const std::string &query, SQLHSTMT stmt, const ODBCTypeConfig &type_config,
                                    SQLHDBC source_connection) {
	vector<ODBCField> fields;
	SQLSMALLINT num_cols;
	SQLRETURN retcode = SQLNumResultCols(stmt, &num_cols);

	if (!SQL_SUCCEEDED(retcode)) {
		ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, source_connection, "source_describe", query);
	}
	if (num_cols == 0) {
		return fields;
	}

	fields.reserve(num_cols);
	for (SQLSMALLINT i = 0; i < num_cols; i++) {
		SQLCHAR col_name[256];
		SQLCHAR type_name_buffer[256] = {};
		SQLSMALLINT name_len = 0, data_type = 0, decimal_digits = 0, nullable = 0;
		SQLSMALLINT type_name_len = 0;
		// Some deployed CLI drivers write a 32-bit size through SQLULEN*.
		// Zero the complete output slot so high stack bytes cannot change an
		// exact DECIMAL descriptor into the out-of-range DOUBLE fallback.
		SQLULEN column_size = 0;
		SQLLEN unused_numeric_attribute = 0;

		retcode = SQLDescribeCol(stmt, i + 1, col_name, sizeof(col_name), &name_len, &data_type, &column_size,
		                         &decimal_digits, &nullable);

		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, source_connection, "source_describe", query);
		}

		string type_name;
		retcode = SQLColAttribute(stmt, i + 1, SQL_DESC_TYPE_NAME, type_name_buffer, sizeof(type_name_buffer),
		                          &type_name_len, &unused_numeric_attribute);
		if (SQL_SUCCEEDED(retcode) && type_name_len > 0) {
			type_name = string(reinterpret_cast<char *>(type_name_buffer), type_name_len);
		}

		LogicalType ltype =
		    ODBCTypes::ODBCToLogicalType(type_config, data_type, column_size, decimal_digits, type_name);
		// SQL_TINYINT is not uniformly unsigned across ODBC drivers. SQL Server
		// reports it as unsigned, while Dameng reports a signed tinyint. Binding a
		// signed source as SQL_C_UTINYINT makes the driver reject negative values
		// (and some boundary values) during SQLFetch with "Data out of range".
		// Prefer the driver's descriptor when it is available; retain the legacy
		// unsigned fallback for drivers that do not expose SQL_DESC_UNSIGNED.
		SQLLEN unsigned_attribute = SQL_TRUE;
		bool tinyint_is_unsigned = true;
		if (data_type == SQL_TINYINT) {
			retcode = SQLColAttribute(stmt, i + 1, SQL_DESC_UNSIGNED, nullptr, 0, nullptr, &unsigned_attribute);
			if (SQL_SUCCEEDED(retcode)) {
				tinyint_is_unsigned = unsigned_attribute != SQL_FALSE;
			}
			if (!tinyint_is_unsigned) {
				ltype = LogicalType::TINYINT;
			}
		}

		// Create field with placeholder information
		ODBCField field(nullptr, std::move(ltype), type_config);
		field.name = string(reinterpret_cast<char *>(col_name), name_len);
		field.type_name = type_name;
		field.odbc_type = data_type;
		field.c_type = ODBCTypes::GetCType(data_type);
		if (data_type == SQL_TINYINT && !tinyint_is_unsigned) {
			field.c_type = SQL_C_STINYINT;
		}
		auto lower_type_name = StringUtil::Lower(type_name);
		if (ltype.id() == LogicalTypeId::TIMESTAMP) {
			field.c_type = SQL_C_TYPE_TIMESTAMP;
		} else if (lower_type_name == "datetimeoffset") {
			// Preserve the UTC offset text; SQL_TIMESTAMP_STRUCT has no offset field.
			field.c_type = SQL_C_CHAR;
		} else if (ltype.id() == LogicalTypeId::DECIMAL || TransferNumericAsText(lower_type_name)) {
			field.c_type = SQL_C_CHAR;
		} else if (ltype.id() == LogicalTypeId::FLOAT) {
			field.c_type = SQL_C_FLOAT;
		} else if (ltype.id() == LogicalTypeId::DOUBLE) {
			field.c_type = SQL_C_DOUBLE;
		} else if (ltype.id() == LogicalTypeId::BLOB) {
			field.c_type = SQL_C_BINARY;
		}

		// Resize buffer based on ODBC type
		size_t buf_size = GetBufferSize(field.c_type, column_size);
		if (field.c_type == SQL_C_CHAR && TransferNumericAsText(lower_type_name)) {
			// Precision counts digits only. Driver text may also contain a sign,
			// decimal point and a leading zero when scale == precision. Reserving
			// a small fixed minimum avoids truncating valid DECIMAL values or
			// relying on SQLGetData continuation for already-bound numeric data.
			buf_size = MaxValue<size_t>(buf_size, DEFAULT_BIND_BUFFER_SIZE);
		}
		field.bind_buffer.resize(buf_size);

		fields.emplace_back(std::move(field));
	}
	return fields;
}

static void InitBinds(vector<ODBCField> &fields, SQLHSTMT stmt, SQLHDBC source_connection, const string &query) {
	for (SQLSMALLINT i = 0; i < fields.size(); i++) {
		ODBCField &f = fields[i];
		SQLRETURN retcode =
		    SQLBindCol(stmt, i + 1, f.c_type, f.bind_buffer.data(), f.bind_buffer.size(), &f.bind_length);

		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, source_connection, "source_bind", query);
		}
	}
}

static size_t TextPayloadLength(const vector<char> &buffer, SQLSMALLINT c_type) {
	if (c_type == SQL_C_CHAR) {
		return strnlen(buffer.data(), buffer.size());
	}
	if (c_type == SQL_C_WCHAR) {
		auto units = buffer.size() / sizeof(SQLWCHAR);
		auto data = reinterpret_cast<const SQLWCHAR *>(buffer.data());
		idx_t offset = 0;
		while (offset < units && data[offset] != 0) {
			offset++;
		}
		return offset * sizeof(SQLWCHAR);
	}
	return 0;
}

static void ReadUnboundVariableData(vector<ODBCField> &fields, SQLHSTMT stmt, const string &query,
                                    SQLHDBC source_connection) {
	static constexpr size_t CHUNK_SIZE = 8192;
	static constexpr size_t MAX_VALUE_SIZE = MAX_BIND_BUFFER_SIZE * 16;
	vector<char> buffer(CHUNK_SIZE);
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &field = fields[i];
		if (field.column_bound) {
			continue;
		}
		field.value_buffer.clear();
		while (true) {
			memset(buffer.data(), 0, buffer.size());
			SQLLEN indicator = UnwrittenIndicator();
			auto retcode = SQLGetData(stmt, static_cast<SQLUSMALLINT>(i + 1), field.c_type, buffer.data(),
			                          static_cast<SQLLEN>(buffer.size()), &indicator);
			if (SQL_SUCCEEDED(retcode)) {
				indicator = DecodeIndicator(indicator);
			}
			if (indicator == SQL_NULL_DATA) {
				field.bind_length = SQL_NULL_DATA;
				field.value_buffer.clear();
				break;
			}
			if (retcode == SQL_NO_DATA) {
				break;
			}
			if (!SQL_SUCCEEDED(retcode)) {
				ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, source_connection, "source_fetch", query);
			}
			size_t payload = 0;
			if (field.c_type == SQL_C_BINARY) {
				payload = retcode == SQL_SUCCESS_WITH_INFO
				              ? buffer.size()
				              : MinValue<size_t>(indicator < 0 ? 0 : static_cast<size_t>(indicator), buffer.size());
			} else {
				payload = TextPayloadLength(buffer, field.c_type);
			}
			if (field.value_buffer.size() + payload > MAX_VALUE_SIZE) {
				throw IOException("Variable-width column %zu exceeds the %zu byte ODBC safety limit for query \"%s\"",
				                  i + 1, MAX_VALUE_SIZE, query.c_str());
			}
			field.value_buffer.insert(field.value_buffer.end(), buffer.data(), buffer.data() + payload);
			if (retcode == SQL_SUCCESS) {
				break;
			}
			if (payload == 0) {
				throw IOException("ODBC driver made no progress reading variable-width column %zu for query \"%s\"",
				                  i + 1, query.c_str());
			}
		}
		if (field.bind_length != SQL_NULL_DATA) {
			field.bind_length = static_cast<SQLLEN>(field.value_buffer.size());
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
      connection(std::move(connection_p)), is_streaming(streaming_p == ODBCResultStreaming::ALLOW_STREAMING) {

	if (!stmt) {
		throw InternalException("ODBCResult: Statement handle is null for query \"%s\"\n", query_p.c_str());
	}

	if (affected_rows != static_cast<idx_t>(-1)) {
		stmt.reset();
		if (connection && !connection_released) {
			connection->ReleaseConnection();
			connection_released = true;
		}
		return;
	}

	try {
		this->fields = ReadFields(query, stmt.get(), type_config, connection ? connection->GetConn() : SQL_NULL_HDBC);

		if (fields.empty()) {
			SQLSMALLINT num_cols;
			SQLRETURN retcode = SQLNumResultCols(stmt.get(), &num_cols);
			if (SQL_SUCCEEDED(retcode) && num_cols > 0) {
				this->fields =
				    ReadFields(query, stmt.get(), type_config, connection ? connection->GetConn() : SQL_NULL_HDBC);
			}
		}

		if (!fields.empty()) {
			InitBinds(fields, stmt.get(), connection ? connection->GetConn() : SQL_NULL_HDBC, query);
			auto ltypes = CreateChunkTypes(fields, type_config);
			this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);

			if (!is_streaming) {
				ReadAllData();
			}
		}

		if (!is_streaming) {
			stmt.reset();
			if (connection && !connection_released) {
				connection->ReleaseConnection();
				connection_released = true;
			}
		}
	} catch (...) {
		// Close before bound buffers are destroyed. QueryInternal releases busy
		// for all construction failures, including member initializers.
		stmt.reset();
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

	// 循环读取所有数据，直到没有更多行
	while (true) {
		// 创建一个新的数据块
		auto chunk = make_uniq<DataChunk>();
		auto ltypes = CreateChunkTypes(fields, type_config);
		chunk->Initialize(Allocator::DefaultAllocator(), ltypes);

		idx_t r = 0;
		for (; r < STANDARD_VECTOR_SIZE; r++) {
			if (!FetchNext()) {
				break;
			}

			WriteToChunk(*chunk, r);
		}

		if (r == 0) {
			// 没有读取到任何数据，结束循环
			break;
		}

		// 设置数据块大小并添加到缓存
		chunk->SetChildCardinality(r);
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
	static constexpr size_t MAX_VALUE_SIZE = MAX_BIND_BUFFER_SIZE * 16;
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &field = fields[i];
		if (!field.column_bound || field.bind_length == SQL_NULL_DATA) {
			continue;
		}
		auto capacity = GetBoundPayloadCapacity(field);
		if (field.bind_length != SQL_NO_TOTAL &&
		    (field.bind_length <= 0 || static_cast<size_t>(field.bind_length) <= capacity)) {
			continue;
		}
		if (field.c_type != SQL_C_CHAR && field.c_type != SQL_C_WCHAR && field.c_type != SQL_C_BINARY) {
			throw IOException("ODBC driver truncated a fixed-width column %zu", i + 1);
		}
		if (field.bind_length > static_cast<SQLLEN>(MAX_VALUE_SIZE)) {
			throw IOException("ODBC column %zu exceeds the %zu byte safety limit", i + 1, MAX_VALUE_SIZE);
		}
		// SQLBindCol 的地址在整个游标期间保持稳定，后续片段仅进入结果所有的 value_buffer。
		field.value_buffer.assign(field.bind_buffer.data(), field.bind_buffer.data() + capacity);
		vector<char> buffer(8192);
		while (true) {
			std::fill(buffer.begin(), buffer.end(), 0);
			SQLLEN indicator = UnwrittenIndicator();
			auto status = SQLGetData(stmt.get(), static_cast<SQLUSMALLINT>(i + 1), field.c_type, buffer.data(),
			                         static_cast<SQLLEN>(buffer.size()), &indicator);
			if (SQL_SUCCEEDED(status)) {
				indicator = DecodeIndicator(indicator);
			}
			if (status == SQL_NO_DATA) {
				break;
			}
			if (!SQL_SUCCEEDED(status) || indicator == SQL_NULL_DATA) {
				ThrowQueryODBCError(SQL_HANDLE_STMT, stmt.get(), connection ? connection->GetConn() : SQL_NULL_HDBC,
				                    "source_fetch", query);
			}
			size_t length;
			if (field.c_type == SQL_C_BINARY) {
				if (status == SQL_SUCCESS && indicator < 0) {
					throw IOException("ODBC driver returned unknown final binary length for column %zu", i + 1);
				}
				length = status == SQL_SUCCESS_WITH_INFO
				             ? buffer.size()
				             : MinValue<size_t>(static_cast<size_t>(indicator), buffer.size());
			} else {
				length = TextPayloadLength(buffer, field.c_type);
			}
			if (field.value_buffer.size() + length > MAX_VALUE_SIZE) {
				throw IOException("ODBC column %zu exceeds the %zu byte safety limit", i + 1, MAX_VALUE_SIZE);
			}
			field.value_buffer.insert(field.value_buffer.end(), buffer.data(), buffer.data() + length);
			if (status == SQL_SUCCESS) {
				break;
			}
			if (length == 0) {
				throw IOException("ODBC driver made no progress completing column %zu", i + 1);
			}
		}
		if (field.bind_length > 0 && field.value_buffer.size() != static_cast<size_t>(field.bind_length)) {
			throw IOException("ODBC driver returned an incomplete value for column %zu", i + 1);
		}
		field.bind_length = static_cast<SQLLEN>(field.value_buffer.size());
	}
}

DataChunk &ODBCResult::NextChunk() {
	if (is_streaming) {
		data_chunk.Reset();

		if (stream_exhausted) {
			data_chunk.SetChildCardinality(0);
			return data_chunk;
		}

		idx_t r = 0;
		for (; r < STANDARD_VECTOR_SIZE; r++) {
			if (!FetchNext()) {
				stream_exhausted = true;
				break;
			}
			WriteToChunk(data_chunk, r);
		}

		data_chunk.SetChildCardinality(r);

		if (stream_exhausted) {
			stmt.reset();
			if (connection && !connection_released) {
				connection->ReleaseConnection();
				connection_released = true;
			}
		}

		return data_chunk;
	}

	// Materialized mode: return from cache
	if (current_chunk_idx < cached_chunks.size()) {
		// Return the owned chunk itself. Retargeting data_chunk here can mutate
		// vectors that are still referenced by a table-function output chunk.
		return *cached_chunks[current_chunk_idx++];
	}

	data_chunk.Reset();
	data_chunk.SetChildCardinality(0);
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
		ThrowQueryODBCError(SQL_HANDLE_STMT, stmt.get(), connection ? connection->GetConn() : SQL_NULL_HDBC,
		                    "source_fetch", query);
	}

	if (retcode == SQL_NO_DATA) {
		return false;
	}

	for (auto &field : fields) {
		if (field.column_bound) {
			field.bind_length = DecodeIndicator(field.bind_length);
		}
	}
	HandleTruncatedData();

	for (size_t i = 0; i < fields.size(); i++) {
		fields[i].bind_is_null = (fields[i].bind_length == SQL_NULL_DATA);
	}

	bool result = (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO);
	return result;
}

bool ODBCResult::Next() {
	if (current_row_chunk && row_idx + 1 < current_row_chunk->size()) {
		row_idx += 1;
		return true;
	}

	auto &chunk = NextChunk();
	current_row_chunk = &chunk;
	row_idx = 0;
	return chunk.size() > 0;
}

DataChunk &ODBCResult::CurrentRowChunk() {
	return current_row_chunk ? *current_row_chunk : data_chunk;
}

void ODBCResult::CheckColumnIdx(idx_t col) {
	auto &chunk = CurrentRowChunk();
	if (col >= chunk.ColumnCount()) {
		throw IOException("Column: %zu out of range of field count: %zu, ODBC query \"%s\"\n", col, chunk.ColumnCount(),
		                  query.c_str());
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
		Vector &vec = CurrentRowChunk().data[col];
		const string_t *data = FlatVector::GetData<string_t>(vec);
		const string_t &st = data[row_idx];
		return string(st.GetData(), st.GetSize());
	}
	throw InternalException("Get called for a String type, actual type: \"%s\", column: %zu, ODBC query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int32_t ODBCResult::GetInt32(idx_t col) {
	CheckNotNull(col);
	ODBCField &f = fields[col];
	Vector &vec = CurrentRowChunk().data[col];
	if (f.duckdb_type.id() == LogicalTypeId::INTEGER) {
		const int32_t *data = FlatVector::GetData<int32_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UINTEGER) {
		const uint32_t *data = FlatVector::GetData<uint32_t>(vec);
		return static_cast<int32_t>(data[row_idx]);
	}
	throw InternalException("Get called for an Int32 type, actual type: \"%s\", column: %zu, ODBC query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int64_t ODBCResult::GetInt64(idx_t col) {
	CheckNotNull(col);
	ODBCField &f = fields[col];
	Vector &vec = CurrentRowChunk().data[col];

	// Handle various integer types that can be converted to int64_t
	switch (f.duckdb_type.id()) {
	case LogicalTypeId::TINYINT: {
		const int8_t *data = FlatVector::GetData<int8_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::UTINYINT: {
		const uint8_t *data = FlatVector::GetData<uint8_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::SMALLINT: {
		const int16_t *data = FlatVector::GetData<int16_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::USMALLINT: {
		const uint16_t *data = FlatVector::GetData<uint16_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::INTEGER: {
		const int32_t *data = FlatVector::GetData<int32_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::UINTEGER: {
		const uint32_t *data = FlatVector::GetData<uint32_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::BIGINT: {
		const int64_t *data = FlatVector::GetData<int64_t>(vec);
		return data[row_idx];
	}
	case LogicalTypeId::UBIGINT: {
		const uint64_t *data = FlatVector::GetData<uint64_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	case LogicalTypeId::DOUBLE: {
		if (vec.GetType().id() == LogicalTypeId::DOUBLE) {
			const double *data = FlatVector::GetData<double>(vec);
			return static_cast<int64_t>(data[row_idx]);
		} else if (vec.GetType().id() == LogicalTypeId::VARCHAR) {
			const string_t *data = FlatVector::GetData<string_t>(vec);
			const string_t st = data[row_idx];
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
	Vector &vec = CurrentRowChunk().data[col];
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
	auto *time_value = reinterpret_cast<SQL_TIME_STRUCT *>(f.bind_buffer.data());
	dtime_t tm = Time::FromTime(time_value->hour, time_value->minute, time_value->second, 0);
	string time_str = Time::ToString(tm);
	string_t st(time_str.c_str(), time_str.length());
	auto data = FlatVector::GetDataMutable<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

template <class T>
static void WriteExactDecimal(const string &text, Vector &vec, idx_t row, uint8_t precision, uint8_t scale) {
	T value;
	string error;
	CastParameters parameters(true, &error);
	if (!TryCastToDecimal::Operation(string_t(text), value, parameters, precision, scale)) {
		throw ConversionException("ODBC DECIMAL value does not fit the declared precision/scale");
	}
	FlatVector::GetDataMutable<T>(vec)[row] = value;
}

static void WriteDecimal(ODBCField &f, Vector &vec, idx_t row) {
	if (f.bind_length == SQL_NULL_DATA) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	if (f.bind_length <= 0 || static_cast<size_t>(f.bind_length) > f.ValueBufferSize()) {
		throw ConversionException("ODBC DECIMAL value has an invalid transfer length");
	}
	string text(f.ValueData(), static_cast<size_t>(f.bind_length));
	auto precision = DecimalType::GetWidth(vec.GetType());
	auto scale = DecimalType::GetScale(vec.GetType());
	idx_t pos = (text[0] == '+' || text[0] == '-') ? 1 : 0;
	bool dot = false, digits = false;
	idx_t fractional = 0;
	for (; pos < text.size(); pos++) {
		auto c = text[pos];
		if (c == '.' && !dot) {
			dot = true;
			continue;
		}
		if (c < '0' || c > '9') {
			throw ConversionException("ODBC DECIMAL value has an invalid numeric representation");
		}
		digits = true;
		if (dot && ++fractional > scale && c != '0') {
			throw ConversionException("ODBC DECIMAL value exceeds its declared scale");
		}
	}
	if (!digits) {
		throw ConversionException("ODBC DECIMAL value has no digits");
	}
	// Use DuckDB's checked decimal parser for all physical widths. Concatenating
	// integer/fractional strings and parsing as an integer rejects leading zeros
	// in strict HUGEINT conversion and can silently truncate fractional digits.
	if (precision <= 4) {
		WriteExactDecimal<int16_t>(text, vec, row, precision, scale);
	} else if (precision <= 9) {
		WriteExactDecimal<int32_t>(text, vec, row, precision, scale);
	} else if (precision <= 18) {
		WriteExactDecimal<int64_t>(text, vec, row, precision, scale);
	} else {
		WriteExactDecimal<hugeint_t>(text, vec, row, precision, scale);
	}
}

static void WriteDoubleFromString(ODBCField &f, Vector &vec, idx_t row) {
	if (f.bind_length <= 0 || static_cast<size_t>(f.bind_length) > f.ValueBufferSize()) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	string value(f.ValueData(), NumericCast<size_t>(f.bind_length));
	try {
		FlatVector::GetDataMutable<double>(vec)[row] = std::stod(value);
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to parse ODBC catalog numeric value '%s' as DOUBLE: %s", value, e.what());
	}
}

static void WriteString(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(vec.GetType().id() == LogicalTypeId::VARCHAR || vec.GetType().id() == LogicalTypeId::BLOB);

	if (f.odbc_type == SQL_TIME || f.odbc_type == SQL_TYPE_TIME) {
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

	SQLSMALLINT c_type = f.c_type;
	if (c_type == SQL_C_WCHAR) {
		if (f.bind_length <= 0 || f.ValueBufferSize() < sizeof(SQLWCHAR)) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		size_t byte_len = static_cast<size_t>(f.bind_length);
		if (byte_len > f.ValueBufferSize()) {
			byte_len = f.ValueBufferSize();
		}
		size_t unit_len = byte_len / sizeof(SQLWCHAR);
		auto *wdata = reinterpret_cast<const SQLWCHAR *>(f.ValueData());
		while (unit_len > 0 && wdata[unit_len - 1] == 0) {
			unit_len--;
		}
		string utf8_val = UTF16UnitsToUTF8(wdata, unit_len);
		string_t st(utf8_val.c_str(), utf8_val.length());
		auto data = FlatVector::GetDataMutable<string_t>(vec);
		data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
		return;
	}

	if (static_cast<size_t>(f.bind_length) > f.ValueBufferSize()) {
		f.bind_length = static_cast<SQLLEN>(f.ValueBufferSize());
	}

	string_t st(f.ValueData(), f.bind_length);
	auto data = FlatVector::GetDataMutable<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

static void WriteBool(ODBCField &f, Vector &vec, idx_t row) {

	D_ASSERT(f.bind_buffer.size() >= sizeof(int8_t));
	auto data = FlatVector::GetDataMutable<bool>(vec);
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

	auto data = FlatVector::GetDataMutable<NUM_TYPE>(vec);
	data[row] = num;
}

static void WriteDateTime(ODBCField &f, Vector &vec, idx_t row) {
	if (vec.GetType().id() == LogicalTypeId::DATE) {
		D_ASSERT(f.bind_buffer.size() >= sizeof(SQL_DATE_STRUCT));
		auto *date_value = reinterpret_cast<SQL_DATE_STRUCT *>(f.bind_buffer.data());
		FlatVector::GetDataMutable<date_t>(vec)[row] = Date::FromDate(date_value->year, date_value->month, date_value->day);
		return;
	}
	if (vec.GetType().id() == LogicalTypeId::TIME) {
		D_ASSERT(f.bind_buffer.size() >= sizeof(SQL_TIME_STRUCT));
		auto *time_value = reinterpret_cast<SQL_TIME_STRUCT *>(f.bind_buffer.data());
		FlatVector::GetDataMutable<dtime_t>(vec)[row] =
		    Time::FromTime(time_value->hour, time_value->minute, time_value->second, 0);
		return;
	}

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
		date_t *data = FlatVector::GetDataMutable<date_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIME: {
		if (ts->hour < 0 || ts->hour > 24 ||
		    (ts->hour == 24 && (ts->minute > 0 || ts->second > 0 || ts->fraction > 0))) {
			throw BinderException("time field value out of range, hour value: " + std::to_string(ts->hour));
		}
		dtime_t val = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		dtime_t *data = FlatVector::GetDataMutable<dtime_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		date_t dt = Date::FromDate(ts->year, ts->month, ts->day);
		dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		timestamp_t *data = FlatVector::GetDataMutable<timestamp_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		// For ODBC, timezone information is not directly available in SQL_TIMESTAMP_STRUCT
		// We assume local time and convert accordingly
		date_t dt = Date::FromDate(ts->year, ts->month, ts->day);
		dtime_t tm = Time::FromTime(ts->hour, ts->minute, ts->second, ts->fraction / 1000);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		timestamp_tz_t *data = FlatVector::GetDataMutable<timestamp_tz_t>(vec);
		data[row] = timestamp_tz_t(val.value);
		break;
	}
	default:
		throw InternalException("Unsupported date/time type: " + vec.GetType().ToString());
	}
}

void ODBCResult::WriteToChunk(DataChunk &chunk, idx_t row) {
	for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
		if (col >= fields.size()) {
			throw InternalException("Column index out of bounds in WriteToChunk");
		}

		ODBCField &f = fields[col];
		Vector &vec = chunk.data[col];

		if (f.bind_length == SQL_NULL_DATA) {
			FlatVector::SetNull(vec, row, true);
			continue;
		}

		try {
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
				if (f.c_type == SQL_C_CHAR) {
					WriteDoubleFromString(f, vec, row);
				} else if (f.c_type == SQL_C_DOUBLE) {
					WriteNumber<double>(f, vec, row);
				} else {
					throw InvalidInputException("Unsupported ODBC catalog DOUBLE transfer type for column %s", f.name);
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
		} catch (const std::exception &error) {
			auto diagnostic = QueryResultErrorMessage(error, "odbc", query, f.name, vec.GetType().ToString());
			if (diagnostic.empty()) {
				throw;
			}
			throw IOException("%s", diagnostic);
		}
	}
}

} // namespace duckdb
