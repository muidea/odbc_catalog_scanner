#include "odbc_result.hpp"

#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "odbc_types.hpp"
#include <sql.h>
#include <sqlext.h>

namespace duckdb {

static const size_t DEFAULT_BIND_BUFFER_SIZE = 64;

static size_t GetBufferSize(SQLSMALLINT sql_type, size_t max_len, const ODBCTypeConfig &type_config) {
	switch (sql_type) {
	case SQL_TINYINT:
		return sizeof(int8_t);
	case SQL_SMALLINT:
		return sizeof(int16_t);
	case SQL_INTEGER:
		return sizeof(int32_t);
	case SQL_BIGINT:
		return sizeof(int64_t);
	case SQL_REAL:
		return sizeof(float);
	case SQL_DOUBLE:
		return sizeof(double);
	case SQL_TYPE_DATE:
	case SQL_TYPE_TIME:
	case SQL_TYPE_TIMESTAMP:
		return sizeof(SQL_DATE_STRUCT); // Use SQL_DATE_STRUCT as base size
	default:
		return max_len > 0 ? max_len : DEFAULT_BIND_BUFFER_SIZE;
	}
}

ODBCField::ODBCField(void *field_info, LogicalType duckdb_type_p, const ODBCTypeConfig &type_config)
    : duckdb_type(std::move(duckdb_type_p)) {
	// TODO: Extract field information from ODBC
	// For now, we'll use basic defaults
	this->odbc_type = SQL_VARCHAR;
	size_t buf_size = GetBufferSize(this->odbc_type, 0, type_config);
	bind_buffer.resize(buf_size);
}

void ODBCField::ResetBind() {
	this->bind_is_null = false;
	this->bind_length = 0;
	this->bind_error = false;
}

static vector<ODBCField> ReadFields(const std::string &query, SQLHSTMT stmt, const ODBCTypeConfig &type_config) {
	vector<ODBCField> fields;
	
	SQLSMALLINT num_cols;
	SQLRETURN ret = SQLNumResultCols(stmt, &num_cols);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		throw IOException("Failed to get number of result columns for ODBC query \"%s\"", query.c_str());
	}
	
	if (num_cols == 0) {
		return fields;
	}

	fields.reserve(num_cols);
	for (SQLSMALLINT i = 0; i < num_cols; i++) {
		SQLCHAR col_name[256];
		SQLSMALLINT name_len, data_type, decimal_digits, nullable;
		SQLULEN col_size;
		
		ret = SQLDescribeCol(stmt, i + 1, col_name, sizeof(col_name), &name_len, 
							&data_type, &col_size, &decimal_digits, &nullable);
		if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
			throw IOException("Failed to describe column %d for ODBC query \"%s\"", i, query.c_str());
		}
		
		// TODO: Map ODBC data type to DuckDB logical type
		// For now, use VARCHAR for everything
		LogicalType ltype = LogicalType::VARCHAR;
		
		ODBCField field(nullptr, std::move(ltype), type_config);
		field.name = string((char*)col_name, name_len);
		field.odbc_type = data_type;
		fields.emplace_back(std::move(field));
	}

	return fields;
}

static void InitODBCBinds(SQLHSTMT stmt, vector<ODBCField> &fields, 
                         vector<SQLPOINTER> &bind_buffers,
                         vector<SQLLEN> &bind_lengths,
                         vector<SQLSMALLINT> &bind_types) {
	SQLRETURN ret;
	
	for (size_t i = 0; i < fields.size(); i++) {
		auto &f = fields[i];
		
		// Allocate buffer for this column
		size_t buf_size = GetBufferSize(f.odbc_type, 0, ODBCTypeConfig());
		f.bind_buffer.resize(buf_size);
		
		SQLPOINTER buffer_ptr = f.bind_buffer.data();
		SQLLEN indicator = 0;
		SQLSMALLINT target_type;
		
		// Map ODBC type to appropriate target type
		switch (f.odbc_type) {
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			target_type = SQL_C_SBIGINT;
			break;
		case SQL_REAL:
			target_type = SQL_C_FLOAT;
			break;
		case SQL_DOUBLE:
			target_type = SQL_C_DOUBLE;
			break;
		case SQL_TYPE_DATE:
			target_type = SQL_C_TYPE_DATE;
			break;
		case SQL_TYPE_TIME:
			target_type = SQL_C_TYPE_TIME;
			break;
		case SQL_TYPE_TIMESTAMP:
			target_type = SQL_C_TYPE_TIMESTAMP;
			break;
		default:
			target_type = SQL_C_CHAR;
			buffer_ptr = f.bind_buffer.data();
			break;
		}
		
		ret = SQLBindCol(stmt, i + 1, target_type, buffer_ptr, buf_size, &indicator);
		if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
			throw IOException("Failed to bind column %zu", i);
		}
		
		bind_buffers.push_back(buffer_ptr);
		bind_lengths.push_back(indicator);
		bind_types.push_back(target_type);
	}
}

static vector<LogicalType> CreateChunkTypes(vector<ODBCField> &fields, const ODBCTypeConfig &type_config) {
	vector<LogicalType> ltypes;
	for (idx_t c = 0; c < fields.size(); c++) {
		ODBCField &f = fields[c];
		LogicalType &lt = f.duckdb_type;
		
		// TODO: Implement proper type mapping from ODBC to DuckDB
		// For now, use VARCHAR for everything
		ltypes.push_back(LogicalType::VARCHAR);
	}
	return ltypes;
}

ODBCResult::ODBCResult(const std::string &query_p, ODBCStatementPtr stmt_p, ODBCTypeConfig type_config_p,
                         idx_t affected_rows_p)
    : query(query_p), stmt(std::move(stmt_p)), type_config(std::move(type_config_p)), affected_rows(affected_rows_p) {
	if (affected_rows != static_cast<idx_t>(-1)) {
		return;
	}
	
	this->fields = ReadFields(query, stmt.get(), type_config);
	InitODBCBinds(stmt.get(), fields, bind_buffers, bind_lengths, bind_types);

	auto ltypes = CreateChunkTypes(fields, type_config);
	this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);
}

void ODBCResult::HandleTruncatedData() {
	// TODO: Implement truncated data handling for ODBC
	// This is complex and driver-specific
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
	for (auto &f : fields) {
#ifdef DEBUG
		std::memset(f.bind_buffer.data(), '\0', f.bind_buffer.size());
#endif
		f.ResetBind();
	}

	SQLRETURN ret = SQLFetch(stmt.get());
	
	if (ret == SQL_ERROR) {
		SQLCHAR sqlstate[6];
		SQLINTEGER native_error;
		SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
		SQLSMALLINT text_length;
		
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, sqlstate, &native_error, 
			message_text, SQL_MAX_MESSAGE_LENGTH, &text_length);
		
		throw IOException("Failed to fetch result row for ODBC query \"%s\": %s", 
			query.c_str(), (char*)message_text);
	}

	HandleTruncatedData();

	return ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO;
}

bool ODBCResult::Next() {
	if (row_idx < data_chunk.size() - 1) {
		row_idx += 1;
		return true;
	}
	NextChunk();
	row_idx = 0;
	return data_chunk.size() > 0;
}

void ODBCResult::CheckColumnIdx(idx_t col) {
	if (col >= data_chunk.ColumnCount()) {
		throw IOException("Column: %zu out of range of field count: %zu, ODBC query \"%s\"", 
			col, data_chunk.ColumnCount(), query.c_str());
	}
}

void ODBCResult::CheckNotNull(idx_t col) {
	if (IsNull(col)) {
		throw InternalException("Get called for a NULL value, column: %zu, ODBC query \"%s\"", 
			col, query.c_str());
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
	throw InternalException("Get called for a String type, actual type: \"%s\", column: %zu, ODBC query \"%s\"",
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
	throw InternalException("Get called for an Int32 type, actual type: \"%s\", column: %zu, ODBC query \"%s\"",
		f.duckdb_type.ToString(), col, query.c_str());
}

int64_t ODBCResult::GetInt64(idx_t col) {
	CheckNotNull(col);
	ODBCField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	if (f.duckdb_type.id() == LogicalTypeId::BIGINT) {
		int64_t *data = FlatVector::GetData<int64_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UBIGINT) {
		uint64_t *data = FlatVector::GetData<uint64_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	}
	throw InternalException("Get called for an Int64 type, actual type: \"%s\", column: %zu, ODBC query \"%s\"",
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
		                        "that didn't affect any rows, query \"%s\"",
		                        query.c_str());
	}
	return affected_rows;
}

const vector<ODBCField> &ODBCResult::Fields() {
	return fields;
}

static void WriteString(ODBCField &f, Vector &vec, idx_t row) {
	string_t st(f.bind_buffer.data(), f.bind_length);
	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

static void WriteBool(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(int8_t));
	auto data = FlatVector::GetData<bool>(vec);
	
	// Simple boolean conversion - check if first character is non-zero
	char first = f.bind_buffer[0];
	auto data_ptr = FlatVector::GetData<bool>(vec);
	data_ptr[row] = (first != '\0' && first != '0');
}

template <typename NUM_TYPE>
static void WriteNumber(ODBCField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(NUM_TYPE));
	NUM_TYPE num = *reinterpret_cast<NUM_TYPE *>(f.bind_buffer.data());
	auto data = FlatVector::GetData<NUM_TYPE>(vec);
	data[row] = num;
}

void ODBCResult::WriteToChunk(idx_t row) {
	for (idx_t col = 0; col < data_chunk.ColumnCount(); col++) {
		ODBCField &f = fields[col];
		Vector &vec = data_chunk.data[col];

		if (f.bind_is_null) {
			FlatVector::SetNull(vec, row, true);
			continue;
		}

		// For now, write everything as string
		// TODO: Implement proper type conversion based on ODBC type
		WriteString(f, vec, row);
	}
}

} // namespace duckdb
