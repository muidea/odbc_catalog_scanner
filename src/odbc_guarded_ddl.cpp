//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_guarded_ddl.cpp
//
// Restricted native DDL and capability introspection for Dameng, Db2, and SAP HANA.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "guarded_odbc_source_error.hpp"
#include "odbc_dialect.hpp"
#include "storage/odbc_catalog.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace duckdb {
namespace {

static bool SimpleIntLiteral(const string &s) {
	if (s.empty() || s.size() > 38)
		return false;
	idx_t i = s[0] == '-' ? 1 : 0;
	if (i == s.size())
		return false;
	for (; i < s.size(); i++) {
		if (s[i] < '0' || s[i] > '9')
			return false;
	}
	return true;
}

static string Quote(const string &s) {
	return KeywordHelper::WriteQuoted(s, '"');
}

static string Literal(const string &s) {
	return ODBCUtils::WriteLiteral(s);
}

static string JSON(const string &s) {
	const char *hex = "0123456789abcdef";
	string out = "\"";
	for (unsigned char c : s) {
		if (c == '"' || c == '\\') {
			out += '\\';
			out += static_cast<char>(c);
		} else if (c < 32) {
			out += "\\u00";
			out += hex[c >> 4];
			out += hex[c & 15];
		} else {
			out += static_cast<char>(c);
		}
	}
	return out + "\"";
}

static bool ValidQuerySQL(const string &sql) {
	if (sql.empty() || sql.size() > 65536 || sql.find('\0') != string::npos)
		return false;
	idx_t i = 0;
	while (i < sql.size() && StringUtil::CharacterIsSpace(sql[i]))
		i++;
	auto rest = StringUtil::Upper(sql.substr(i));
	bool starts_select = StringUtil::StartsWith(rest, "SELECT") &&
	                     (rest.size() == 6 || StringUtil::CharacterIsSpace(rest[6]) || rest[6] == '(');
	bool starts_with = StringUtil::StartsWith(rest, "WITH") &&
	                   (rest.size() == 4 || StringUtil::CharacterIsSpace(rest[4]) || rest[4] == '(');
	if (!starts_select && !starts_with)
		return false;
	char quote = 0;
	for (; i < sql.size(); i++) {
		char c = sql[i];
		if (quote) {
			if (c == quote) {
				if (i + 1 < sql.size() && sql[i + 1] == quote)
					i++;
				else
					quote = 0;
			}
		} else if (c == '\'' || c == '"') {
			quote = c;
		} else if (c == ';') {
			return false;
		}
	}
	return quote == 0;
}

static string DefaultLiteralSQL(ODBCDialect dialect, const string &kind, const string &value) {
	if (kind == "null" && value.empty())
		return "NULL";
	if (kind == "integer" && SimpleIntLiteral(value))
		return value;
	if (kind == "boolean" && (value == "true" || value == "false"))
		return dialect == ODBCDialect::DAMENG ? (value == "true" ? "1" : "0") : (value == "true" ? "TRUE" : "FALSE");
	if (kind == "string" && value.size() <= 4096 && value.find('\0') == string::npos)
		return Literal(value);
	throw BinderException("DDL default literal is unsupported");
}

struct DDLIntent {
	int32_t contract_version = 2;
	bool conditional = false;
	string database, schema, operation, action, name, index, owner, new_name, column_name, query_sql, sql;
	vector<pair<string, string>> column_specs;
	vector<string> columns, constraints, keys;
	string default_kind, default_value, constraint_name, constraint_kind, check_column, check_operator, check_value,
	    reference_schema, reference_table, identity_mode;
	vector<string> constraint_keys, reference_keys;

	static bool Name(const string &s) {
		return !s.empty() && s.size() <= 128 && s.find('\0') == string::npos;
	}
	static string Text(const Value &v) {
		if (v.IsNull() || v.type().id() != LogicalTypeId::VARCHAR)
			throw BinderException("DDL structured input is invalid");
		return v.GetValue<string>();
	}
	static string ColumnType(ODBCDialect dialect, const string &type, int32_t length, int32_t precision,
	                         int32_t scale, bool v5) {
		if (v5) {
			if (type == "numeric") {
				int max_p = dialect == ODBCDialect::DB2 ? 31 : (dialect == ODBCDialect::HANA ? 34 : 38);
				if (length || precision < 1 || precision > max_p || scale < 0 || scale > precision)
					throw BinderException("ODBC DDL numeric precision/scale is unsupported");
				return "DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
			}
			if (type == "timestamp" || type == "time" || (dialect == ODBCDialect::DAMENG && type == "timestamptz")) {
				if (length || precision < 0 || precision > 6 || scale)
					throw BinderException("ODBC DDL temporal precision is unsupported");
				if (type == "timestamptz")
					return "TIMESTAMP(" + std::to_string(precision) + ") WITH TIME ZONE";
				if (dialect == ODBCDialect::HANA)
					return type == "time" ? string("TIME") : string("TIMESTAMP");
				if (dialect == ODBCDialect::DB2 && type == "time")
					return "TIME";
				return string(type == "time" ? "TIME(" : "TIMESTAMP(") + std::to_string(precision) + ")";
			}
			if (type == "timestamptz")
				throw BinderException("ODBC DDL column type is unsupported");
			if (precision || scale)
				throw BinderException("ODBC DDL unexpected type modifiers");
			int max_v = dialect == ODBCDialect::DB2 ? 32672 : (dialect == ODBCDialect::HANA ? 5000 : 8188);
			if (type == "varchar" && length >= 1 && length <= max_v) {
				return string(dialect == ODBCDialect::HANA ? "NVARCHAR(" : "VARCHAR(") + std::to_string(length) + ")";
			}
			if (length)
				throw BinderException("ODBC DDL unexpected column length");
			if (type == "smallint")
				return "SMALLINT";
			if (type == "integer")
				return dialect == ODBCDialect::DAMENG ? "INT" : "INTEGER";
			if (type == "bigint")
				return "BIGINT";
			if (type == "real")
				return "REAL";
			if (type == "double")
				return "DOUBLE";
			if (type == "boolean")
				return dialect == ODBCDialect::DAMENG ? "BIT" : "BOOLEAN";
			if (type == "text") {
				if (dialect == ODBCDialect::DAMENG)
					return "TEXT";
				if (dialect == ODBCDialect::HANA)
					return "NCLOB";
				return "CLOB";
			}
			if (type == "date")
				return "DATE";
			if (type == "uuid")
				return dialect == ODBCDialect::HANA ? "NVARCHAR(36)" : "CHAR(36)";
			if (type == "bytea") {
				if (dialect == ODBCDialect::HANA)
					return "VARBINARY(5000)";
				if (dialect == ODBCDialect::DB2)
					return "BLOB";
				return "BLOB";
			}
			throw BinderException("ODBC DDL column type is unsupported");
		}
		if (type == "smallint")
			return length ? throw BinderException("DDL column length is invalid") : string("SMALLINT");
		if (type == "integer")
			return length ? throw BinderException("DDL column length is invalid")
			              : string(dialect == ODBCDialect::DAMENG ? "INT" : "INTEGER");
		if (type == "bigint")
			return length ? throw BinderException("DDL column length is invalid") : string("BIGINT");
		int max_v = dialect == ODBCDialect::DB2 ? 32672 : (dialect == ODBCDialect::HANA ? 5000 : 8188);
		if (type == "varchar" && length >= 1 && length <= max_v)
			return string(dialect == ODBCDialect::HANA ? "NVARCHAR(" : "VARCHAR(") + std::to_string(length) + ")";
		if (type == "text" && !length)
			return dialect == ODBCDialect::DAMENG ? "TEXT" : (dialect == ODBCDialect::HANA ? "NCLOB" : "CLOB");
		throw BinderException("ODBC DDL column type is unsupported");
	}
	static DDLIntent Bind(ODBCDialect dialect, const vector<Value> &input, bool conditional = false) {
		if (input.size() != 5 || input[4].IsNull() || input[4].type().id() != LogicalTypeId::STRUCT)
			throw BinderException("DDL structured input is invalid");
		const auto &types = StructType::GetChildTypes(input[4].type());
		const auto &fields = StructValue::GetChildren(input[4]);
		if (types.empty() || types[0].first != "contract_version" || fields[0].IsNull() ||
		    fields[0].type().id() != LogicalTypeId::INTEGER)
			throw BinderException("DDL contract is unavailable");
		const int32_t contract = fields[0].GetValue<int32_t>();
		if (contract < 2 || contract > 6)
			throw BinderException("DDL contract is unavailable");
		DDLIntent d;
		d.contract_version = contract;
		d.conditional = conditional;
		d.database = Text(input[1]);
		d.schema = Text(input[2]);
		d.operation = Text(input[3]);
		if (!Name(d.database) || !Name(d.schema) || d.operation.size() != 36)
			throw BinderException("DDL source scope is invalid");

		vector<string> names = {"contract_version", "target_contract", "action", "name", "index_name", "owner"};
		if (contract >= 3) {
			names.push_back("new_name");
			names.push_back("column_name");
		}
		if (contract == 4) {
			for (auto *f : {"default_kind", "default_value", "constraint_name", "constraint_kind", "check_column",
			                "check_operator", "check_value", "reference_schema", "reference_table", "identity_mode",
			                "constraint_keys", "reference_keys"})
				names.push_back(f);
		}
		if (contract == 6) {
			names.push_back("query_sql");
			names.push_back("constraints");
		}
		names.push_back("columns");
		names.push_back("keys");
		if (types.size() != names.size())
			throw BinderException("DDL fields are invalid");
		for (idx_t i = 0; i < names.size(); i++)
			if (types[i].first != names[i] || fields[i].IsNull())
				throw BinderException("DDL fields are invalid");
		if (Text(fields[1]) != "EXECUTION_NAME")
			throw BinderException("DDL contract is unavailable");
		d.action = Text(fields[2]);
		if (contract == 6 && d.action != "create_table" && d.action != "add_column" && d.action != "create_table_as")
			throw BinderException("DDL action requires a different contract");
		if (contract != 6 && d.action == "create_table_as")
			throw BinderException("DDL action requires contract v6");
		d.name = Text(fields[3]);
		d.index = Text(fields[4]);
		d.owner = Text(fields[5]);
		if (!Name(d.name) || (!d.index.empty() && !Name(d.index)) || (!d.owner.empty() && !Name(d.owner)))
			throw BinderException("DDL target name is invalid");
		idx_t idx = 6;
		if (contract >= 3) {
			d.new_name = Text(fields[idx++]);
			d.column_name = Text(fields[idx++]);
			if ((!d.new_name.empty() && !Name(d.new_name)) || (!d.column_name.empty() && !Name(d.column_name)))
				throw BinderException("DDL altered name is invalid");
		}
		if (contract == 4) {
			d.default_kind = Text(fields[idx++]);
			d.default_value = Text(fields[idx++]);
			d.constraint_name = Text(fields[idx++]);
			d.constraint_kind = Text(fields[idx++]);
			d.check_column = Text(fields[idx++]);
			d.check_operator = Text(fields[idx++]);
			d.check_value = Text(fields[idx++]);
			d.reference_schema = Text(fields[idx++]);
			d.reference_table = Text(fields[idx++]);
			d.identity_mode = Text(fields[idx++]);
			if (fields[idx].type().id() != LogicalTypeId::LIST || fields[idx + 1].type().id() != LogicalTypeId::LIST)
				throw BinderException("DDL constraint keys are invalid");
			for (auto &k : ListValue::GetChildren(fields[idx++])) {
				auto s = Text(k);
				if (!Name(s))
					throw BinderException("DDL constraint key is invalid");
				d.constraint_keys.push_back(s);
			}
			for (auto &k : ListValue::GetChildren(fields[idx++])) {
				auto s = Text(k);
				if (!Name(s))
					throw BinderException("DDL reference key is invalid");
				d.reference_keys.push_back(s);
			}
		}
		auto constraint_clause = [&](const string &kind, const vector<string> &ck, const string &rs, const string &rt,
		                             const vector<string> &rk, const string &col, const string &op,
		                             const string &val) {
			if (kind == "primary_key" || kind == "unique") {
				if (ck.empty() || ck.size() > 32)
					throw BinderException("DDL constraint keys are invalid");
				vector<string> ks;
				for (auto &k : ck)
					ks.push_back(Quote(k));
				return string(kind == "primary_key" ? "PRIMARY KEY (" : "UNIQUE (") + StringUtil::Join(ks, ",") + ")";
			}
			if (kind == "foreign_key") {
				if (ck.empty() || ck.size() != rk.size() || !Name(rs) || !Name(rt))
					throw BinderException("DDL foreign key options are invalid");
				vector<string> cqs, rqs;
				for (auto &k : ck)
					cqs.push_back(Quote(k));
				for (auto &k : rk)
					rqs.push_back(Quote(k));
				return "FOREIGN KEY (" + StringUtil::Join(cqs, ",") + ") REFERENCES " + Quote(rs) + "." + Quote(rt) +
				       " (" + StringUtil::Join(rqs, ",") + ")";
			}
			if (kind == "check") {
				if (!Name(col) || !SimpleIntLiteral(val) ||
				    (op != "=" && op != "<>" && op != "<" && op != ">" && op != "<=" && op != ">="))
					throw BinderException("DDL check constraint is unsupported");
				return "CHECK (" + Quote(col) + " " + op + " " + val + ")";
			}
			throw BinderException("DDL constraint kind is unsupported");
		};
		if (contract == 6) {
			d.query_sql = Text(fields[idx++]);
			if (!d.query_sql.empty() && !ValidQuerySQL(d.query_sql))
				throw BinderException("DDL query payload is invalid");
			if (fields[idx].type().id() != LogicalTypeId::LIST)
				throw BinderException("DDL constraint list is invalid");
			const auto &cons = ListValue::GetChildren(fields[idx++]);
			if (cons.size() > 64)
				throw BinderException("DDL constraint bound exceeded");
			for (auto &c : cons) {
				if (c.IsNull() || c.type().id() != LogicalTypeId::STRUCT)
					throw BinderException("DDL constraint definition is invalid");
				const auto &ct = StructType::GetChildTypes(c.type());
				const auto &cv = StructValue::GetChildren(c);
				const vector<string> cnames = {"name",             "kind",            "columns",
				                               "reference_schema", "reference_table", "reference_columns",
				                               "check_column",     "check_operator",  "check_value"};
				if (ct.size() != cnames.size())
					throw BinderException("DDL constraint fields are invalid");
				for (idx_t j = 0; j < cnames.size(); j++)
					if (ct[j].first != cnames[j] || cv[j].IsNull())
						throw BinderException("DDL constraint fields are invalid");
				string cname = Text(cv[0]), kind = Text(cv[1]);
				if (!cname.empty() && !Name(cname))
					throw BinderException("DDL constraint name is invalid");
				auto read_keys = [&](const Value &lv) {
					if (lv.type().id() != LogicalTypeId::LIST)
						throw BinderException("DDL constraint keys are invalid");
					vector<string> out;
					for (auto &item : ListValue::GetChildren(lv)) {
						auto kn = Text(item);
						if (!Name(kn))
							throw BinderException("DDL constraint key name is invalid");
						out.push_back(kn);
					}
					if (out.size() > 32)
						throw BinderException("DDL constraint keys bound exceeded");
					return out;
				};
				auto ck = (kind == "check") ? vector<string>() : read_keys(cv[2]);
				auto rk = (kind == "foreign_key") ? read_keys(cv[5]) : vector<string>();
				string rs = Text(cv[3]), rt = Text(cv[4]);
				if (kind == "foreign_key" && rs != d.schema)
					throw BinderException("DDL foreign key schema must match source schema");
				auto clause = constraint_clause(kind, ck, rs, rt, rk, Text(cv[6]), Text(cv[7]), Text(cv[8]));
				d.constraints.push_back(cname.empty() ? clause : "CONSTRAINT " + Quote(cname) + " " + clause);
			}
		}
		if (fields[idx].type().id() != LogicalTypeId::LIST || fields[idx + 1].type().id() != LogicalTypeId::LIST)
			throw BinderException("DDL payload is invalid");
		const auto &cols = ListValue::GetChildren(fields[idx]);
		const auto &keys = ListValue::GetChildren(fields[idx + 1]);
		if (cols.size() > 256 || keys.size() > 32)
			throw BinderException("DDL payload bound exceeded");
		vector<string> seen;
		for (auto &col : cols) {
			if (col.IsNull() || col.type().id() != LogicalTypeId::STRUCT)
				throw BinderException("DDL column definition is invalid");
			const auto &ct = StructType::GetChildTypes(col.type());
			const auto &cv = StructValue::GetChildren(col);
			idx_t expected_fields = contract == 6 ? 10 : (contract == 5 ? 5 : 3);
			if (ct.size() != expected_fields || ct[0].first != "name" || ct[1].first != "type" ||
			    ct[2].first != "length" || cv[2].IsNull() || cv[2].type().id() != LogicalTypeId::INTEGER)
				throw BinderException("DDL column fields are invalid");
			string n = Text(cv[0]), type = Text(cv[1]);
			if (!Name(n) || std::find(seen.begin(), seen.end(), n) != seen.end())
				throw BinderException("DDL duplicate column is invalid");
			seen.push_back(n);
			int32_t prec = 0, scale = 0;
			if (contract >= 5) {
				if (ct[3].first != "precision" || ct[4].first != "scale" || cv[3].IsNull() || cv[4].IsNull() ||
				    cv[3].type().id() != LogicalTypeId::INTEGER || cv[4].type().id() != LogicalTypeId::INTEGER)
					throw BinderException("DDL numeric/time modifiers are invalid");
				prec = cv[3].GetValue<int32_t>();
				scale = cv[4].GetValue<int32_t>();
			}
			auto native = ColumnType(dialect, type, cv[2].GetValue<int32_t>(), prec, scale, contract >= 5);
			string extra;
			if (contract == 6) {
				if (ct[5].first != "not_null" || ct[6].first != "default_kind" || ct[7].first != "default_value" ||
				    ct[8].first != "primary_key" || ct[9].first != "unique" || cv[5].IsNull() || cv[8].IsNull() ||
				    cv[9].IsNull() || cv[5].type().id() != LogicalTypeId::BOOLEAN ||
				    cv[8].type().id() != LogicalTypeId::BOOLEAN || cv[9].type().id() != LogicalTypeId::BOOLEAN)
					throw BinderException("DDL v6 column modifiers are invalid");
				string dk = Text(cv[6]), dv = Text(cv[7]);
				if (!dk.empty())
					extra += " DEFAULT " + DefaultLiteralSQL(dialect, dk, dv);
				else if (!dv.empty())
					throw BinderException("DDL column default kind is missing");
				if (cv[5].GetValue<bool>())
					extra += " NOT NULL";
				if (cv[8].GetValue<bool>())
					extra += " PRIMARY KEY";
				if (cv[9].GetValue<bool>())
					extra += " UNIQUE";
			}
			d.columns.push_back(Quote(n) + " " + native + extra);
			d.column_specs.emplace_back(n, native);
		}
		for (auto &k : keys) {
			auto n = Text(k);
			if (!Name(n) || std::find(d.keys.begin(), d.keys.end(), n) != d.keys.end())
				throw BinderException("DDL index key is invalid");
			d.keys.push_back(n);
		}
		const auto target = Quote(d.schema) + "." + Quote(d.name);
		if (d.action == "create_table") {
			if (d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty())
				throw BinderException("DDL create table options are invalid");
			vector<string> defs = d.columns;
			defs.insert(defs.end(), d.constraints.begin(), d.constraints.end());
			d.sql = "CREATE TABLE " + target + " (" + StringUtil::Join(defs, ",") + ")";
		} else if (d.action == "create_table_as") {
			if (!d.columns.empty() || !d.constraints.empty() || !d.keys.empty() || !d.index.empty() ||
			    !d.owner.empty() || !d.new_name.empty() || !d.column_name.empty() || d.query_sql.empty())
				throw BinderException("DDL create table as options are invalid");
			if (dialect == ODBCDialect::HANA)
				d.sql = "CREATE COLUMN TABLE " + target + " AS (" + d.query_sql + ")";
			else if (dialect == ODBCDialect::DB2)
				d.sql = "CREATE TABLE " + target + " AS (" + d.query_sql + ") DEFINITION ONLY";
			else
				d.sql = "CREATE TABLE " + target + " AS (" + d.query_sql + ")";
		} else if (d.action == "drop_table") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty())
				throw BinderException("DDL drop table options are invalid");
			d.sql = "DROP TABLE " + target + (dialect == ODBCDialect::HANA ? " RESTRICT" : "");
		} else if (d.action == "create_index" || d.action == "create_unique_index") {
			if (!d.columns.empty() || d.keys.empty() || d.index.empty() || !d.owner.empty())
				throw BinderException("DDL create index options are invalid");
			vector<string> ks;
			for (auto &k : d.keys)
				ks.push_back(Quote(k));
			d.sql = string(d.action == "create_unique_index" ? "CREATE UNIQUE INDEX " : "CREATE INDEX ") +
			        Quote(d.schema) + "." + Quote(d.index) + " ON " + target + " (" + StringUtil::Join(ks, ",") + ")";
		} else if (d.action == "drop_index") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty())
				throw BinderException("DDL drop index options are invalid");
			d.sql = "DROP INDEX " + target;
		} else if (d.action == "rename_table") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty() ||
			    !d.column_name.empty() || d.new_name.empty() || d.new_name == d.name)
				throw BinderException("DDL rename table options are invalid");
			if (dialect == ODBCDialect::DAMENG)
				d.sql = "ALTER TABLE " + target + " RENAME TO " + Quote(d.new_name);
			else if (dialect == ODBCDialect::DB2)
				d.sql = "RENAME TABLE " + target + " TO " + Quote(d.new_name);
			else
				d.sql = "RENAME TABLE " + target + " TO " + Quote(d.schema) + "." + Quote(d.new_name);
		} else if (d.action == "add_column") {
			if (d.columns.size() != 1 || !d.keys.empty() || !d.index.empty() || !d.owner.empty())
				throw BinderException("DDL add column options are invalid");
			if (dialect == ODBCDialect::HANA)
				d.sql = "ALTER TABLE " + target + " ADD (" + d.columns[0] + ")";
			else
				d.sql = "ALTER TABLE " + target + " ADD COLUMN " + d.columns[0];
		} else if (d.action == "drop_column") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty() ||
			    !d.new_name.empty() || d.column_name.empty())
				throw BinderException("DDL drop column options are invalid");
			if (dialect == ODBCDialect::HANA)
				d.sql = "ALTER TABLE " + target + " DROP (" + Quote(d.column_name) + ")";
			else
				d.sql = "ALTER TABLE " + target + " DROP COLUMN " + Quote(d.column_name);
		} else if (d.action == "rename_column") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty() ||
			    d.column_name.empty() || d.new_name.empty() || d.new_name == d.column_name)
				throw BinderException("DDL rename column options are invalid");
			if (dialect == ODBCDialect::HANA)
				d.sql = "RENAME COLUMN " + target + "." + Quote(d.column_name) + " TO " + Quote(d.new_name);
			else
				d.sql = "ALTER TABLE " + target + " RENAME COLUMN " + Quote(d.column_name) + " TO " + Quote(d.new_name);
		} else if (d.action == "alter_column_type") {
			if (d.column_specs.size() != 1 || d.column_name.empty() || d.column_specs[0].first != d.column_name)
				throw BinderException("DDL alter column type options are invalid");
			if (dialect == ODBCDialect::DAMENG)
				d.sql = "ALTER TABLE " + target + " MODIFY " + Quote(d.column_name) + " " + d.column_specs[0].second;
			else if (dialect == ODBCDialect::DB2)
				d.sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(d.column_name) + " SET DATA TYPE " +
				        d.column_specs[0].second;
			else
				d.sql = "ALTER TABLE " + target + " ALTER (" + Quote(d.column_name) + " " + d.column_specs[0].second + ")";
		} else if (d.action == "rename_index") {
			if (!d.columns.empty() || !d.keys.empty() || !d.owner.empty() || d.new_name.empty() || d.new_name == d.name)
				throw BinderException("DDL rename index options are invalid");
			if (dialect == ODBCDialect::DAMENG)
				d.sql = "ALTER INDEX " + target + " RENAME TO " + Quote(d.new_name);
			else if (dialect == ODBCDialect::DB2)
				d.sql = "RENAME INDEX " + target + " TO " + Quote(d.new_name);
			else
				d.sql = "RENAME INDEX " + target + " TO " + Quote(d.schema) + "." + Quote(d.new_name);
		} else if (d.action == "create_schema") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty() ||
			    !d.new_name.empty() || !d.column_name.empty() || d.name != d.schema)
				throw BinderException("DDL create schema options are invalid");
			d.sql = "CREATE SCHEMA " + Quote(d.name);
		} else if (d.action == "drop_schema") {
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty() || !d.owner.empty() ||
			    !d.new_name.empty() || !d.column_name.empty() || d.name != d.schema)
				throw BinderException("DDL drop schema options are invalid");
			d.sql = "DROP SCHEMA " + Quote(d.name) + " RESTRICT";
		} else if (d.action == "set_default") {
			if (d.column_name.empty())
				throw BinderException("DDL default column is missing");
			string lit = DefaultLiteralSQL(dialect, d.default_kind, d.default_value);
			if (dialect == ODBCDialect::DAMENG)
				d.sql = "ALTER TABLE " + target + " MODIFY " + Quote(d.column_name) + " DEFAULT " + lit;
			else if (dialect == ODBCDialect::DB2)
				d.sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(d.column_name) + " SET DEFAULT " + lit;
			// HANA builds ALTER (<col> <existing_type> DEFAULT <lit>) dynamically inside ScanDDL.
		} else if (d.action == "drop_default") {
			if (d.column_name.empty())
				throw BinderException("DDL default column is missing");
			if (dialect == ODBCDialect::DAMENG)
				d.sql = "ALTER TABLE " + target + " MODIFY " + Quote(d.column_name) + " DEFAULT NULL";
			else if (dialect == ODBCDialect::DB2)
				d.sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(d.column_name) + " DROP DEFAULT";
		} else if (d.action == "set_not_null" || d.action == "drop_not_null") {
			if (d.column_name.empty())
				throw BinderException("DDL nullability column is missing");
			if (dialect == ODBCDialect::DAMENG)
				d.sql = "ALTER TABLE " + target + " MODIFY " + Quote(d.column_name) +
				        (d.action == "set_not_null" ? " NOT NULL" : " NULL");
			else if (dialect == ODBCDialect::DB2)
				d.sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(d.column_name) +
				        (d.action == "set_not_null" ? " SET NOT NULL" : " DROP NOT NULL");
		} else if (d.action == "add_constraint") {
			if (!Name(d.constraint_name))
				throw BinderException("DDL constraint name is invalid");
			string clause;
			if (d.constraint_kind == "primary_key" || d.constraint_kind == "unique") {
				if (d.constraint_keys.empty() || d.constraint_keys.size() > 32)
					throw BinderException("DDL constraint keys are invalid");
				vector<string> ks;
				for (auto &k : d.constraint_keys)
					ks.push_back(Quote(k));
				clause = string(d.constraint_kind == "primary_key" ? "PRIMARY KEY (" : "UNIQUE (") +
				         StringUtil::Join(ks, ",") + ")";
			} else if (d.constraint_kind == "foreign_key") {
				if (d.constraint_keys.empty() || d.constraint_keys.size() != d.reference_keys.size() ||
				    !Name(d.reference_schema) || !Name(d.reference_table))
					throw BinderException("DDL foreign key options are invalid");
				vector<string> ck, rk;
				for (auto &k : d.constraint_keys)
					ck.push_back(Quote(k));
				for (auto &k : d.reference_keys)
					rk.push_back(Quote(k));
				clause = "FOREIGN KEY (" + StringUtil::Join(ck, ",") + ") REFERENCES " + Quote(d.reference_schema) +
				         "." + Quote(d.reference_table) + " (" + StringUtil::Join(rk, ",") + ")";
			} else if (d.constraint_kind == "check") {
				if (!Name(d.check_column) || !SimpleIntLiteral(d.check_value) ||
				    (d.check_operator != "=" && d.check_operator != "<>" && d.check_operator != "<" &&
				     d.check_operator != ">" && d.check_operator != "<=" && d.check_operator != ">="))
					throw BinderException("DDL check constraint is unsupported");
				clause = "CHECK (" + Quote(d.check_column) + " " + d.check_operator + " " + d.check_value + ")";
			} else {
				throw BinderException("DDL constraint kind is unsupported");
			}
			d.sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Quote(d.constraint_name) + " " + clause;
		} else if (d.action == "drop_constraint") {
			if (!Name(d.constraint_name))
				throw BinderException("DDL constraint name is invalid");
			d.sql = "ALTER TABLE " + target + " DROP CONSTRAINT " + Quote(d.constraint_name);
		} else if (d.action == "truncate") {
			if (!d.identity_mode.empty() && d.identity_mode != "restart" && d.identity_mode != "continue")
				throw BinderException("DDL truncate identity mode is unsupported");
			d.sql = "TRUNCATE TABLE " + target + (dialect == ODBCDialect::DB2 ? " IMMEDIATE" : "");
		} else if (d.action == "reindex") {
			if (dialect != ODBCDialect::DAMENG)
				throw BinderException("DDL REINDEX is unsupported for this dialect");
			if (!d.owner.empty())
				throw BinderException("Dameng REINDEX must not set table owner");
			d.sql = "ALTER INDEX " + target + " REBUILD";
		} else {
			throw BinderException("ODBC DDL action is unsupported");
		}
		if (d.sql.size() > 65536)
			throw BinderException("DDL statement length exceeded");
		return d;
	}
};

class DDLSession {
public:
	DDLSession(ClientContext &ctx_p, SQLHDBC dbc, const string &product_p, int seconds)
	    : ctx(ctx_p), product(product_p), stmt(nullptr, ODBCStatementDelete) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS)
			throw IOException("ODBC DDL statement allocation failed");
		stmt = ODBCStatementPtr(h, ODBCStatementDelete);
		watcher = std::thread([this, seconds] {
			std::unique_lock<std::mutex> lock(mu);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
			while (!stopped) {
				if (ctx.IsInterrupted() || std::chrono::steady_clock::now() >= deadline) {
					cancelled.store(true);
					SQLCancelHandle(SQL_HANDLE_STMT, stmt.get());
					break;
				}
				cv.wait_for(lock, std::chrono::milliseconds(25), [this] { return stopped; });
			}
		});
	}
	~DDLSession() {
		{
			std::lock_guard<std::mutex> lock(mu);
			stopped = true;
		}
		cv.notify_all();
		if (watcher.joinable())
			watcher.join();
	}
	string product, sqlstate;
	SQLINTEGER native_code = 0;
	GuardedODBCSourceError source;
	std::atomic<bool> cancelled {false};

	bool CheckRC(SQLRETURN rc) {
		if (SQL_SUCCEEDED(rc))
			return true;
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), product);
		SQLCHAR st[6] = {0};
		SQLSMALLINT len = 0;
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, st, &native_code, nullptr, 0, &len);
		if (cancelled.load() || ctx.IsInterrupted())
			sqlstate = "57014";
		else
			sqlstate = st[0] ? string(reinterpret_cast<char *>(st)) : "HY000";
		return false;
	}
	vector<vector<string>> Query(const string &sql, const vector<string> &params = {}) {
		SQLFreeStmt(stmt.get(), SQL_CLOSE);
		SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS);
		if (!CheckRC(SQLPrepare(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS)))
			throw IOException("ODBC DDL query prepare failed");
		vector<string> copy = params;
		vector<SQLLEN> lens(copy.size(), 0);
		for (idx_t i = 0; i < copy.size(); i++) {
			lens[i] = copy[i].size();
			if (!CheckRC(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
			                              MaxValue<idx_t>(1, copy[i].size()), 0, const_cast<char *>(copy[i].c_str()),
			                              copy[i].size() + 1, &lens[i])))
				throw IOException("ODBC DDL query bind failed");
		}
		if (!CheckRC(SQLExecute(stmt.get())))
			throw IOException("ODBC DDL query failed");
		SQLSMALLINT ncols = 0;
		if (!CheckRC(SQLNumResultCols(stmt.get(), &ncols)))
			throw IOException("ODBC DDL column count failed");
		vector<vector<string>> rows;
		while (true) {
			auto rc = SQLFetch(stmt.get());
			if (rc == SQL_NO_DATA)
				break;
			if (!CheckRC(rc))
				throw IOException("ODBC DDL fetch failed");
			vector<string> row;
			row.reserve(ncols);
			for (SQLSMALLINT c = 1; c <= ncols; c++) {
				SQLCHAR buf[4096] = {};
				SQLLEN ind = 0;
				auto grc = SQLGetData(stmt.get(), c, SQL_C_CHAR, buf, sizeof(buf), &ind);
				if (!CheckRC(grc))
					throw IOException("ODBC DDL cell read failed");
				row.emplace_back(ind == SQL_NULL_DATA ? string() : string(reinterpret_cast<char *>(buf)));
			}
			rows.push_back(std::move(row));
		}
		return rows;
	}
	string Scalar(const string &sql, const vector<string> &params = {}) {
		auto r = Query(sql, params);
		if (r.size() != 1 || r[0].size() != 1)
			throw IOException("ODBC DDL scalar unavailable");
		return r[0][0];
	}
	void Exec(const string &sql) {
		SQLFreeStmt(stmt.get(), SQL_CLOSE);
		SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS);
		auto rc = SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS);
		if (rc != SQL_NO_DATA && !CheckRC(rc))
			throw IOException("ODBC DDL execution failed");
	}
	bool TryExec(const string &sql) {
		SQLFreeStmt(stmt.get(), SQL_CLOSE);
		SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS);
		auto rc = SQLExecDirect(stmt.get(), reinterpret_cast<SQLCHAR *>(const_cast<char *>(sql.c_str())), SQL_NTS);
		return SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA;
	}

private:
	ClientContext &ctx;
	ODBCStatementPtr stmt;
	std::mutex mu;
	std::condition_variable cv;
	bool stopped = false;
	std::thread watcher;
};

static string Info(SQLHDBC dbc, SQLUSMALLINT key) {
	SQLCHAR value[256] = {0};
	SQLSMALLINT len = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(dbc, key, value, sizeof(value), &len)) || len <= 0 || len >= sizeof(value))
		return string();
	return string(reinterpret_cast<char *>(value), len);
}

static ODBCDialect DialectFromFn(const string &fn_name) {
	if (StringUtil::StartsWith(fn_name, "dameng_"))
		return ODBCDialect::DAMENG;
	if (StringUtil::StartsWith(fn_name, "db2_"))
		return ODBCDialect::DB2;
	if (StringUtil::StartsWith(fn_name, "hana_"))
		return ODBCDialect::HANA;
	throw BinderException("ODBC DDL function dialect is unknown");
}

static string FormatHanaColumnType(const string &data_type, const string &len_s, const string &scale_s) {
	auto u = StringUtil::Upper(data_type);
	if (u == "VARCHAR" || u == "NVARCHAR" || u == "ALPHANUM" || u == "VARBINARY" || u == "BINARY")
		return u + "(" + len_s + ")";
	if (u == "DECIMAL")
		return u + "(" + len_s + "," + scale_s + ")";
	return u;
}

static bool VerifyDialectIdentity(ODBCDialect dialect, SQLHDBC dbc, DDLSession &s, string &version, string &db_name) {
	auto dbms = Info(dbc, SQL_DBMS_NAME);
	if (dialect == ODBCDialect::DAMENG) {
		if (dbms != "DM DATABASE MANAGEMENT SYSTEM")
			return false;
		version = Info(dbc, SQL_DBMS_VER);
		try {
			db_name = s.Scalar("SELECT NAME FROM V$DATABASE");
		} catch (...) {
			db_name = Info(dbc, SQL_DATABASE_NAME);
		}
		return !version.empty();
	}
	if (dialect == ODBCDialect::DB2) {
		if (dbms.find("DB2/") != 0)
			return false;
		version = Info(dbc, SQL_DBMS_VER);
		db_name = s.Scalar("SELECT RTRIM(CURRENT SERVER) FROM SYSIBM.SYSDUMMY1");
		return !version.empty() && !db_name.empty();
	}
	auto lower = StringUtil::Lower(dbms);
	if (lower.find("hdb") == string::npos && lower.find("hana") == string::npos)
		return false;
	auto rows = s.Query("SELECT DATABASE_NAME,VERSION FROM SYS.M_DATABASE");
	if (rows.size() != 1 || rows[0].size() != 2)
		return false;
	db_name = rows[0][0];
	version = rows[0][1];
	return !version.empty();
}

struct CapData : TableFunctionData {
	ODBCCatalog &catalog;
	ODBCDialect dialect;
	string schema;
	CapData(ODBCCatalog &c, ODBCDialect d) : catalog(c), dialect(d) {
	}
};

struct DDLData : TableFunctionData {
	ODBCCatalog &catalog;
	ODBCDialect dialect;
	DDLIntent intent;
	bool diagnostics = false;
	DDLData(ODBCCatalog &c, ODBCDialect dl, DDLIntent i) : catalog(c), dialect(dl), intent(std::move(i)) {
	}
};

struct OneRowState : GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> InitOne(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OneRowState>();
}

static unique_ptr<FunctionData> BindCapabilities(ClientContext &ctx, TableFunctionBindInput &in,
                                                 vector<LogicalType> &types, vector<Identifier> &names) {
	if (in.inputs.size() != 2 || in.inputs[0].IsNull() || in.inputs[1].IsNull())
		throw BinderException("ODBC DDL capability input is incomplete");
	auto dialect = DialectFromFn(in.table_function.GetName().GetIdentifierName());
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, Identifier(in.inputs[0].GetValue<string>()));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		throw BinderException("ODBC DDL catalog is unavailable");
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != dialect)
		throw BinderException("ODBC DDL catalog dialect mismatch");
	auto d = make_uniq<CapData>(catalog, dialect);
	d->schema = in.inputs[1].GetValue<string>();
	if (!DDLIntent::Name(d->schema))
		throw BinderException("ODBC DDL schema name is invalid");
	names = {"sqlstate", "stage", "document"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(d);
}

static void ScanCapabilities(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<OneRowState>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<CapData>();
	string product = ODBCDialectUtil::Name(d.dialect);
	string sqlstate, stage = "source_identity", document;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<DDLSession> session;
	try {
		if (ctx.IsInterrupted())
			throw InterruptException();
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		session = make_uniq<DDLSession>(ctx, dbc, product, 10);
		string version, db_name;
		if (!VerifyDialectIdentity(d.dialect, dbc, *session, version, db_name)) {
			sqlstate = "0A000";
			throw IOException("ODBC DDL identity not certified");
		}
		stage = "source_metadata";
		bool read_only = false;
		SQLULEN mode = SQL_MODE_READ_WRITE;
		if (SQL_SUCCEEDED(SQLGetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, &mode, sizeof(mode), nullptr)) &&
		    mode == SQL_MODE_READ_ONLY)
			read_only = true;
		bool ns_present = false;
		if (d.dialect == ODBCDialect::DAMENG) {
			ns_present =
			    session->Scalar("SELECT COUNT(*) FROM SYS.SYSOBJECTS WHERE TYPE$='SCH' AND NAME=?", {d.schema}) == "1";
		} else if (d.dialect == ODBCDialect::DB2) {
			ns_present =
			    session->Scalar("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME=?", {d.schema}) == "1";
		} else {
			ns_present =
			    session->Scalar("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.SCHEMAS WHERE SCHEMA_NAME=?",
			                    {d.schema}) == "1";
		}
		document = "{\"contract_version\":1,\"product\":" + JSON(product) + ",\"source_version\":" + JSON(version) +
		           ",\"database\":" + JSON(db_name) + ",\"schema\":" + JSON(d.schema) +
		           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
		           ",\"source_read_only\":" + (read_only ? "true" : "false") +
		           ",\"visibility\":\"CATALOG_READ\",\"executor_available\":true}";
		stage.clear();
	} catch (...) {
		if (sqlstate.empty()) {
			sqlstate = session && !session->sqlstate.empty()
			               ? session->sqlstate
			               : (ctx.IsInterrupted() ? "57014" : "08006");
		}
		document.clear();
	}
	if (connection) {
		session.reset();
		connection->Close();
	}
	out.SetValue(0, 0, Value(sqlstate));
	out.SetValue(1, 0, Value(stage));
	out.SetValue(2, 0, Value(document));
	out.SetChildCardinality(1);
}

static unique_ptr<FunctionData> BindDDL(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                        vector<Identifier> &names) {
	auto dialect = DialectFromFn(in.table_function.GetName().GetIdentifierName());
	auto cond_it = in.named_parameters.find("conditional");
	const bool conditional = cond_it != in.named_parameters.end() && !cond_it->second.IsNull() &&
	                         cond_it->second.GetValue<bool>();
	auto intent = DDLIntent::Bind(dialect, in.inputs, conditional);
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, Identifier(DDLIntent::Text(in.inputs[0])));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		throw BinderException("ODBC DDL catalog is unavailable");
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != dialect)
		throw BinderException("ODBC DDL catalog dialect mismatch");
	auto d = make_uniq<DDLData>(catalog, dialect, std::move(intent));
	auto diag = in.named_parameters.find("source_diagnostics");
	d->diagnostics = diag != in.named_parameters.end() && !diag->second.IsNull() && diag->second.GetValue<bool>();
	names = {"operation_id", "status", "sqlstate", "stage", "warning_count"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::BIGINT};
	if (d->diagnostics) {
		names.push_back("source_error");
		types.push_back(LogicalType::VARCHAR);
	}
	return std::move(d);
}

static void ScanDDL(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<OneRowState>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<DDLData>();
	string product = ODBCDialectUtil::Name(d.dialect);
	string status = "CONFIRMED_NOT_EXECUTED", error, stage = "source_identity";
	GuardedODBCSourceError source;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<DDLSession> session;
	const bool transactional = d.dialect == ODBCDialect::DB2;
	bool sent = false, commit_sent = false;
	try {
		if (ctx.IsInterrupted()) {
			error = "57014";
			throw InterruptException();
		}
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		if (!SQL_SUCCEEDED(
		        SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0))) {
			error = "25006";
			throw IOException("ODBC DDL source is read-only");
		}
		if (transactional) {
			if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT,
			                                     reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0))) {
				error = "HY000";
				throw IOException("DB2 DDL transaction setup failed");
			}
		} else {
			if (!SQL_SUCCEEDED(
			        SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_ON), 0))) {
				error = "HY000";
				throw IOException("ODBC DDL autocommit setup failed");
			}
		}
		session = make_uniq<DDLSession>(ctx, dbc, product, d.intent.action == "reindex" ? 290 : 60);
		string version, db_name;
		if (!VerifyDialectIdentity(d.dialect, dbc, *session, version, db_name)) {
			error = "0A000";
			throw IOException("ODBC DDL source identity not certified");
		}
		if (d.dialect == ODBCDialect::DB2)
			session->TryExec("SET CURRENT LOCK TIMEOUT 5");

		const auto target = Quote(d.intent.schema) + "." + Quote(d.intent.name);
		bool idempotent_skip = false;
		if (d.intent.conditional) {
			auto count_query = [&](const string &sql, const vector<string> &params) -> bool {
				auto r = session->Query(sql, params);
				return r.size() == 1 && !r[0].empty() && r[0][0] != "0";
			};
			auto table_present = [&]() -> bool {
				if (d.dialect == ODBCDialect::DAMENG)
					return count_query("SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER=? AND TABLE_NAME=?",
					                   {d.intent.schema, d.intent.name});
				if (d.dialect == ODBCDialect::DB2)
					return count_query("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.TABLES WHERE TABSCHEMA=? AND TABNAME=?",
					                   {d.intent.schema, d.intent.name});
				return count_query(
				    "SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.TABLES WHERE SCHEMA_NAME=? AND TABLE_NAME=?",
				    {d.intent.schema, d.intent.name});
			};
			if (d.intent.action == "create_table" || d.intent.action == "create_table_as") {
				idempotent_skip = table_present();
			} else if (d.intent.action == "drop_table") {
				idempotent_skip = !table_present();
			} else if (d.intent.action == "create_schema" || d.intent.action == "drop_schema") {
				bool present = false;
				if (d.dialect == ODBCDialect::DAMENG)
					present = count_query("SELECT COUNT(*) FROM SYS.SYSOBJECTS WHERE TYPE$='SCH' AND NAME=?",
					                      {d.intent.name});
				else if (d.dialect == ODBCDialect::DB2)
					present = count_query("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME=?",
					                      {d.intent.name});
				else
					present = count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.SCHEMAS WHERE SCHEMA_NAME=?",
					                      {d.intent.name});
				idempotent_skip = (d.intent.action == "create_schema") ? present : !present;
			} else if (d.intent.action == "add_column" || d.intent.action == "drop_column") {
				if (table_present()) {
					const auto &cname = d.intent.action == "add_column" ? d.intent.column_specs[0].first
					                                                    : d.intent.column_name;
					bool col_present = false;
					if (d.dialect == ODBCDialect::DAMENG)
						col_present = count_query(
						    "SELECT COUNT(*) FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=?",
						    {d.intent.schema, d.intent.name, cname});
					else if (d.dialect == ODBCDialect::DB2)
						col_present = count_query(
						    "SELECT VARCHAR(COUNT(*)) FROM SYSCAT.COLUMNS WHERE TABSCHEMA=? AND TABNAME=? AND COLNAME=?",
						    {d.intent.schema, d.intent.name, cname});
					else
						col_present = count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.TABLE_COLUMNS WHERE "
						                          "SCHEMA_NAME=? AND TABLE_NAME=? AND COLUMN_NAME=?",
						                          {d.intent.schema, d.intent.name, cname});
					idempotent_skip = (d.intent.action == "add_column") ? col_present : !col_present;
				}
			} else if (d.intent.action == "create_index" || d.intent.action == "create_unique_index") {
				if (table_present()) {
					if (d.dialect == ODBCDialect::DAMENG)
						idempotent_skip = count_query(
						    "SELECT COUNT(*) FROM ALL_INDEXES WHERE OWNER=? AND INDEX_NAME=?",
						    {d.intent.schema, d.intent.index});
					else if (d.dialect == ODBCDialect::DB2)
						idempotent_skip = count_query(
						    "SELECT VARCHAR(COUNT(*)) FROM SYSCAT.INDEXES WHERE INDSCHEMA=? AND INDNAME=?",
						    {d.intent.schema, d.intent.index});
					else
						idempotent_skip = count_query(
						    "SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.INDEXES WHERE SCHEMA_NAME=? AND INDEX_NAME=?",
						    {d.intent.schema, d.intent.index});
				}
			} else if (d.intent.action == "drop_index") {
				if (d.dialect == ODBCDialect::DAMENG)
					idempotent_skip = !count_query("SELECT COUNT(*) FROM ALL_INDEXES WHERE OWNER=? AND INDEX_NAME=?",
					                               {d.intent.schema, d.intent.name});
				else if (d.dialect == ODBCDialect::DB2)
					idempotent_skip = !count_query(
					    "SELECT VARCHAR(COUNT(*)) FROM SYSCAT.INDEXES WHERE INDSCHEMA=? AND INDNAME=?",
					    {d.intent.schema, d.intent.name});
				else
					idempotent_skip = !count_query(
					    "SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.INDEXES WHERE SCHEMA_NAME=? AND INDEX_NAME=?",
					    {d.intent.schema, d.intent.name});
			} else if (d.intent.action == "drop_constraint") {
				if (table_present()) {
					if (d.dialect == ODBCDialect::DAMENG)
						idempotent_skip = !count_query("SELECT COUNT(*) FROM ALL_CONSTRAINTS WHERE OWNER=? AND "
						                               "TABLE_NAME=? AND CONSTRAINT_NAME=?",
						                               {d.intent.schema, d.intent.name, d.intent.constraint_name});
					else if (d.dialect == ODBCDialect::DB2)
						idempotent_skip = !count_query("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.TABCONST WHERE "
						                               "TABSCHEMA=? AND TABNAME=? AND CONSTNAME=?",
						                               {d.intent.schema, d.intent.name, d.intent.constraint_name});
					else
						idempotent_skip = !count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.CONSTRAINTS "
						                               "WHERE SCHEMA_NAME=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?",
						                               {d.intent.schema, d.intent.name, d.intent.constraint_name});
				}
			}
		}

		string native_sql = d.intent.sql;
		if (!idempotent_skip && d.dialect == ODBCDialect::HANA &&
		    (d.intent.action == "set_default" || d.intent.action == "drop_default" ||
		     d.intent.action == "set_not_null" || d.intent.action == "drop_not_null")) {
			auto rows = session->Query(
			    "SELECT DATA_TYPE_NAME,CAST(LENGTH AS VARCHAR(20)),COALESCE(CAST(SCALE AS VARCHAR(20)),'0') "
			    "FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME=? AND COLUMN_NAME=?",
			    {d.intent.schema, d.intent.name, d.intent.column_name});
			if (rows.size() != 1) {
				error = "42703";
				throw IOException("HANA DDL column unavailable");
			}
			auto col_type = FormatHanaColumnType(rows[0][0], rows[0][1], rows[0][2]);
			if (d.intent.action == "set_not_null" || d.intent.action == "drop_not_null") {
				native_sql = "ALTER TABLE " + target + " ALTER (" + Quote(d.intent.column_name) + " " + col_type +
				             (d.intent.action == "set_not_null" ? " NOT NULL)" : " NULL)");
			} else if (d.intent.action == "drop_default") {
				native_sql =
				    "ALTER TABLE " + target + " ALTER (" + Quote(d.intent.column_name) + " " + col_type + " DEFAULT NULL)";
			} else {
				string lit = DefaultLiteralSQL(d.dialect, d.intent.default_kind, d.intent.default_value);
				native_sql = "ALTER TABLE " + target + " ALTER (" + Quote(d.intent.column_name) + " " + col_type +
				             " DEFAULT " + lit + ")";
			}
		}

		if (idempotent_skip) {
			if (transactional)
				session->Exec("COMMIT");
			status = "COMMITTED";
			stage.clear();
		} else {
			stage = "source_execute";
			if (ctx.IsInterrupted()) {
				error = "57014";
				throw InterruptException();
			}
			sent = true;
			session->Exec(native_sql);
			if (d.dialect == ODBCDialect::DB2 && d.intent.action == "create_table_as") {
				session->Exec("INSERT INTO " + target + " " + d.intent.query_sql);
			}
			if (transactional) {
				stage = "source_commit";
				commit_sent = true;
				status = "UNKNOWN";
				session->Exec("COMMIT");
			}
			status = "COMMITTED";
			stage.clear();
		}
	} catch (...) {
		if (session) {
			source = session->source;
			if (error.empty())
				error = session->sqlstate;
		}
		if (error.empty())
			error = ctx.IsInterrupted() ? "57014" : "08006";
		if (sent && status != "COMMITTED") {
			if (transactional && !commit_sent && session && session->TryExec("ROLLBACK")) {
				status = "CONFIRMED_ROLLED_BACK";
			} else if (!transactional && error.size() == 5 &&
			           (error == "42S02" || error == "42S01" || error == "42S22" || error == "42S21" ||
			            error == "42000" || error == "42501" || error == "3F000" || error == "42P01" ||
			            error == "42P07" || error == "42703" || error == "42701" || error == "42704")) {
				status = "CONFIRMED_NOT_EXECUTED";
			} else {
				status = "UNKNOWN";
			}
		} else if (transactional && session) {
			session->TryExec("ROLLBACK");
		}
	}
	if (connection) {
		session.reset();
		connection->Close();
	}
	out.SetValue(0, 0, Value(d.intent.operation));
	out.SetValue(1, 0, Value(status));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value::BIGINT(0));
	if (d.diagnostics)
		out.SetValue(5, 0, Value(source.JSON()));
	out.SetChildCardinality(1);
}

} // namespace

void RegisterODBCGuardedDDL(ExtensionLoader &loader) {
	for (auto *name : {"dameng", "db2", "hana"}) {
		loader.RegisterFunction(TableFunction(Identifier(string(name) + "_ddl_capabilities"),
		                                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanCapabilities,
		                                      BindCapabilities, InitOne));
		TableFunction ddl(Identifier(string(name) + "_guarded_ddl"),
		                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                   LogicalType::ANY},
		                  ScanDDL, BindDDL, InitOne);
		ddl.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
			options.Add("source_diagnostics", LogicalType::BOOLEAN);
			options.Add("conditional", LogicalType::BOOLEAN);
		});
		loader.RegisterFunction(ddl);
	}
}

} // namespace duckdb
