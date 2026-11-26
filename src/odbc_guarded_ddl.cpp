//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_guarded_ddl.cpp
//
// Restricted native DDL and capability/describe introspection for Dameng, Db2,
// and SAP HANA with REORG_PENDING, lossless widening, dependency, and UTC
// session timezone safety guards.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "guarded_odbc_source_error.hpp"
#include "odbc_dialect.hpp"
#include "odbc_temporal_utils.hpp"
#include "storage/odbc_catalog.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace duckdb {
namespace {

struct DDLGuardReject : public std::runtime_error {
	string sqlstate;
	string stage;
	DDLGuardReject(string state_p, string stage_p, const string &msg)
	    : std::runtime_error(msg), sqlstate(std::move(state_p)), stage(std::move(stage_p)) {
	}
};

[[noreturn]] static void RejectGuard(const string &sqlstate, const string &stage, const string &msg) {
	throw DDLGuardReject(sqlstate, stage, msg);
}

static string Trimmed(string s) {
	StringUtil::Trim(s);
	return s;
}

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

static bool IsSignedDecimalLiteral(const string &s) {
	if (s.empty() || s == "-" || s == "+" || s == ".") {
		return false;
	}
	idx_t i = 0;
	if (s[0] == '-' || s[0] == '+') {
		i = 1;
		if (i >= s.size()) {
			return false;
		}
	}
	bool seen_digit = false;
	bool seen_dot = false;
	for (; i < s.size(); i++) {
		char c = s[i];
		if (c >= '0' && c <= '9') {
			seen_digit = true;
		} else if (c == '.' && !seen_dot) {
			seen_dot = true;
		} else {
			return false;
		}
	}
	return seen_digit;
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

static bool IsDamengSystemNotNullCheck(const string &col_name, const string &raw_expr) {
	string norm = Trimmed(raw_expr);
	while (norm.size() >= 2 && norm.front() == '(' && norm.back() == ')') {
		int bal = 0;
		bool in_quote = false;
		bool wrapped = true;
		for (idx_t idx = 0; idx < norm.size(); idx++) {
			char ch = norm[idx];
			if (in_quote) {
				if (ch == '"') {
					if (idx + 1 < norm.size() && norm[idx + 1] == '"') {
						idx++;
						continue;
					}
					in_quote = false;
				}
				continue;
			}
			if (ch == '"') {
				in_quote = true;
			} else if (ch == '(') {
				bal++;
			} else if (ch == ')') {
				bal--;
				if (bal == 0 && idx + 1 < norm.size()) {
					wrapped = false;
				}
			}
			if (!wrapped || bal < 0) {
				wrapped = false;
				break;
			}
		}
		if (!wrapped || bal != 0 || in_quote) {
			break;
		}
		norm = norm.substr(1, norm.size() - 2);
		StringUtil::Trim(norm);
	}
	const string upper = StringUtil::Upper(norm);
	const string suffix = " IS NOT NULL";
	if (upper.size() <= suffix.size() || !StringUtil::EndsWith(upper, suffix)) {
		return false;
	}
	string lhs = norm.substr(0, norm.size() - suffix.size());
	StringUtil::Trim(lhs);
	return lhs == col_name || lhs == Quote(col_name);
}

static int ExtractDamengParenPrecision(const string &dtype, const string &scale_str) {
	auto lp = dtype.find('(');
	auto rp = dtype.find(')');
	if (lp != string::npos && rp != string::npos && rp == lp + 2 && dtype[lp + 1] >= '0' && dtype[lp + 1] <= '6') {
		return dtype[lp + 1] - '0';
	}
	if (!scale_str.empty()) {
		try {
			int s = std::stoi(scale_str);
			if (s >= 0 && s <= 6) {
				return s;
			}
		} catch (...) {
		}
	}
	return 0;
}

static bool HanaCheckExprMentionsColumn(const string &expr, const string &col) {
	for (idx_t idx = 0; idx < expr.size();) {
		char ch = expr[idx];
		if (ch == '\'') {
			idx++;
			while (idx < expr.size()) {
				if (expr[idx] == '\'') {
					if (idx + 1 < expr.size() && expr[idx + 1] == '\'') {
						idx += 2;
						continue;
					}
					idx++;
					break;
				}
				idx++;
			}
		} else if (ch == '"') {
			idx++;
			string ident;
			while (idx < expr.size()) {
				if (expr[idx] == '"') {
					if (idx + 1 < expr.size() && expr[idx + 1] == '"') {
						ident.push_back('"');
						idx += 2;
						continue;
					}
					idx++;
					break;
				}
				ident.push_back(expr[idx]);
				idx++;
			}
			if (ident == col) {
				return true;
			}
		} else if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_') {
			idx_t start = idx;
			while (idx < expr.size() &&
			       ((expr[idx] >= 'A' && expr[idx] <= 'Z') || (expr[idx] >= 'a' && expr[idx] <= 'z') ||
			        (expr[idx] >= '0' && expr[idx] <= '9') || expr[idx] == '_' || expr[idx] == '$' ||
			        expr[idx] == '#')) {
				idx++;
			}
			string tok = expr.substr(start, idx - start);
			if (tok == col || StringUtil::Upper(tok) == StringUtil::Upper(col)) {
				return true;
			}
		} else {
			idx++;
		}
	}
	return false;
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

struct DDLColumnSpec {
	string name;
	string type;
	string native_type;
	int32_t length = 0;
	int32_t precision = 0;
	int32_t scale = 0;
	bool not_null = false;
	bool primary_key = false;
	bool unique = false;
	string default_kind;
	string default_value;
};

struct DDLIntent {
	int32_t contract_version = 2;
	bool conditional = false;
	string database, schema, operation, action, name, index, owner, new_name, column_name, query_sql, sql;
	vector<DDLColumnSpec> column_specs;
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
				int max_p = dialect == ODBCDialect::DB2 ? 31 : 38;
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
			DDLColumnSpec spec;
			spec.name = Text(cv[0]);
			spec.type = Text(cv[1]);
			spec.length = cv[2].GetValue<int32_t>();
			if (!Name(spec.name) || std::find(seen.begin(), seen.end(), spec.name) != seen.end())
				throw BinderException("DDL duplicate column is invalid");
			seen.push_back(spec.name);
			if (contract >= 5) {
				if (ct[3].first != "precision" || ct[4].first != "scale" || cv[3].IsNull() || cv[4].IsNull() ||
				    cv[3].type().id() != LogicalTypeId::INTEGER || cv[4].type().id() != LogicalTypeId::INTEGER)
					throw BinderException("DDL numeric/time modifiers are invalid");
				spec.precision = cv[3].GetValue<int32_t>();
				spec.scale = cv[4].GetValue<int32_t>();
			}
			spec.native_type = ColumnType(dialect, spec.type, spec.length, spec.precision, spec.scale, contract >= 5);
			string extra;
			if (contract == 6) {
				if (ct[5].first != "not_null" || ct[6].first != "default_kind" || ct[7].first != "default_value" ||
				    ct[8].first != "primary_key" || ct[9].first != "unique" || cv[5].IsNull() || cv[8].IsNull() ||
				    cv[9].IsNull() || cv[5].type().id() != LogicalTypeId::BOOLEAN ||
				    cv[8].type().id() != LogicalTypeId::BOOLEAN || cv[9].type().id() != LogicalTypeId::BOOLEAN)
					throw BinderException("DDL v6 column modifiers are invalid");
				spec.not_null = cv[5].GetValue<bool>();
				spec.default_kind = Text(cv[6]);
				spec.default_value = Text(cv[7]);
				spec.primary_key = cv[8].GetValue<bool>();
				spec.unique = cv[9].GetValue<bool>();
				if (!spec.default_kind.empty()) {
					if ((spec.not_null || spec.primary_key) && spec.default_kind == "null")
						throw BinderException("DDL NOT NULL column cannot default to NULL");
				} else if (!spec.default_value.empty()) {
					throw BinderException("DDL column default kind is missing");
				}
				if (dialect == ODBCDialect::DB2) {
					if (spec.not_null || spec.primary_key)
						extra += " NOT NULL";
					if (!spec.default_kind.empty())
						extra += " DEFAULT " + DefaultLiteralSQL(dialect, spec.default_kind, spec.default_value);
					if (spec.primary_key)
						extra += " PRIMARY KEY";
					if (spec.unique)
						extra += " UNIQUE";
				} else {
					if (!spec.default_kind.empty())
						extra += " DEFAULT " + DefaultLiteralSQL(dialect, spec.default_kind, spec.default_value);
					if (spec.not_null)
						extra += " NOT NULL";
					if (spec.primary_key)
						extra += " PRIMARY KEY";
					if (spec.unique)
						extra += " UNIQUE";
				}
			}
			d.columns.push_back(Quote(spec.name) + " " + spec.native_type + extra);
			d.column_specs.push_back(std::move(spec));
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
			if (!d.columns.empty() || !d.keys.empty() || !d.index.empty())
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
			if (d.column_specs.size() != 1 || d.column_name.empty() || d.column_specs[0].name != d.column_name)
				throw BinderException("DDL alter column type options are invalid");
			if (dialect == ODBCDialect::DAMENG)
				d.sql =
				    "ALTER TABLE " + target + " MODIFY " + Quote(d.column_name) + " " + d.column_specs[0].native_type;
			else if (dialect == ODBCDialect::DB2)
				d.sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(d.column_name) + " SET DATA TYPE " +
				        d.column_specs[0].native_type;
			else
				d.sql = "ALTER TABLE " + target + " ALTER (" + Quote(d.column_name) + " " +
				        d.column_specs[0].native_type + ")";
		} else if (d.action == "rename_index") {
			if (!d.columns.empty() || !d.keys.empty() || d.new_name.empty() || d.new_name == d.name)
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
				d.sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(d.column_name) + " DROP DEFAULT";
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
	int64_t last_warning_count = 0;
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
		if (rc == SQL_SUCCESS_WITH_INFO) {
			last_warning_count++;
		}
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
	auto u = StringUtil::Upper(Trimmed(data_type));
	if (u == "VARCHAR" || u == "NVARCHAR" || u == "ALPHANUM" || u == "VARBINARY" || u == "BINARY" || u == "CHAR" ||
	    u == "NCHAR")
		return u + "(" + Trimmed(len_s) + ")";
	if (u == "DECIMAL") {
		if (scale_s == "-1" || scale_s.empty()) {
			RejectGuard("0A000", "source_definition", "unconstrained HANA DECIMAL alter is unsupported");
		}
		return u + "(" + Trimmed(len_s) + "," + Trimmed(scale_s) + ")";
	}
	return u;
}

struct ServerFacts {
	string version;
	string database_name;
	string database_identity;
	string server_identity;
	string schema_id;
	string session_user;
	string current_schema;
	bool read_only = false;
};

static bool QueryDialectFacts(ODBCDialect dialect, SQLHDBC dbc, DDLSession &s, const string &schema,
                              ServerFacts &facts) {
	auto dbms = Info(dbc, SQL_DBMS_NAME);
	if (dialect == ODBCDialect::DAMENG) {
		if (StringUtil::Upper(dbms) != "DM DATABASE MANAGEMENT SYSTEM")
			return false;
		facts.version = Trimmed(Info(dbc, SQL_DBMS_VER));
		if (facts.version.empty())
			return false;
		auto modes = s.Query("SELECT PARA_VALUE FROM V$DM_INI WHERE PARA_NAME='COMPATIBLE_MODE'");
		if (modes.size() != 1 || Trimmed(modes[0][0]) != "0")
			return false;
		auto db_rows = s.Query("SELECT COALESCE(NAME,''), COALESCE(USER,''), "
		                       "COALESCE(SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),'') FROM V$DATABASE");
		auto inst_rows =
		    s.Query("SELECT COALESCE(INSTANCE_NAME,''), COALESCE(HOST_NAME,''), COALESCE(STATUS$,'') FROM V$INSTANCE");
		if (db_rows.size() == 1 && db_rows[0].size() == 3) {
			facts.database_name = Trimmed(db_rows[0][0]);
			facts.session_user = Trimmed(db_rows[0][1]);
			facts.current_schema = Trimmed(db_rows[0][2]);
		} else {
			facts.database_name = Trimmed(Info(dbc, SQL_DATABASE_NAME));
		}
		string inst_name, host_name;
		if (inst_rows.size() == 1 && inst_rows[0].size() == 3) {
			inst_name = Trimmed(inst_rows[0][0]);
			host_name = Trimmed(inst_rows[0][1]);
			facts.read_only = StringUtil::Upper(Trimmed(inst_rows[0][2])) != "OPEN";
		}
		facts.database_identity = inst_name.empty() ? facts.database_name : (facts.database_name + ":" + inst_name);
		facts.server_identity = inst_name.empty() ? host_name : (host_name + ":" + inst_name);
		auto sch_rows =
		    s.Query("SELECT COALESCE(MAX(CAST(ID AS VARCHAR(32))),'') FROM SYS.SYSOBJECTS WHERE TYPE$='SCH' AND NAME=?",
		            {schema});
		if (sch_rows.size() == 1 && !sch_rows[0].empty())
			facts.schema_id = Trimmed(sch_rows[0][0]);
		return !facts.version.empty() && !facts.database_name.empty();
	}
	if (dialect == ODBCDialect::DB2) {
		if (dbms.find("DB2/") != 0)
			return false;
		facts.version = Trimmed(Info(dbc, SQL_DBMS_VER));
		auto id_rows =
		    s.Query("SELECT RTRIM(CURRENT SERVER), RTRIM(CURRENT USER), RTRIM(CURRENT SCHEMA) FROM SYSIBM.SYSDUMMY1");
		if (id_rows.size() != 1 || id_rows[0].size() != 3)
			return false;
		facts.database_name = Trimmed(id_rows[0][0]);
		facts.session_user = Trimmed(id_rows[0][1]);
		facts.current_schema = Trimmed(id_rows[0][2]);
		string inst_name, host_name;
		try {
			auto inst_rows = s.Query("SELECT RTRIM(INST_NAME), RTRIM(SERVICE_LEVEL) FROM SYSIBMADM.ENV_INST_INFO");
			if (inst_rows.size() == 1 && inst_rows[0].size() == 2) {
				inst_name = Trimmed(inst_rows[0][0]);
				if (!Trimmed(inst_rows[0][1]).empty())
					facts.version = Trimmed(inst_rows[0][1]);
			}
			auto sys_rows = s.Query("SELECT RTRIM(HOST_NAME) FROM SYSIBMADM.ENV_SYS_INFO");
			if (sys_rows.size() == 1 && !sys_rows[0].empty())
				host_name = Trimmed(sys_rows[0][0]);
			auto hadr_rows = s.Query("SELECT COALESCE(VALUE,'') FROM SYSIBMADM.DBCFG WHERE NAME='hadr_db_role'");
			if (!hadr_rows.empty() && StringUtil::Upper(Trimmed(hadr_rows[0][0])) == "STANDBY")
				facts.read_only = true;
		} catch (...) {
		}
		facts.database_identity = inst_name.empty() ? facts.database_name : (facts.database_name + ":" + inst_name);
		facts.server_identity = inst_name.empty() ? host_name : (host_name + ":" + inst_name);
		auto sch_rows =
		    s.Query("SELECT RTRIM(SCHEMANAME), RTRIM(CHAR(CREATE_TIME)) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME=?",
		            {schema});
		if (sch_rows.size() == 1 && sch_rows[0].size() == 2)
			facts.schema_id = Trimmed(sch_rows[0][0]) + ":" + Trimmed(sch_rows[0][1]);
		return !facts.version.empty() && !facts.database_name.empty();
	}
	auto lower = StringUtil::Lower(dbms);
	if (lower.find("hdb") == string::npos && lower.find("hana") == string::npos)
		return false;
	auto db_rows = s.Query("SELECT SYSTEM_ID, DATABASE_NAME, HOST, VERSION FROM SYS.M_DATABASE");
	if (db_rows.size() != 1 || db_rows[0].size() != 4)
		return false;
	string sys_id = Trimmed(db_rows[0][0]);
	facts.database_name = Trimmed(db_rows[0][1]);
	string host_name = Trimmed(db_rows[0][2]);
	facts.version = Trimmed(db_rows[0][3]);
	if (facts.version.empty())
		facts.version = Trimmed(Info(dbc, SQL_DBMS_VER));
	facts.database_identity = sys_id + ":" + facts.database_name;
	facts.server_identity = host_name + ":" + sys_id;
	try {
		auto ctx_rows = s.Query("SELECT CURRENT_USER, CURRENT_SCHEMA FROM DUMMY");
		if (ctx_rows.size() == 1 && ctx_rows[0].size() == 2) {
			facts.session_user = Trimmed(ctx_rows[0][0]);
			facts.current_schema = Trimmed(ctx_rows[0][1]);
		}
	} catch (...) {
	}
	auto sch_rows = s.Query("SELECT SCHEMA_NAME, TO_VARCHAR(CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7') "
	                        "FROM SYS.SCHEMAS WHERE SCHEMA_NAME=?",
	                        {schema});
	if (sch_rows.size() == 1 && sch_rows[0].size() == 2)
		facts.schema_id = Trimmed(sch_rows[0][0]) + ":" + Trimmed(sch_rows[0][1]);
	return !facts.version.empty() && !facts.database_name.empty();
}

struct MetaData : TableFunctionData {
	ODBCCatalog &catalog;
	ODBCDialect dialect;
	string schema;
	string kind;
	string name;
	bool describe = false;
	MetaData(ODBCCatalog &c, ODBCDialect d, bool desc) : catalog(c), dialect(d), describe(desc) {
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

static unique_ptr<FunctionData> BindMetadata(ClientContext &ctx, TableFunctionBindInput &in,
                                             vector<LogicalType> &types, vector<Identifier> &names) {
	const bool describe = in.inputs.size() == 4;
	if ((in.inputs.size() != 2 && !describe))
		throw BinderException("ODBC DDL metadata input count is invalid");
	for (auto &v : in.inputs)
		if (v.IsNull())
			throw BinderException("ODBC DDL metadata input is incomplete");
	auto dialect = DialectFromFn(in.table_function.GetName().GetIdentifierName());
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, Identifier(in.inputs[0].GetValue<string>()));
	if (!db || db->GetCatalog().GetCatalogType() != "odbc_catalog")
		throw BinderException("ODBC DDL catalog is unavailable");
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	if (catalog.GetDialect() != dialect)
		throw BinderException("ODBC DDL catalog dialect mismatch");
	auto d = make_uniq<MetaData>(catalog, dialect, describe);
	d->schema = in.inputs[1].GetValue<string>();
	if (!DDLIntent::Name(d->schema))
		throw BinderException("ODBC DDL schema name is invalid");
	if (describe) {
		d->kind = in.inputs[2].GetValue<string>();
		d->name = in.inputs[3].GetValue<string>();
		if ((d->kind != "table" && d->kind != "index") || !DDLIntent::Name(d->name))
			throw BinderException("ODBC DDL describe target is invalid");
	}
	names = {"sqlstate", "stage", "document"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(d);
}

static void ScanMetadata(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<OneRowState>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<MetaData>();
	string product = ODBCDialectUtil::Name(d.dialect);
	string sqlstate, stage = "source_identity", document;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<DDLSession> session;
	try {
		if (ctx.IsInterrupted())
			throw InterruptException();
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, d.dialect), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		session = make_uniq<DDLSession>(ctx, dbc, product, 10);
		auto &s = *session;
		ServerFacts facts;
		if (!QueryDialectFacts(d.dialect, dbc, s, d.schema, facts)) {
			sqlstate = "0A000";
			throw IOException("ODBC DDL identity not certified");
		}
		stage = "source_metadata";
		SQLULEN mode = SQL_MODE_READ_WRITE;
		if (SQL_SUCCEEDED(SQLGetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, &mode, sizeof(mode), nullptr)) &&
		    mode == SQL_MODE_READ_ONLY)
			facts.read_only = true;
		const bool ns_present = !facts.schema_id.empty();

		if (!d.describe) {
			document = string("{") + "\"contract_version\":1,\"product\":" + JSON(product) +
			           ",\"source_version\":" + JSON(facts.version) + ",\"database\":" + JSON(facts.database_name) +
			           ",\"database_identity\":" + JSON(facts.database_identity) +
			           ",\"server_identity\":" + JSON(facts.server_identity) + ",\"schema\":" + JSON(d.schema) +
			           ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
			           ",\"source_read_only\":" + (facts.read_only ? "true" : "false") +
			           ",\"session_identity\":{\"user\":" + JSON(facts.session_user) +
			           ",\"current_schema\":" + JSON(facts.current_schema) +
			           "},\"visibility\":\"CATALOG_READ\",\"executor_available\":true" +
			           ",\"identity_protection\":\"UNVERIFIED\"}";
		} else {
			string presence = "ABSENT";
			string object_identity, observed_kind, owning_table, table_identity;
			bool has_reorg_flag = false;
			bool reorg_pending = false;
			string cols_json = "[]", indexes_json = "[]", constraints_json = "[]";
			if (ns_present && d.dialect == ODBCDialect::DB2) {
				if (d.kind == "table") {
					auto objs = s.Query(
					    "SELECT RTRIM(CHAR(t.TBSPACEID)),RTRIM(CHAR(t.TABLEID)),RTRIM(CHAR(t.CREATE_TIME)),t.TYPE,"
					    "COALESCE(a.REORG_PENDING,'N') FROM SYSCAT.TABLES t "
					    "LEFT JOIN SYSIBMADM.ADMINTABINFO a ON a.TABSCHEMA=t.TABSCHEMA AND a.TABNAME=t.TABNAME "
					    "WHERE t.TABSCHEMA=? AND t.TABNAME=? AND t.TYPE IN ('T','V')",
					    {d.schema, d.name});
					if (objs.size() == 1) {
						presence = "PRESENT";
						object_identity = objs[0][0] + ":" + objs[0][1] + ":" + objs[0][2];
						observed_kind = objs[0][3] == "V" ? "v" : "r";
						if (observed_kind == "r") {
							has_reorg_flag = true;
							reorg_pending = Trimmed(objs[0][4]) == "Y";
						}
						auto col_rows = s.Query(
						    "SELECT RTRIM(CHAR(c.COLNO + 1)),c.COLNAME,RTRIM(c.TYPENAME),"
						    "RTRIM(CHAR(c.LENGTH)),COALESCE(RTRIM(CHAR(c.SCALE)),'0'),c.NULLS,"
						    "CASE WHEN c.DEFAULT IS NOT NULL THEN '1' ELSE '0' END "
						    "FROM SYSCAT.COLUMNS c WHERE c.TABSCHEMA=? AND c.TABNAME=? ORDER BY c.COLNO",
						    {d.schema, d.name});
						cols_json = "[";
						for (idx_t idx = 0; idx < col_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								cols_json += ",";
							auto &r = col_rows[idx];
							cols_json += "{\"ordinal\":" + r[0] + ",\"name\":" + JSON(r[1]) +
							             ",\"type\":" + JSON(r[2]) + ",\"length\":" + (r[3].empty() ? "0" : r[3]) +
							             ",\"precision\":null,\"scale\":" + (r[4].empty() ? "0" : r[4]) +
							             ",\"nullable\":" + (r[5] == "Y" ? "true" : "false") +
							             ",\"has_default\":" + (r[6] == "1" ? "true" : "false") + "}";
						}
						cols_json += "]";

						auto idx_rows = s.Query(
						    "SELECT i.INDNAME,RTRIM(i.INDEXTYPE),i.UNIQUERULE "
						    "FROM SYSCAT.INDEXES i WHERE i.TABSCHEMA=? AND i.TABNAME=? ORDER BY i.INDNAME",
						    {d.schema, d.name});
						indexes_json = "[";
						for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								indexes_json += ",";
							auto &r = idx_rows[idx];
							bool uniq = r[2] == "U" || r[2] == "P";
							indexes_json += "{\"table\":" + JSON(d.name) + ",\"name\":" + JSON(r[0]) +
							                ",\"type\":" + JSON(r[1]) + ",\"unique\":" + (uniq ? "true" : "false") +
							                "}";
						}
						indexes_json += "]";

						auto con_rows = s.Query(
						    "SELECT c.CONSTNAME,c.TYPE,c.ENFORCED,c.CHECKEXISTINGDATA "
						    "FROM SYSCAT.TABCONST c WHERE c.TABSCHEMA=? AND c.TABNAME=? "
						    "AND c.TYPE IN ('P','U','F','K') ORDER BY c.CONSTNAME",
						    {d.schema, d.name});
						constraints_json = "[";
						for (idx_t idx = 0; idx < con_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								constraints_json += ",";
							auto &r = con_rows[idx];
							constraints_json += "{\"name\":" + JSON(r[0]) + ",\"type\":" + JSON(r[1]) +
							                    ",\"enforced\":" + JSON(r[2]) +
							                    ",\"check_existing_data\":" + JSON(r[3]) + "}";
						}
						constraints_json += "]";
					}
				} else if (d.kind == "index") {
					auto idx_rows = s.Query(
					    "SELECT RTRIM(CHAR(i.TBSPACEID)),RTRIM(CHAR(i.INDEX_OBJECTID)),RTRIM(CHAR(i.IID)),"
					    "i.TABNAME,i.UNIQUERULE,RTRIM(CHAR(t.TBSPACEID)),RTRIM(CHAR(t.TABLEID)),"
					    "RTRIM(CHAR(t.CREATE_TIME)) FROM SYSCAT.INDEXES i "
					    "JOIN SYSCAT.TABLES t ON t.TABSCHEMA=i.TABSCHEMA AND t.TABNAME=i.TABNAME "
					    "WHERE i.INDSCHEMA=? AND i.INDNAME=? AND i.TABSCHEMA=? ORDER BY i.TABNAME",
					    {d.schema, d.name, d.schema});
					indexes_json = "[";
					for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
						if (idx > 0)
							indexes_json += ",";
						auto &r = idx_rows[idx];
						bool uniq = r[4] == "U" || r[4] == "P";
						indexes_json += "{\"table\":" + JSON(r[3]) + ",\"name\":" + JSON(d.name) +
						                ",\"unique\":" + (uniq ? "true" : "false") + "}";
					}
					indexes_json += "]";
					if (idx_rows.size() == 1) {
						presence = "PRESENT";
						object_identity = idx_rows[0][0] + ":" + idx_rows[0][1] + ":" + idx_rows[0][2];
						owning_table = idx_rows[0][3];
						table_identity = idx_rows[0][5] + ":" + idx_rows[0][6] + ":" + idx_rows[0][7];
						observed_kind = "i";
					} else if (idx_rows.size() > 1) {
						presence = "UNKNOWN";
					}
				}
			} else if (ns_present && d.dialect == ODBCDialect::DAMENG) {
				if (d.kind == "table") {
					auto objs = s.Query(
					    "SELECT CAST(o.OBJECT_ID AS VARCHAR(32)),o.OBJECT_TYPE FROM ALL_OBJECTS o "
					    "WHERE o.OWNER=? AND o.OBJECT_NAME=? AND o.OBJECT_TYPE IN ('TABLE','VIEW') "
					    "AND o.SUBOBJECT_NAME IS NULL",
					    {d.schema, d.name});
					if (objs.size() == 1) {
						presence = "PRESENT";
						object_identity = objs[0][0];
						observed_kind = objs[0][1] == "VIEW" ? "v" : "r";
						auto col_rows = s.Query(
						    "SELECT CAST(c.COLUMN_ID AS VARCHAR(20)),c.COLUMN_NAME,c.DATA_TYPE,"
						    "COALESCE(CAST(c.DATA_LENGTH AS VARCHAR(20)),'0'),"
						    "COALESCE(CAST(c.DATA_PRECISION AS VARCHAR(20)),''),"
						    "COALESCE(CAST(c.DATA_SCALE AS VARCHAR(20)),''),c.NULLABLE,"
						    "CASE WHEN COALESCE(c.DEFAULT_LENGTH,0)>0 THEN '1' ELSE '0' END "
						    "FROM ALL_TAB_COLUMNS c WHERE c.OWNER=? AND c.TABLE_NAME=? ORDER BY c.COLUMN_ID",
						    {d.schema, d.name});
						cols_json = "[";
						for (idx_t idx = 0; idx < col_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								cols_json += ",";
							auto &r = col_rows[idx];
							cols_json += "{\"ordinal\":" + r[0] + ",\"name\":" + JSON(r[1]) +
							             ",\"type\":" + JSON(r[2]) + ",\"length\":" + r[3] +
							             ",\"precision\":" + (r[4].empty() ? "null" : r[4]) +
							             ",\"scale\":" + (r[5].empty() ? "null" : r[5]) +
							             ",\"nullable\":" + (r[6] == "Y" ? "true" : "false") +
							             ",\"has_default\":" + (r[7] == "1" ? "true" : "false") + "}";
						}
						cols_json += "]";

						auto idx_rows = s.Query(
						    "SELECT i.INDEX_NAME,i.INDEX_TYPE,i.UNIQUENESS,i.STATUS FROM ALL_INDEXES i "
						    "WHERE i.TABLE_OWNER=? AND i.TABLE_NAME=? ORDER BY i.INDEX_NAME",
						    {d.schema, d.name});
						indexes_json = "[";
						for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								indexes_json += ",";
							auto &r = idx_rows[idx];
							indexes_json += "{\"table\":" + JSON(d.name) + ",\"name\":" + JSON(r[0]) +
							                ",\"type\":" + JSON(r[1]) +
							                ",\"unique\":" + (r[2] == "UNIQUE" ? "true" : "false") +
							                ",\"status\":" + JSON(r[3]) + "}";
						}
						indexes_json += "]";

						auto con_rows = s.Query(
						    "SELECT c.CONSTRAINT_NAME,c.CONSTRAINT_TYPE,c.STATUS,c.VALIDATED "
						    "FROM ALL_CONSTRAINTS c WHERE c.OWNER=? AND c.TABLE_NAME=? "
						    "AND c.CONSTRAINT_TYPE IN ('P','U','R','C') ORDER BY c.CONSTRAINT_NAME",
						    {d.schema, d.name});
						constraints_json = "[";
						for (idx_t idx = 0; idx < con_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								constraints_json += ",";
							auto &r = con_rows[idx];
							constraints_json += "{\"name\":" + JSON(r[0]) + ",\"type\":" + JSON(r[1]) +
							                    ",\"status\":" + JSON(r[2]) + ",\"validated\":" + JSON(r[3]) + "}";
						}
						constraints_json += "]";
					}
				} else if (d.kind == "index") {
					auto idx_rows = s.Query(
					    "SELECT CAST(o.OBJECT_ID AS VARCHAR(32)),i.TABLE_NAME,CAST(t.OBJECT_ID AS VARCHAR(32)),"
					    "i.UNIQUENESS FROM ALL_INDEXES i "
					    "JOIN ALL_OBJECTS o ON o.OWNER=i.OWNER AND o.OBJECT_NAME=i.INDEX_NAME AND o.OBJECT_TYPE='INDEX' "
					    "JOIN ALL_OBJECTS t ON t.OWNER=i.TABLE_OWNER AND t.OBJECT_NAME=i.TABLE_NAME AND "
					    "t.OBJECT_TYPE='TABLE' WHERE i.OWNER=? AND i.INDEX_NAME=? AND o.SUBOBJECT_NAME IS NULL "
					    "AND t.SUBOBJECT_NAME IS NULL ORDER BY i.TABLE_NAME",
					    {d.schema, d.name});
					indexes_json = "[";
					for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
						if (idx > 0)
							indexes_json += ",";
						auto &r = idx_rows[idx];
						indexes_json += "{\"table\":" + JSON(r[1]) + ",\"name\":" + JSON(d.name) +
						                ",\"unique\":" + (r[3] == "UNIQUE" ? "true" : "false") + "}";
					}
					indexes_json += "]";
					if (idx_rows.size() == 1) {
						presence = "PRESENT";
						object_identity = idx_rows[0][0];
						owning_table = idx_rows[0][1];
						table_identity = idx_rows[0][2];
						observed_kind = "i";
					} else if (idx_rows.size() > 1) {
						presence = "UNKNOWN";
					}
				}
			} else if (ns_present && d.dialect == ODBCDialect::HANA) {
				if (d.kind == "table") {
					auto tbls = s.Query(
					    "SELECT TO_VARCHAR(TABLE_OID),TABLE_TYPE,TO_VARCHAR(CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7') "
					    "FROM SYS.TABLES WHERE SCHEMA_NAME=? AND TABLE_NAME=?",
					    {d.schema, d.name});
					if (tbls.size() == 1) {
						presence = "PRESENT";
						object_identity = tbls[0][0] + ":" + tbls[0][2];
						observed_kind = "r";
						auto col_rows = s.Query(
						    "SELECT TO_VARCHAR(POSITION),COLUMN_NAME,DATA_TYPE_NAME,"
						    "COALESCE(TO_VARCHAR(LENGTH),'0'),COALESCE(TO_VARCHAR(SCALE),'0'),IS_NULLABLE,"
						    "CASE WHEN DEFAULT_VALUE IS NOT NULL THEN '1' ELSE '0' END "
						    "FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME=? ORDER BY POSITION",
						    {d.schema, d.name});
						cols_json = "[";
						for (idx_t idx = 0; idx < col_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								cols_json += ",";
							auto &r = col_rows[idx];
							cols_json += "{\"ordinal\":" + r[0] + ",\"name\":" + JSON(r[1]) +
							             ",\"type\":" + JSON(r[2]) + ",\"length\":" + r[3] + ",\"scale\":" + r[4] +
							             ",\"nullable\":" + (r[5] == "TRUE" ? "true" : "false") +
							             ",\"has_default\":" + (r[6] == "1" ? "true" : "false") + "}";
						}
						cols_json += "]";

						auto idx_rows = s.Query(
						    "SELECT INDEX_NAME,INDEX_TYPE,COALESCE(CONSTRAINT,'') "
						    "FROM SYS.INDEXES WHERE SCHEMA_NAME=? AND TABLE_NAME=? ORDER BY INDEX_NAME",
						    {d.schema, d.name});
						indexes_json = "[";
						for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								indexes_json += ",";
							auto &r = idx_rows[idx];
							bool uniq = r[2] == "PRIMARY KEY" || r[2] == "UNIQUE" || r[2] == "NOT NULL UNIQUE" ||
							            StringUtil::EndsWith(r[1], " UNIQUE");
							indexes_json += "{\"table\":" + JSON(d.name) + ",\"name\":" + JSON(r[0]) +
							                ",\"type\":" + JSON(r[1]) + ",\"unique\":" + (uniq ? "true" : "false") +
							                "}";
						}
						indexes_json += "]";

						auto con_rows = s.Query(
						    "SELECT CONSTRAINT_NAME, CASE WHEN MAX(IS_PRIMARY_KEY)='TRUE' THEN 'P' "
						    "WHEN MAX(IS_UNIQUE_KEY)='TRUE' THEN 'U' ELSE 'C' END AS KIND "
						    "FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=? AND TABLE_NAME=? GROUP BY CONSTRAINT_NAME "
						    "UNION ALL SELECT CONSTRAINT_NAME,'F' FROM SYS.REFERENTIAL_CONSTRAINTS "
						    "WHERE SCHEMA_NAME=? AND TABLE_NAME=? ORDER BY 1",
						    {d.schema, d.name, d.schema, d.name});
						constraints_json = "[";
						for (idx_t idx = 0; idx < con_rows.size() && idx < 256; idx++) {
							if (idx > 0)
								constraints_json += ",";
							auto &r = con_rows[idx];
							constraints_json += "{\"name\":" + JSON(r[0]) + ",\"type\":" + JSON(r[1]) + "}";
						}
						constraints_json += "]";
					} else {
						auto views =
						    s.Query("SELECT TO_VARCHAR(VIEW_OID) FROM SYS.VIEWS WHERE SCHEMA_NAME=? AND VIEW_NAME=?",
						            {d.schema, d.name});
						if (views.size() == 1) {
							presence = "PRESENT";
							object_identity = views[0][0];
							observed_kind = "v";
						}
					}
				} else if (d.kind == "index") {
					auto idx_rows = s.Query(
					    "SELECT TO_VARCHAR(i.INDEX_OID),i.TABLE_NAME,TO_VARCHAR(t.TABLE_OID),"
					    "TO_VARCHAR(t.CREATE_TIME,'YYYY-MM-DD HH24:MI:SS.FF7'),i.INDEX_TYPE,COALESCE(i.CONSTRAINT,'') "
					    "FROM SYS.INDEXES i JOIN SYS.TABLES t ON t.SCHEMA_NAME=i.SCHEMA_NAME AND t.TABLE_NAME=i.TABLE_NAME "
					    "WHERE i.SCHEMA_NAME=? AND i.INDEX_NAME=? ORDER BY i.TABLE_NAME",
					    {d.schema, d.name});
					indexes_json = "[";
					for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
						if (idx > 0)
							indexes_json += ",";
						auto &r = idx_rows[idx];
						bool uniq = r[5] == "PRIMARY KEY" || r[5] == "UNIQUE" || r[5] == "NOT NULL UNIQUE" ||
						            StringUtil::EndsWith(r[4], " UNIQUE");
						indexes_json += "{\"table\":" + JSON(r[1]) + ",\"name\":" + JSON(d.name) +
						                ",\"unique\":" + (uniq ? "true" : "false") + "}";
					}
					indexes_json += "]";
					if (idx_rows.size() == 1) {
						presence = "PRESENT";
						object_identity = idx_rows[0][0];
						owning_table = idx_rows[0][1];
						table_identity = idx_rows[0][2] + ":" + idx_rows[0][3];
						observed_kind = "i";
					} else if (idx_rows.size() > 1) {
						presence = "UNKNOWN";
					}
				}
			}
			document = string("{") + "\"contract_version\":1,\"product\":" + JSON(product) +
			           ",\"source_version\":" + JSON(facts.version) + ",\"database\":" + JSON(facts.database_name) +
			           ",\"database_identity\":" + JSON(facts.database_identity) +
			           ",\"server_identity\":" + JSON(facts.server_identity) + ",\"schema\":" + JSON(d.schema) +
			           ",\"name\":" + JSON(d.name) + ",\"kind\":" + JSON(d.kind) +
			           ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
			           ",\"presence\":" + JSON(presence) + ",\"object_identity\":" + JSON(object_identity);
			if (!observed_kind.empty())
				document += ",\"observed_kind\":" + JSON(observed_kind);
			if (!owning_table.empty())
				document += ",\"owning_table\":" + JSON(owning_table);
			if (!table_identity.empty())
				document += ",\"table_identity\":" + JSON(table_identity);
			if (has_reorg_flag)
				document += ",\"reorg_pending\":" + string(reorg_pending ? "true" : "false");
			document += ",\"metadata_complete\":false,\"identity_protection\":\"UNVERIFIED\",\"columns\":" + cols_json +
			            ",\"indexes\":" + indexes_json + ",\"constraints\":" + constraints_json + "}";
		}
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
	const auto &intent = d.intent;
	string product = ODBCDialectUtil::Name(d.dialect);
	string status = "CONFIRMED_NOT_EXECUTED", error, stage = "source_identity";
	int64_t warning_count = 0;
	GuardedODBCSourceError source;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<DDLSession> session;
	const bool transactional = d.dialect == ODBCDialect::DB2;
	bool sent = false, commit_sent = false;
	try {
		if (ctx.IsInterrupted()) {
			RejectGuard("57014", "source_identity", "operation cancelled");
		}
		connection = ODBCConnection::Open(ODBCTypeConfig(ctx, d.dialect), d.catalog.connection_string, 5);
		auto dbc = connection->GetConn();
		if (!SQL_SUCCEEDED(
		        SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0))) {
			RejectGuard("25006", "source_identity", "ODBC DDL source is read-only");
		}
		if (transactional) {
			if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT,
			                                     reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0))) {
				RejectGuard("HY000", "source_identity", "DB2 DDL transaction setup failed");
			}
		} else {
			if (!SQL_SUCCEEDED(
			        SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_ON), 0))) {
				RejectGuard("HY000", "source_identity", "ODBC DDL autocommit setup failed");
			}
		}
		session = make_uniq<DDLSession>(ctx, dbc, product, intent.action == "reindex" ? 290 : 60);
		auto &s = *session;
		ServerFacts facts;
		if (!QueryDialectFacts(d.dialect, dbc, s, intent.schema, facts)) {
			RejectGuard("0A000", "source_identity", "ODBC DDL source identity not certified");
		}
		if (facts.read_only) {
			RejectGuard("25006", "source_identity", "ODBC DDL source is read-only");
		}

		// Per-vendor session guards
		if (d.dialect == ODBCDialect::DB2) {
			s.Exec("SET CURRENT LOCK TIMEOUT 2");
			s.Exec("COMMIT");
		} else if (d.dialect == ODBCDialect::DAMENG) {
			// Fail-fast on active lock contention and pin UTC session timezone for temporal DDL.
			s.Exec("ALTER SESSION SET 'DDL_WAIT_TIME' = 2");
			s.Exec(ODBC_DAMENG_SESSION_TEMPORAL_SQL);
		} else {
			s.TryExec("SET TRANSACTION LOCK WAIT TIMEOUT 5000");
		}

		stage = "source_definition";
		const auto target = Quote(intent.schema) + "." + Quote(intent.name);
		string native_sql = intent.sql;
		bool source_noop = false;
		bool no_op_commit = false;
		bool is_db2_truncate = false;
		vector<string> lock_tables;
		auto add_lock_table = [&](const string &tbl) {
			if (!tbl.empty() && std::find(lock_tables.begin(), lock_tables.end(), tbl) == lock_tables.end()) {
				lock_tables.push_back(tbl);
			}
		};

		auto count_query = [&](const string &sql, const vector<string> &params) -> bool {
			auto r = s.Query(sql, params);
			return r.size() == 1 && !r[0].empty() && Trimmed(r[0][0]) != "0";
		};

		if (d.dialect == ODBCDialect::DB2) {
			auto db2_table_row = [&](const string &tbl) {
				return s.Query("SELECT t.TYPE,t.STATUS,COALESCE(t.TABLEORG,''),COALESCE(a.REORG_PENDING,'N'),"
				               "COALESCE(a.READ_ACCESS_ONLY,'N') FROM SYSCAT.TABLES t "
				               "LEFT JOIN SYSIBMADM.ADMINTABINFO a ON a.TABSCHEMA=t.TABSCHEMA AND a.TABNAME=t.TABNAME "
				               "WHERE t.TABSCHEMA=? AND t.TABNAME=?",
				               {intent.schema, tbl});
			};
			auto check_db2_table = [&](const string &tbl) {
				auto rows = db2_table_row(tbl);
				if (rows.empty())
					RejectGuard("42P01", "source_definition", "target table does not exist");
				const auto &r = rows[0];
				if (r[0] != "T")
					RejectGuard("42809", "source_definition", "target object is not a base table");
				if (Trimmed(r[3]) == "Y")
					RejectGuard("0A000", "source_definition", "Db2 target table is in REORG PENDING state");
				if (Trimmed(r[4]) == "Y")
					RejectGuard("25006", "source_definition", "Db2 target table is read-access-only");
				if (r[1] != "N" || Trimmed(r[2]) != "R")
					RejectGuard("0A000", "source_definition", "Db2 target table state or organization is unsupported");
			};
			auto db2_col_info = [&](const string &tbl, const string &col) {
				return s.Query("SELECT NULLS,RTRIM(TYPENAME),RTRIM(CHAR(LENGTH)),IDENTITY,COALESCE(RTRIM(GENERATED),''),"
				               "CASE WHEN DEFAULT IS NOT NULL THEN '1' ELSE '0' END "
				               "FROM SYSCAT.COLUMNS WHERE TABSCHEMA=? AND TABNAME=? AND COLNAME=?",
				               {intent.schema, tbl, col});
			};
			auto db2_has_ref_fk = [&](const string &tbl, bool exclude_self) {
				if (exclude_self) {
					return count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.REFERENCES WHERE REFTABSCHEMA=? "
					                   "AND REFTABNAME=? AND NOT (TABSCHEMA=? AND TABNAME=?)",
					                   {intent.schema, tbl, intent.schema, tbl});
				}
				return count_query(
				    "SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.REFERENCES WHERE REFTABSCHEMA=? AND REFTABNAME=?",
				    {intent.schema, tbl});
			};
			auto check_db2_col_deps = [&](const string &tbl, const string &col) {
				if (count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.INDEXCOLUSE ic "
				                "JOIN SYSCAT.INDEXES ix ON ix.INDSCHEMA=ic.INDSCHEMA AND ix.INDNAME=ic.INDNAME "
				                "WHERE ix.TABSCHEMA=? AND ix.TABNAME=? AND ic.COLNAME=?",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition", "cannot modify column referenced by an index");
				if (count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.KEYCOLUSE WHERE TABSCHEMA=? AND TABNAME=? "
				                "AND COLNAME=?",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition", "cannot modify column referenced by a key constraint");
				if (count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.COLCHECKS WHERE TABSCHEMA=? AND TABNAME=? "
				                "AND COLNAME=?",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition", "cannot modify column referenced by a check constraint");
				if (count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TABDEP WHERE BSCHEMA=? AND BNAME=? "
				                "AND BTYPE='T' AND DTYPE IN ('V','S')",
				                {intent.schema, tbl}))
					RejectGuard("2BP01", "source_definition", "cannot modify column of table referenced by a view");
			};

			if (intent.action == "drop_column") {
				RejectGuard("0A000", "source_definition", "Db2 DROP COLUMN triggers REORG PENDING and is unsupported");
			} else if (intent.action == "set_not_null" || intent.action == "drop_not_null") {
				RejectGuard("0A000", "source_definition",
				            "Db2 SET/DROP NOT NULL triggers REORG PENDING and is unsupported");
			} else if (intent.action == "create_schema" || intent.action == "drop_schema") {
				bool present = count_query("SELECT VARCHAR(COUNT(*)) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME=?",
				                           {intent.name});
				if (intent.conditional && ((intent.action == "create_schema") ? present : !present))
					source_noop = true;
			} else if (intent.action == "create_table" || intent.action == "create_table_as") {
				auto existing = db2_table_row(intent.name);
				if (!existing.empty()) {
					if (intent.conditional && existing[0][0] == "T")
						source_noop = true;
					else
						RejectGuard("42P07", "source_definition", "target table already exists");
				}
			} else if (intent.action == "drop_table") {
				if (db2_table_row(intent.name).empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42P01", "source_definition", "target table does not exist");
				} else {
					check_db2_table(intent.name);
					if (db2_has_ref_fk(intent.name, true))
						RejectGuard("2BP01", "source_definition", "cannot drop table with referencing foreign keys");
					add_lock_table(intent.name);
				}
			} else if (intent.action == "rename_table") {
				check_db2_table(intent.name);
				if (!db2_table_row(intent.new_name).empty())
					RejectGuard("42P07", "source_definition", "destination table already exists");
				if (db2_has_ref_fk(intent.name, false))
					RejectGuard("2BP01", "source_definition", "cannot rename table referenced by foreign keys");
				add_lock_table(intent.name);
			} else if (intent.action == "add_column") {
				check_db2_table(intent.name);
				const auto &c = intent.column_specs[0];
				if (!db2_col_info(intent.name, c.name).empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42701", "source_definition", "column already exists");
				} else {
					if (c.not_null && (c.default_kind.empty() || c.default_kind == "null")) {
						if (count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM (SELECT 1 FROM " + target +
						                    " FETCH FIRST 1 ROW ONLY) t",
						                {}))
							RejectGuard("23502", "source_definition",
							            "cannot add NOT NULL column without default to non-empty table");
						RejectGuard("0A000", "source_definition",
						            "Db2 ADD COLUMN NOT NULL requires an explicit DEFAULT");
					}
					add_lock_table(intent.name);
				}
			} else if (intent.action == "alter_column_type") {
				check_db2_table(intent.name);
				auto crows = db2_col_info(intent.name, intent.column_name);
				if (crows.empty())
					RejectGuard("42703", "source_definition", "column does not exist");
				const auto &r = crows[0];
				if (r[3] == "Y" || !Trimmed(r[4]).empty())
					RejectGuard("0A000", "source_definition", "cannot alter identity or generated column");
				const string cur_type = StringUtil::Upper(Trimmed(r[1]));
				const int cur_len = std::stoi(Trimmed(r[2]));
				const auto &tc = intent.column_specs[0];
				if (cur_type != "VARCHAR" || tc.type != "varchar" || tc.precision != 0 || tc.scale != 0 ||
				    tc.length <= cur_len || tc.length > 32672) {
					RejectGuard("0A000", "source_definition",
					            "Db2 ALTER COLUMN TYPE only supports REORG-free VARCHAR length widening");
				}
				check_db2_col_deps(intent.name, intent.column_name);
				native_sql = "ALTER TABLE " + target + " ALTER COLUMN " + Quote(intent.column_name) +
				             " SET DATA TYPE VARCHAR(" + std::to_string(tc.length) + ")";
				add_lock_table(intent.name);
			} else if (intent.action == "rename_column") {
				check_db2_table(intent.name);
				if (db2_col_info(intent.name, intent.column_name).empty())
					RejectGuard("42703", "source_definition", "column does not exist");
				if (!db2_col_info(intent.name, intent.new_name).empty())
					RejectGuard("42701", "source_definition", "destination column already exists");
				add_lock_table(intent.name);
			} else if (intent.action == "set_default" || intent.action == "drop_default") {
				check_db2_table(intent.name);
				auto crows = db2_col_info(intent.name, intent.column_name);
				if (crows.empty())
					RejectGuard("42703", "source_definition", "column does not exist");
				if (crows[0][3] == "Y" || !Trimmed(crows[0][4]).empty())
					RejectGuard("0A000", "source_definition", "cannot alter default on identity or generated column");
				if (intent.action == "drop_default" && crows[0][5] == "0") {
					no_op_commit = true;
				} else {
					add_lock_table(intent.name);
				}
			} else if (intent.action == "add_constraint") {
				check_db2_table(intent.name);
				if (intent.constraint_kind == "foreign_key") {
					check_db2_table(intent.reference_table);
					add_lock_table(intent.reference_table);
				}
				add_lock_table(intent.name);
			} else if (intent.action == "drop_constraint") {
				check_db2_table(intent.name);
				auto crows = s.Query(
				    "SELECT TYPE FROM SYSCAT.TABCONST WHERE TABSCHEMA=? AND TABNAME=? AND CONSTNAME=?",
				    {intent.schema, intent.name, intent.constraint_name});
				if (crows.empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42704", "source_definition", "constraint does not exist");
				} else {
					if ((crows[0][0] == "P" || crows[0][0] == "U") &&
					    count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.REFERENCES WHERE REFTABSCHEMA=? "
					                "AND REFTABNAME=? AND REFKEYNAME=?",
					                {intent.schema, intent.name, intent.constraint_name}))
						RejectGuard("2BP01", "source_definition",
						            "cannot drop constraint with referencing foreign keys");
					add_lock_table(intent.name);
				}
			} else if (intent.action == "create_index" || intent.action == "create_unique_index") {
				check_db2_table(intent.name);
				auto idx_rows = s.Query("SELECT TABSCHEMA,TABNAME FROM SYSCAT.INDEXES WHERE INDSCHEMA=? AND INDNAME=?",
				                        {intent.schema, intent.index});
				if (!idx_rows.empty()) {
					if (intent.conditional && idx_rows.size() == 1 && Trimmed(idx_rows[0][0]) == intent.schema &&
					    idx_rows[0][1] == intent.name)
						source_noop = true;
					else
						RejectGuard("42P07", "source_definition", "index name already exists in schema");
				} else {
					add_lock_table(intent.name);
				}
			} else if (intent.action == "drop_index") {
				auto idx_rows = s.Query(
				    "SELECT TABNAME,UNIQUERULE FROM SYSCAT.INDEXES WHERE INDSCHEMA=? AND INDNAME=? AND TABSCHEMA=?",
				    {intent.schema, intent.name, intent.schema});
				if (idx_rows.empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42704", "source_definition", "index does not exist");
				} else if (!intent.owner.empty() && idx_rows[0][0] != intent.owner) {
					RejectGuard("42704", "source_definition", "index owner mismatch");
				} else {
					if (idx_rows[0][1] == "P" ||
					    count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.CONSTDEP WHERE TABSCHEMA=? AND TABNAME=? "
					                "AND BSCHEMA=? AND BNAME=? AND BTYPE='I'",
					                {intent.schema, idx_rows[0][0], intent.schema, intent.name}))
						RejectGuard("2BP01", "source_definition", "cannot drop constraint backing index directly");
					check_db2_table(idx_rows[0][0]);
					add_lock_table(idx_rows[0][0]);
				}
			} else if (intent.action == "rename_index") {
				auto idx_rows = s.Query(
				    "SELECT TABNAME FROM SYSCAT.INDEXES WHERE INDSCHEMA=? AND INDNAME=? AND TABSCHEMA=?",
				    {intent.schema, intent.name, intent.schema});
				if (idx_rows.empty())
					RejectGuard("42704", "source_definition", "index does not exist");
				if (!intent.owner.empty() && idx_rows[0][0] != intent.owner)
					RejectGuard("42704", "source_definition", "index owner mismatch");
				check_db2_table(idx_rows[0][0]);
				add_lock_table(idx_rows[0][0]);
			} else if (intent.action == "truncate") {
				if (intent.identity_mode == "restart")
					RejectGuard("0A000", "source_definition", "Db2 TRUNCATE RESTART IDENTITY is unsupported");
				check_db2_table(intent.name);
				if (db2_has_ref_fk(intent.name, false))
					RejectGuard("2BP01", "source_definition", "cannot truncate table with referencing foreign keys");
				if (count_query("SELECT RTRIM(CHAR(COUNT(*))) FROM SYSCAT.TRIGGERS WHERE TABSCHEMA=? AND TABNAME=? "
				                "AND TRIGEVENT='D'",
				                {intent.schema, intent.name}))
					RejectGuard("0A000", "source_definition", "cannot truncate table with DELETE triggers");
				is_db2_truncate = true;
			}
		} else if (d.dialect == ODBCDialect::DAMENG) {
			auto dm_table_exists = [&](const string &tbl) {
				return count_query("SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER=? AND TABLE_NAME=?",
				                   {intent.schema, tbl});
			};
			auto dm_object_exists = [&](const string &obj) {
				return count_query(
				    "SELECT COUNT(*) FROM ALL_OBJECTS WHERE OWNER=? AND OBJECT_NAME=? AND SUBOBJECT_NAME IS NULL",
				    {intent.schema, obj});
			};
			auto dm_col_nullable = [&](const string &tbl, const string &col) {
				auto rows = s.Query(
				    "SELECT COALESCE(MAX(NULLABLE),'') FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=?",
				    {intent.schema, tbl, col});
				return rows.size() == 1 && !rows[0].empty() ? Trimmed(rows[0][0]) : string();
			};
			auto check_dm_col_deps = [&](const string &tbl, const string &col, const string &nullable) {
				auto tc_flags = s.Query(
				    "SELECT COALESCE(VIRTUAL_COLUMN,'NO'),COALESCE(HIDDEN_COLUMN,'NO') "
				    "FROM ALL_TAB_COLS WHERE OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=?",
				    {intent.schema, tbl, col});
				if (tc_flags.size() == 1 && (tc_flags[0][0] == "YES" || tc_flags[0][1] == "YES"))
					RejectGuard("0A000", "source_definition", "cannot modify or drop virtual or hidden column");
				if (count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.SYSCOLUMNS sc "
				                "JOIN SYS.SYSOBJECTS tab ON sc.ID=tab.ID "
				                "JOIN SYS.SYSOBJECTS sch ON tab.SCHID=sch.ID "
				                "WHERE sch.TYPE$='SCH' AND sch.NAME=? AND tab.TYPE$='SCHOBJ' AND tab.SUBTYPE$='UTAB' "
				                "AND tab.NAME=? AND sc.NAME=? AND BITAND(COALESCE(sc.INFO2,0),1)=1",
				                {intent.schema, tbl, col}))
					RejectGuard("0A000", "source_definition", "cannot modify or drop identity column");
				if (count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_INDEXES idx "
				                "JOIN ALL_IND_COLUMNS ic ON ic.INDEX_OWNER=idx.OWNER AND ic.INDEX_NAME=idx.INDEX_NAME "
				                "WHERE idx.TABLE_OWNER=? AND idx.TABLE_NAME=? AND ic.COLUMN_NAME=?",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition", "cannot modify or drop column referenced by an index");
				if (count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_CONSTRAINTS con "
				                "JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=con.OWNER AND cc.CONSTRAINT_NAME=con.CONSTRAINT_NAME "
				                "AND cc.TABLE_NAME=con.TABLE_NAME WHERE con.OWNER=? AND con.TABLE_NAME=? "
				                "AND cc.COLUMN_NAME=? AND con.CONSTRAINT_TYPE IN ('P','U','R')",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition",
					            "cannot modify or drop column referenced by a constraint");
				auto chk_rows = s.Query(
				    "SELECT con.CONSTRAINT_NAME,COALESCE(con.GENERATED,''),"
				    "COALESCE(CAST(con.SEARCH_CONDITION AS VARCHAR(4000)),'') FROM ALL_CONSTRAINTS con "
				    "JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=con.OWNER AND cc.CONSTRAINT_NAME=con.CONSTRAINT_NAME "
				    "AND cc.TABLE_NAME=con.TABLE_NAME WHERE con.OWNER=? AND con.TABLE_NAME=? "
				    "AND cc.COLUMN_NAME=? AND con.CONSTRAINT_TYPE='C'",
				    {intent.schema, tbl, col});
				for (const auto &r : chk_rows) {
					if (r[1] != "GENERATED NAME" || nullable != "N" || !IsDamengSystemNotNullCheck(col, r[2]))
						RejectGuard("2BP01", "source_definition",
						            "cannot modify or drop column referenced by a check constraint");
				}
				if (count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_DEPENDENCIES WHERE REFERENCED_OWNER=? "
				                "AND REFERENCED_NAME=? AND REFERENCED_TYPE='TABLE' AND TYPE IN ('VIEW','MATERIALIZED VIEW')",
				                {intent.schema, tbl}))
					RejectGuard("2BP01", "source_definition",
					            "cannot modify or drop column of table referenced by a view");
			};

			if (intent.action == "create_schema" || intent.action == "drop_schema") {
				bool present =
				    count_query("SELECT COUNT(*) FROM SYS.SYSOBJECTS WHERE TYPE$='SCH' AND NAME=?", {intent.name});
				if (intent.conditional && ((intent.action == "create_schema") ? present : !present))
					source_noop = true;
			} else if (intent.action == "create_table" || intent.action == "create_table_as") {
				if (dm_table_exists(intent.name)) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42P07", "source_definition", "target object already exists");
				} else if (dm_object_exists(intent.name)) {
					RejectGuard("42P07", "source_definition", "target object already exists");
				}
			} else if (intent.action == "drop_table") {
				if (!dm_table_exists(intent.name)) {
					if (intent.conditional && !dm_object_exists(intent.name))
						source_noop = true;
					else
						RejectGuard("42P01", "source_definition", "target table does not exist");
				} else if (count_query(
				               "SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_CONSTRAINTS pk "
				               "JOIN ALL_CONSTRAINTS fk ON fk.R_OWNER=pk.OWNER AND fk.R_CONSTRAINT_NAME=pk.CONSTRAINT_NAME "
				               "WHERE pk.OWNER=? AND pk.TABLE_NAME=? AND fk.CONSTRAINT_TYPE='R' "
				               "AND NOT (fk.OWNER=pk.OWNER AND fk.TABLE_NAME=pk.TABLE_NAME)",
				               {intent.schema, intent.name})) {
					RejectGuard("2BP01", "source_definition", "cannot drop table with referencing foreign keys");
				}
			} else if (intent.action == "add_column") {
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				const auto &c = intent.column_specs[0];
				if (!dm_col_nullable(intent.name, c.name).empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42701", "source_definition", "column already exists");
				} else if (c.not_null && (c.default_kind.empty() || c.default_kind == "null")) {
					if (count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM (SELECT 1 FROM " + target + " LIMIT 1)",
					                {}))
						RejectGuard("23502", "source_definition",
						            "cannot add NOT NULL column without default to non-empty table");
				}
			} else if (intent.action == "drop_column") {
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				auto nullable = dm_col_nullable(intent.name, intent.column_name);
				if (nullable.empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42703", "source_definition", "column does not exist");
				} else {
					if (std::stoi(Trimmed(s.Scalar("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_TAB_COLUMNS "
					                               "WHERE OWNER=? AND TABLE_NAME=?",
					                               {intent.schema, intent.name}))) <= 1)
						RejectGuard("42P16", "source_definition", "cannot drop the only column of a table");
					check_dm_col_deps(intent.name, intent.column_name, nullable);
				}
			} else if (intent.action == "alter_column_type") {
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				auto col_rows = s.Query(
				    "SELECT DATA_TYPE,COALESCE(CAST(DATA_LENGTH AS VARCHAR(20)),'0'),"
				    "COALESCE(CAST(CHAR_LENGTH AS VARCHAR(20)),'0'),"
				    "COALESCE(CAST(DATA_PRECISION AS VARCHAR(20)),''),"
				    "COALESCE(CAST(DATA_SCALE AS VARCHAR(20)),''),NULLABLE "
				    "FROM ALL_TAB_COLS WHERE OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=? "
				    "AND COALESCE(HIDDEN_COLUMN,'NO')='NO'",
				    {intent.schema, intent.name, intent.column_name});
				if (col_rows.size() != 1)
					RejectGuard("42703", "source_definition", "column does not exist");
				const auto &r = col_rows[0];
				check_dm_col_deps(intent.name, intent.column_name, r[5]);
				const string dtype = StringUtil::Upper(Trimmed(r[0]));
				const int data_len = std::stoi(Trimmed(r[1]));
				const int char_len = std::stoi(Trimmed(r[2]));
				const string prec_str = Trimmed(r[3]);
				const string scale_str = Trimmed(r[4]);
				const auto &tc = intent.column_specs[0];
				string new_type;

				if (dtype == "SMALLINT") {
					if (tc.type == "integer" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "INT";
					else if (tc.type == "bigint" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "BIGINT";
				} else if (dtype == "INT" || dtype == "INTEGER") {
					if (tc.type == "bigint" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "BIGINT";
				} else if (dtype == "REAL") {
					if (tc.type == "double" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "DOUBLE";
				} else if (dtype == "DECIMAL" || dtype == "DEC" || dtype == "NUMERIC" || dtype == "NUMBER") {
					if (!prec_str.empty() && !scale_str.empty()) {
						int p1 = std::stoi(prec_str);
						int s1 = std::stoi(scale_str);
						if (p1 > 0 && tc.type == "numeric" && tc.length == 0 && s1 == tc.scale && tc.precision > p1 &&
						    tc.precision <= 38)
							new_type =
							    "DECIMAL(" + std::to_string(tc.precision) + "," + std::to_string(tc.scale) + ")";
					}
				} else if (dtype == "VARCHAR" || dtype == "VARCHAR2") {
					int cur_len = char_len > 0 ? char_len : data_len;
					if (tc.type == "varchar" && tc.precision == 0 && tc.scale == 0 && tc.length > cur_len &&
					    tc.length <= 8188)
						new_type = "VARCHAR(" + std::to_string(tc.length) + ")";
				} else if ((dtype == "TIME" || StringUtil::StartsWith(dtype, "TIME(")) &&
				           !StringUtil::Contains(dtype, "STAMP") && !StringUtil::Contains(dtype, "ZONE")) {
					int p1 = ExtractDamengParenPrecision(dtype, scale_str);
					if (tc.type == "time" && tc.length == 0 && tc.scale == 0 && tc.precision > p1 && tc.precision <= 6)
						new_type = "TIME(" + std::to_string(tc.precision) + ")";
				} else if (!StringUtil::Contains(dtype, "LOCAL TIME ZONE") &&
				           StringUtil::Contains(dtype, "WITH TIME ZONE") &&
				           (StringUtil::StartsWith(dtype, "TIMESTAMP") || StringUtil::StartsWith(dtype, "DATETIME"))) {
					int p1 = ExtractDamengParenPrecision(dtype, scale_str);
					if (tc.type == "timestamptz" && tc.length == 0 && tc.scale == 0 && tc.precision > p1 &&
					    tc.precision <= 6)
						new_type = "TIMESTAMP(" + std::to_string(tc.precision) + ") WITH TIME ZONE";
				} else if (!StringUtil::Contains(dtype, "ZONE") &&
				           (dtype == "TIMESTAMP" || dtype == "DATETIME" ||
				            StringUtil::StartsWith(dtype, "TIMESTAMP(") ||
				            StringUtil::StartsWith(dtype, "DATETIME("))) {
					int p1 = ExtractDamengParenPrecision(dtype, scale_str);
					if (tc.type == "timestamp" && tc.length == 0 && tc.scale == 0 && tc.precision > p1 &&
					    tc.precision <= 6)
						new_type = "TIMESTAMP(" + std::to_string(tc.precision) + ")";
				}
				if (new_type.empty())
					RejectGuard("0A000", "source_definition", "only lossless type widening is supported");
				native_sql = "ALTER TABLE " + target + " MODIFY " + Quote(intent.column_name) + " " + new_type;
			} else if (intent.action == "set_not_null" || intent.action == "drop_not_null") {
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				auto nullable = dm_col_nullable(intent.name, intent.column_name);
				if (nullable.empty())
					RejectGuard("42703", "source_definition", "column does not exist");
				if (intent.action == "set_not_null") {
					if (nullable == "N")
						no_op_commit = true;
				} else {
					if (count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_CONSTRAINTS con "
					                "JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=con.OWNER AND cc.CONSTRAINT_NAME=con.CONSTRAINT_NAME "
					                "WHERE con.OWNER=? AND con.TABLE_NAME=? AND cc.COLUMN_NAME=? AND con.CONSTRAINT_TYPE='P'",
					                {intent.schema, intent.name, intent.column_name}))
						RejectGuard("42831", "source_definition", "cannot drop NOT NULL on primary key column");
					if (nullable == "Y")
						no_op_commit = true;
				}
			} else if (intent.action == "create_index" || intent.action == "create_unique_index") {
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				auto idx_rows = s.Query(
				    "SELECT TABLE_OWNER,TABLE_NAME FROM ALL_INDEXES WHERE OWNER=? AND INDEX_NAME=?",
				    {intent.schema, intent.index});
				if (!idx_rows.empty()) {
					if (intent.conditional && idx_rows.size() == 1 && idx_rows[0][0] == intent.schema &&
					    idx_rows[0][1] == intent.name)
						source_noop = true;
					else
						RejectGuard("42P07", "source_definition", "index name already exists in schema");
				}
			} else if (intent.action == "drop_index") {
				auto idx_rows = s.Query("SELECT TABLE_NAME FROM ALL_INDEXES WHERE OWNER=? AND INDEX_NAME=?",
				                        {intent.schema, intent.name});
				if (idx_rows.empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42704", "source_definition", "index does not exist");
				} else if (!intent.owner.empty() && idx_rows[0][0] != intent.owner) {
					RejectGuard("42704", "source_definition", "index owner mismatch");
				} else if (count_query(
				               "SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_CONSTRAINTS WHERE OWNER=? "
				               "AND INDEX_NAME=? AND CONSTRAINT_TYPE IN ('P','U')",
				               {intent.schema, intent.name})) {
					RejectGuard("2BP01", "source_definition", "cannot drop constraint backing index directly");
				}
			} else if (intent.action == "drop_constraint") {
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				if (!count_query("SELECT COUNT(*) FROM ALL_CONSTRAINTS WHERE OWNER=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?",
				                 {intent.schema, intent.name, intent.constraint_name})) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42704", "source_definition", "constraint does not exist");
				}
			} else if (intent.action == "truncate") {
				if (intent.identity_mode == "restart")
					RejectGuard("0A000", "source_definition", "Dameng TRUNCATE RESTART IDENTITY is unsupported");
				if (!dm_table_exists(intent.name))
					RejectGuard("42P01", "source_definition", "target table does not exist");
				if (count_query(
				        "SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM ALL_CONSTRAINTS pk "
				        "JOIN ALL_CONSTRAINTS fk ON fk.R_OWNER=pk.OWNER AND fk.R_CONSTRAINT_NAME=pk.CONSTRAINT_NAME "
				        "WHERE pk.OWNER=? AND pk.TABLE_NAME=? AND fk.CONSTRAINT_TYPE='R'",
				        {intent.schema, intent.name}))
					RejectGuard("2BP01", "source_definition", "cannot truncate table with referencing foreign keys");
			}
		} else {
			// SAP HANA
			auto hana_table_row = [&](const string &tbl) {
				return s.Query(
				    "SELECT TABLE_TYPE,IS_TEMPORARY,IS_USER_DEFINED_TYPE FROM SYS.TABLES WHERE SCHEMA_NAME=? AND TABLE_NAME=?",
				    {intent.schema, tbl});
			};
			auto hana_view_exists = [&](const string &obj) {
				return count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.VIEWS WHERE SCHEMA_NAME=? AND VIEW_NAME=?",
				                   {intent.schema, obj});
			};
			auto check_hana_base_table = [&](const string &tbl) {
				auto rows = hana_table_row(tbl);
				if (rows.empty()) {
					if (hana_view_exists(tbl))
						RejectGuard("42809", "source_definition", "target object is a view, not a table");
					RejectGuard("42P01", "source_definition", "target table does not exist");
				}
				const auto &r = rows[0];
				if ((r[0] != "COLUMN" && r[0] != "ROW") || r[1] != "FALSE" || r[2] != "FALSE")
					RejectGuard("0A000", "source_definition", "unsupported HANA table kind");
				return r[0];
			};
			auto hana_col_row = [&](const string &tbl, const string &col) {
				return s.Query("SELECT DATA_TYPE_NAME,TO_VARCHAR(LENGTH),COALESCE(TO_VARCHAR(SCALE),'-1'),"
				               "IS_NULLABLE,CASE WHEN DEFAULT_VALUE IS NOT NULL THEN '1' ELSE '0' END,"
				               "COALESCE(GENERATION_TYPE,''),COALESCE(GENERATED_ALWAYS_AS,''),COALESCE(DEFAULT_VALUE,'') "
				               "FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME=? AND COLUMN_NAME=?",
				               {intent.schema, tbl, col});
			};
			auto check_hana_col_deps = [&](const string &tbl, const string &col, const vector<string> &cm) {
				auto gen_upper = StringUtil::Upper(Trimmed(cm[5]));
				if ((!gen_upper.empty() && gen_upper != "NONE") || !Trimmed(cm[6]).empty())
					RejectGuard("0A000", "source_definition", "cannot modify or drop identity or generated column");
				if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=? "
				                "AND TABLE_NAME=? AND COLUMN_NAME=?",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition",
					            "cannot modify or drop column referenced by a constraint");
				auto tbl_checks = s.Query(
				    "SELECT COALESCE(CHECK_CONDITION,'') FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=? AND TABLE_NAME=? "
				    "AND IS_PRIMARY_KEY='FALSE' AND IS_UNIQUE_KEY='FALSE' AND COLUMN_NAME IS NULL",
				    {intent.schema, tbl});
				for (const auto &chk : tbl_checks) {
					if (HanaCheckExprMentionsColumn(chk[0], col))
						RejectGuard("2BP01", "source_definition",
						            "cannot modify or drop column referenced by a check constraint");
				}
				if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE (SCHEMA_NAME=? "
				                "AND TABLE_NAME=? AND COLUMN_NAME=?) OR (REFERENCED_SCHEMA_NAME=? "
				                "AND REFERENCED_TABLE_NAME=? AND REFERENCED_COLUMN_NAME=?)",
				                {intent.schema, tbl, col, intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition",
					            "cannot modify or drop column referenced by a foreign key constraint");
				if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.INDEX_COLUMNS WHERE SCHEMA_NAME=? "
				                "AND TABLE_NAME=? AND COLUMN_NAME=?",
				                {intent.schema, tbl, col}))
					RejectGuard("2BP01", "source_definition", "cannot modify or drop column referenced by an index");
				if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.OBJECT_DEPENDENCIES WHERE BASE_SCHEMA_NAME=? "
				                "AND BASE_OBJECT_NAME=? AND BASE_OBJECT_TYPE='TABLE' "
				                "AND DEPENDENT_OBJECT_TYPE IN ('VIEW','CALCULATION_VIEW')",
				                {intent.schema, tbl}))
					RejectGuard("2BP01", "source_definition",
					            "cannot modify or drop column of table referenced by a view");
			};

			if (intent.action == "create_schema" || intent.action == "drop_schema") {
				bool present =
				    count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.SCHEMAS WHERE SCHEMA_NAME=?",
				                {intent.name});
				if (intent.conditional && ((intent.action == "create_schema") ? present : !present))
					source_noop = true;
			} else if (intent.action == "create_table" || intent.action == "create_table_as") {
				if (!hana_table_row(intent.name).empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42P07", "source_definition", "target table already exists");
				} else if (hana_view_exists(intent.name)) {
					RejectGuard("42P07", "source_definition", "target object already exists");
				}
			} else if (intent.action == "drop_table") {
				if (hana_table_row(intent.name).empty()) {
					if (intent.conditional && !hana_view_exists(intent.name))
						source_noop = true;
					else
						RejectGuard("42P01", "source_definition", "target table does not exist");
				} else {
					check_hana_base_table(intent.name);
					if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.REFERENTIAL_CONSTRAINTS WHERE "
					                "REFERENCED_SCHEMA_NAME=? AND REFERENCED_TABLE_NAME=? "
					                "AND NOT (SCHEMA_NAME=? AND TABLE_NAME=?)",
					                {intent.schema, intent.name, intent.schema, intent.name}))
						RejectGuard("2BP01", "source_definition", "cannot drop table with referencing foreign keys");
				}
			} else if (intent.action == "add_column") {
				check_hana_base_table(intent.name);
				const auto &c = intent.column_specs[0];
				if (!hana_col_row(intent.name, c.name).empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42701", "source_definition", "column already exists");
				} else if (c.not_null && (c.default_kind.empty() || c.default_kind == "null")) {
					if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM (SELECT 1 FROM " + target + " LIMIT 1)", {}))
						RejectGuard("23502", "source_definition",
						            "cannot add NOT NULL column without default to non-empty table");
				}
			} else if (intent.action == "drop_column") {
				check_hana_base_table(intent.name);
				auto cr = hana_col_row(intent.name, intent.column_name);
				if (cr.empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42703", "source_definition", "column does not exist");
				} else {
					if (std::stoi(Trimmed(s.Scalar("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.TABLE_COLUMNS "
					                               "WHERE SCHEMA_NAME=? AND TABLE_NAME=?",
					                               {intent.schema, intent.name}))) <= 1)
						RejectGuard("42P16", "source_definition", "cannot drop the only column of a table");
					check_hana_col_deps(intent.name, intent.column_name, cr[0]);
				}
			} else if (intent.action == "alter_column_type") {
				const string ttype = check_hana_base_table(intent.name);
				if (ttype != "COLUMN")
					RejectGuard("0A000", "source_definition", "HANA ALTER COLUMN TYPE requires COLUMN table");
				auto cr = hana_col_row(intent.name, intent.column_name);
				if (cr.empty())
					RejectGuard("42703", "source_definition", "column does not exist");
				const auto &c = cr[0];
				check_hana_col_deps(intent.name, intent.column_name, c);
				const string dtype = StringUtil::Upper(Trimmed(c[0]));
				const int cur_len = std::stoi(Trimmed(c[1]));
				const string scale_str = Trimmed(c[2]);
				const auto &tc = intent.column_specs[0];
				string new_type;

				if (dtype == "SMALLINT") {
					if (tc.type == "integer" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "INTEGER";
					else if (tc.type == "bigint" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "BIGINT";
				} else if (dtype == "INTEGER") {
					if (tc.type == "bigint" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "BIGINT";
				} else if (dtype == "REAL") {
					if (tc.type == "double" && tc.length == 0 && tc.precision == 0 && tc.scale == 0)
						new_type = "DOUBLE";
				} else if (dtype == "DECIMAL" && scale_str != "-1") {
					int s1 = std::stoi(scale_str);
					if (tc.type == "numeric" && tc.length == 0 && s1 == tc.scale && tc.precision > cur_len &&
					    tc.precision <= 38)
						new_type = "DECIMAL(" + std::to_string(tc.precision) + "," + std::to_string(tc.scale) + ")";
				} else if (dtype == "NVARCHAR") {
					if (tc.type == "varchar" && tc.precision == 0 && tc.scale == 0 && tc.length > cur_len &&
					    tc.length <= 5000)
						new_type = "NVARCHAR(" + std::to_string(tc.length) + ")";
				}
				if (new_type.empty())
					RejectGuard("0A000", "source_definition", "only lossless type widening is supported");
				const string null_kw = c[3] == "FALSE" ? "NOT NULL" : "NULL";
				string alter_col_clause = Quote(intent.column_name) + " " + new_type + " " + null_kw;
				if (c[4] == "1") {
					if (dtype == "NVARCHAR" || dtype == "VARCHAR" || dtype == "CHAR" || dtype == "NCHAR") {
						alter_col_clause += " DEFAULT " + Literal(c[7]);
					} else {
						string def_trim = Trimmed(c[7]);
						bool ok = (dtype == "SMALLINT" || dtype == "INTEGER") ? SimpleIntLiteral(def_trim)
						                                                      : IsSignedDecimalLiteral(def_trim);
						if (!ok)
							RejectGuard("0A000", "source_definition",
							            "cannot preserve non-literal default during HANA alter column type");
						alter_col_clause += " DEFAULT " + def_trim;
					}
				}
				native_sql = "ALTER TABLE " + target + " ALTER (" + alter_col_clause + ")";
			} else if (intent.action == "set_default" || intent.action == "drop_default" ||
			           intent.action == "set_not_null" || intent.action == "drop_not_null") {
				check_hana_base_table(intent.name);
				auto cr = hana_col_row(intent.name, intent.column_name);
				if (cr.empty())
					RejectGuard("42703", "source_definition", "HANA DDL column unavailable");
				const auto &c = cr[0];
				auto gen_upper = StringUtil::Upper(Trimmed(c[5]));
				if ((!gen_upper.empty() && gen_upper != "NONE") || !Trimmed(c[6]).empty())
					RejectGuard("0A000", "source_definition", "cannot alter identity or generated column");
				auto col_type = FormatHanaColumnType(c[0], c[1], c[2]);
				if (intent.action == "set_not_null") {
					if (c[3] == "FALSE")
						no_op_commit = true;
					else
						native_sql =
						    "ALTER TABLE " + target + " ALTER (" + Quote(intent.column_name) + " " + col_type + " NOT NULL)";
				} else if (intent.action == "drop_not_null") {
					if (count_query("SELECT TO_VARCHAR(COUNT(*)) FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=? "
					                "AND TABLE_NAME=? AND COLUMN_NAME=? AND IS_PRIMARY_KEY='TRUE'",
					                {intent.schema, intent.name, intent.column_name}))
						RejectGuard("42831", "source_definition", "cannot drop NOT NULL on primary key column");
					if (c[3] == "TRUE")
						no_op_commit = true;
					else
						native_sql =
						    "ALTER TABLE " + target + " ALTER (" + Quote(intent.column_name) + " " + col_type + " NULL)";
				} else if (intent.action == "drop_default") {
					native_sql = "ALTER TABLE " + target + " ALTER (" + Quote(intent.column_name) + " " + col_type +
					             " DEFAULT NULL)";
				} else {
					string lit = DefaultLiteralSQL(d.dialect, intent.default_kind, intent.default_value);
					native_sql = "ALTER TABLE " + target + " ALTER (" + Quote(intent.column_name) + " " + col_type +
					             " DEFAULT " + lit + ")";
				}
			} else if (intent.action == "add_constraint") {
				const string ttype = check_hana_base_table(intent.name);
				if (intent.constraint_kind == "foreign_key") {
					if (ttype != "COLUMN")
						RejectGuard("0A000", "source_definition", "HANA foreign keys require COLUMN tables");
					if (check_hana_base_table(intent.reference_table) != "COLUMN")
						RejectGuard("0A000", "source_definition",
						            "HANA foreign key referenced table must be COLUMN table");
				}
			} else if (intent.action == "drop_constraint") {
				check_hana_base_table(intent.name);
				if (!count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.CONSTRAINTS "
				                 "WHERE SCHEMA_NAME=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?",
				                 {intent.schema, intent.name, intent.constraint_name}) &&
				    !count_query("SELECT CAST(COUNT(*) AS VARCHAR(20)) FROM SYS.REFERENTIAL_CONSTRAINTS "
				                 "WHERE SCHEMA_NAME=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?",
				                 {intent.schema, intent.name, intent.constraint_name})) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42704", "source_definition", "constraint does not exist");
				}
			} else if (intent.action == "create_index" || intent.action == "create_unique_index") {
				check_hana_base_table(intent.name);
				auto idx_rows = s.Query("SELECT TABLE_NAME FROM SYS.INDEXES WHERE SCHEMA_NAME=? AND INDEX_NAME=?",
				                        {intent.schema, intent.index});
				if (!idx_rows.empty()) {
					if (intent.conditional && idx_rows.size() == 1 && idx_rows[0][0] == intent.name)
						source_noop = true;
					else
						RejectGuard("42P07", "source_definition", "index name already exists in schema");
				} else {
					for (const auto &k : intent.keys) {
						auto cr = hana_col_row(intent.name, k);
						if (cr.empty())
							RejectGuard("42703", "source_definition", "index column does not exist");
						auto dt = StringUtil::Upper(Trimmed(cr[0][0]));
						if (dt == "CLOB" || dt == "NCLOB" || dt == "BLOB" || dt == "TEXT" || dt == "BINTEXT")
							RejectGuard("0A000", "source_definition", "cannot index LOB column on HANA");
					}
				}
			} else if (intent.action == "drop_index") {
				auto idx_rows = s.Query(
				    "SELECT TABLE_NAME,COALESCE(CONSTRAINT,'') FROM SYS.INDEXES WHERE SCHEMA_NAME=? AND INDEX_NAME=?",
				    {intent.schema, intent.name});
				if (idx_rows.empty()) {
					if (intent.conditional)
						source_noop = true;
					else
						RejectGuard("42704", "source_definition", "index does not exist");
				} else if (!intent.owner.empty() && idx_rows[0][0] != intent.owner) {
					RejectGuard("42704", "source_definition", "index owner mismatch");
				} else if (!Trimmed(idx_rows[0][1]).empty()) {
					RejectGuard("2BP01", "source_definition", "cannot drop constraint backing index directly");
				}
			} else if (intent.action == "truncate") {
				if (intent.identity_mode == "restart")
					RejectGuard("0A000", "source_definition", "HANA TRUNCATE RESTART IDENTITY is unsupported");
				check_hana_base_table(intent.name);
			}
		}

		if (source_noop) {
			if (transactional)
				s.Exec("COMMIT");
			status = "COMMITTED";
			warning_count = 0;
			stage = "source_noop";
		} else if (no_op_commit) {
			if (transactional)
				s.Exec("COMMIT");
			status = "COMMITTED";
			warning_count = 0;
			stage.clear();
		} else {
			if (is_db2_truncate) {
				// Db2 requires TRUNCATE TABLE ... IMMEDIATE to be the first statement in its UOW.
				s.Exec("COMMIT");
			} else if (transactional && !lock_tables.empty()) {
				std::sort(lock_tables.begin(), lock_tables.end());
				stage = "source_lock";
				for (const auto &tbl : lock_tables) {
					if (ctx.IsInterrupted())
						RejectGuard("57014", "source_lock", "operation cancelled");
					s.Exec("LOCK TABLE " + Quote(intent.schema) + "." + Quote(tbl) + " IN EXCLUSIVE MODE");
				}
			}

			stage = "source_execute";
			if (ctx.IsInterrupted())
				RejectGuard("57014", "source_execute", "operation cancelled");
			s.last_warning_count = 0;
			sent = true;
			s.Exec(native_sql);
			if (d.dialect == ODBCDialect::DB2 && intent.action == "create_table_as") {
				s.Exec("INSERT INTO " + target + " " + intent.query_sql);
			}
			if (transactional) {
				stage = "source_commit";
				commit_sent = true;
				status = "UNKNOWN";
				s.Exec("COMMIT");
			}
			status = "COMMITTED";
			warning_count = s.last_warning_count;
			stage = warning_count > 0 ? "source_warning" : "";
		}
	} catch (const DDLGuardReject &rej) {
		error = rej.sqlstate;
		stage = rej.stage;
		if (transactional && session)
			session->TryExec("ROLLBACK");
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
	out.SetValue(0, 0, Value(intent.operation));
	out.SetValue(1, 0, Value(status));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value::BIGINT(warning_count));
	if (d.diagnostics)
		out.SetValue(5, 0, Value(source.JSON()));
	out.SetChildCardinality(1);
}

} // namespace

void RegisterODBCGuardedDDL(ExtensionLoader &loader) {
	for (auto *name : {"dameng", "db2", "hana"}) {
		loader.RegisterFunction(TableFunction(Identifier(string(name) + "_ddl_capabilities"),
		                                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanMetadata, BindMetadata,
		                                      InitOne));
		loader.RegisterFunction(TableFunction(
		    Identifier(string(name) + "_ddl_describe"),
		    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanMetadata,
		    BindMetadata, InitOne));
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
