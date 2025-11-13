#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_table_entry.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

ODBCSchemaEntry::ODBCSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this), indexes(*this) {
}

ODBCTransaction &GetODBCTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<ODBCTransaction>();
}

void ODBCSchemaEntry::TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name) {
	DropInfo info;
	info.type = catalog_type;
	info.name = name;
	info.cascade = false;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	DropEntry(context, info);
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TABLE_ENTRY, table_name);
	}
	return tables.CreateTable(transaction.GetContext(), info);
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("ODBC databases do not support creating functions");
}

void ODBCUnqualifyColumnRef(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, ODBCUnqualifyColumnRef);
}

string GetODBCCreateIndex(CreateIndexInfo &info, TableCatalogEntry &tbl) {
	string sql;
	sql = "CREATE";
	if (info.constraint_type == IndexConstraintType::UNIQUE) {
		sql += " UNIQUE";
	}
	sql += " INDEX ";
	sql += ODBCUtils::WriteIdentifier(info.index_name);
	sql += " ON ";
	sql += ODBCUtils::WriteIdentifier(tbl.name);
	sql += "(";
	for (idx_t i = 0; i < info.parsed_expressions.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		ODBCUnqualifyColumnRef(*info.parsed_expressions[i]);
		if (info.parsed_expressions[i]->type == ExpressionType::COLUMN_REF) {
			// index on column
			sql += info.parsed_expressions[i]->ToString();
		} else {
			// index on expression
			// expressions need to be wrapped in brackets
			sql += "(" + info.parsed_expressions[i]->ToString() + ")";
		}
	}
	sql += ")";
	return sql;
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	auto &odbc_transaction = ODBCTransaction::Get(transaction.GetContext(), table.catalog);
	odbc_transaction.Query(GetODBCCreateIndex(info, table));
	return nullptr;
}

string GetODBCCreateView(CreateViewInfo &info) {
	string sql;
	sql = "CREATE VIEW ";
	sql += ODBCUtils::WriteIdentifier(info.view_name);
	sql += " ";
	if (!info.aliases.empty()) {
		sql += "(";
		for (idx_t i = 0; i < info.aliases.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto &alias = info.aliases[i];
			sql += ODBCUtils::WriteIdentifier(alias);
		}
		sql += ") ";
	}
	sql += "AS ";
	sql += info.query->ToString();
	return sql;
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty()) {
		throw BinderException("Cannot create view in ODBC that originated from an "
		                      "empty SQL statement");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (current_entry) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return current_entry;
			}
			// CREATE OR REPLACE - drop any existing entries first (if any)
			TryDropEntry(transaction.GetContext(), CatalogType::VIEW_ENTRY, info.view_name);
		}
	}
	auto &odbc_transaction = GetODBCTransaction(transaction);
	odbc_transaction.Query(GetODBCCreateView(info));
	return tables.RefreshTable(transaction.GetContext(), info.view_name);
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("ODBC databases do not support creating types");
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("ODBC databases do not support creating sequences");
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                 CreateTableFunctionInfo &info) {
	throw BinderException("ODBC databases do not support creating table functions");
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                CreateCopyFunctionInfo &info) {
	throw BinderException("ODBC databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                  CreatePragmaFunctionInfo &info) {
	throw BinderException("ODBC databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                             CreateCollationInfo &info) {
	throw BinderException("ODBC databases do not support creating collations");
}

void ODBCSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for now");
	}
	auto &alter = info.Cast<AlterTableInfo>();
	tables.AlterTable(transaction.GetContext(), alter);
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void ODBCSchemaEntry::Scan(ClientContext &context, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	GetCatalogSet(type).Scan(context, callback);
}
void ODBCSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void ODBCSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	GetCatalogSet(info.type).DropEntry(context, info);
}

optional_ptr<CatalogEntry> ODBCSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	auto lookup_type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(lookup_type)) {
		return nullptr;
	}
	return GetCatalogSet(lookup_type).GetEntry(transaction.GetContext(), lookup_info.GetEntryName());
}

ODBCCatalogSet &ODBCSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
