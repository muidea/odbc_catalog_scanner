//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "odbc_types.hpp"

namespace duckdb {

struct ODBCFilterPushdownResult {
	string where_clause;
	vector<idx_t> unpushed_filter_columns;
};

class ODBCFilterPushdown {
public:
	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names, ODBCDialect dialect = ODBCDialect::DB2);
	static ODBCFilterPushdownResult TransformFiltersWithFallback(const vector<column_t> &column_ids,
	                                                             optional_ptr<TableFilterSet> filters,
	                                                             const vector<string> &names,
	                                                             const vector<string> &remote_type_names,
	                                                             ODBCDialect dialect = ODBCDialect::DB2);
	static string TransformFiltersWithFallback(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                                           const vector<string> &names, const vector<string> &remote_type_names,
	                                           ODBCDialect dialect, bool &has_unpushed_filters);

private:
	static bool TryTransformFilter(const string &column_name, const string &remote_type_name, const TableFilter &filter,
	                               ODBCDialect dialect, string &out);
};

} // namespace duckdb
