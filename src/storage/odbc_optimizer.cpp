#include "storage/odbc_optimizer.hpp"
#include "storage/odbc_catalog.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "odbc_scanner.hpp"

namespace duckdb {

struct ODBCOperators {
	reference_map_t<ODBCCatalog, vector<reference<LogicalGet>>> scans;
};

void GatherODBCScans(LogicalOperator &op, ODBCOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (ODBCCatalog::IsODBCScan(table_scan.name)) {
			// odbc table scan - add to the catalog
			auto &bind_data = get.bind_data->Cast<ODBCBindData>();
			auto &catalog = bind_data.table.ParentCatalog().Cast<ODBCCatalog>();
			result.scans[catalog].push_back(get);
		}
	}
	// recurse into children
	for (auto &child : op.children) {
		GatherODBCScans(*child, result);
	}
}

void ODBCOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	ODBCOperators operators;
	GatherODBCScans(*plan, operators);
	for (auto &entry : operators.scans) {
		ODBCResultStreaming result_streaming = ODBCResultStreaming::FORCE_MATERIALIZATION;
		if (entry.second.size() == 1) {
			// if we have exactly one scan for a given catalog we can stream it
			result_streaming = ODBCResultStreaming::ALLOW_STREAMING;
		}
		for (auto &logical_get : entry.second) {
			auto &get = logical_get.get();
			if (ODBCCatalog::IsODBCScan(get.function.name)) {
				// odbc table scan - add to the catalog
				auto &bind_data = get.bind_data->Cast<ODBCBindData>();
				if (bind_data.streaming == ODBCResultStreaming::UNINITIALIZED ||
				    result_streaming == ODBCResultStreaming::FORCE_MATERIALIZATION) {
					bind_data.streaming = result_streaming;
				}
			}
		}
	}
}

} // namespace duckdb
