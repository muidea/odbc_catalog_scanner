//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_vendor_metadata.hpp
//
// Fixed, read-only vendor catalog contracts. Identifiers are always values.
//===----------------------------------------------------------------------===//
#pragma once

#include "odbc_dialect.hpp"
#include "odbc_relational_reader.hpp"
#include "odbc_utils.hpp"

namespace duckdb {

inline string ODBCVendorMetadataSQL(ODBCDialect dialect, const string &category, const ODBCRelationalObject &object) {
	const auto schema = ODBCUtils::WriteLiteral(object.schema);
	const auto table = ODBCUtils::WriteLiteral(object.name);
	if (dialect == ODBCDialect::DB2) {
		const auto scope = " WHERE c.TABSCHEMA=" + schema + " AND c.TABNAME=" + table;
		if (category == "columnDetails") {
			return "SELECT RTRIM(c.TABSCHEMA),c.TABNAME,c.COLNAME,c.IDENTITY,c.GENERATED,COALESCE(c.TEXT,''),"
			       "COALESCE(c.DEFAULT,'') FROM SYSCAT.COLUMNS c" +
			       scope + " ORDER BY c.COLNO";
		}
		if (category == "constraintDetails") {
			return "SELECT RTRIM(c.TABSCHEMA),c.TABNAME,c.CONSTNAME,c.TYPE,COALESCE(CAST(k.COLSEQ AS VARCHAR(20)),''),"
			       "COALESCE(k.COLNAME,''),COALESCE(ch.TEXT,''),c.ENFORCED,c.CHECKEXISTINGDATA,'','','' "
			       "FROM SYSCAT.TABCONST c LEFT JOIN SYSCAT.KEYCOLUSE k ON k.TABSCHEMA=c.TABSCHEMA "
			       "AND k.TABNAME=c.TABNAME AND k.CONSTNAME=c.CONSTNAME LEFT JOIN SYSCAT.CHECKS ch "
			       "ON ch.TABSCHEMA=c.TABSCHEMA AND ch.TABNAME=c.TABNAME AND ch.CONSTNAME=c.CONSTNAME" +
			       scope + " AND c.TYPE IN ('P','U','F','K') ORDER BY c.CONSTNAME,k.COLSEQ";
		}
		if (category == "indexDetails") {
			return "SELECT RTRIM(i.TABSCHEMA),i.TABNAME,i.INDNAME,CAST(k.COLSEQ AS VARCHAR(20)),k.COLNAME,"
			       "COALESCE(k.TEXT,''),CASE WHEN k.COLORDER='I' THEN 'true' ELSE 'false' END,k.COLORDER,"
			       "CASE WHEN k.COLORDER='A' THEN 'last' WHEN k.COLORDER='D' THEN 'first' ELSE 'unknown' END,"
			       "'','',i.INDEXTYPE,CASE WHEN i.UNIQUERULE IN ('P','U') THEN 'true' ELSE 'false' END,"
			       "CASE WHEN i.UNIQUERULE='P' THEN 'true' ELSE 'false' END,'' "
			       "FROM SYSCAT.INDEXES i JOIN SYSCAT.INDEXCOLUSE k ON k.INDSCHEMA=i.INDSCHEMA "
			       "AND k.INDNAME=i.INDNAME WHERE i.TABSCHEMA=" +
			       schema + " AND i.TABNAME=" + table +
			       " ORDER BY i.INDNAME,CASE WHEN k.COLORDER='I' THEN 1 ELSE 0 END,k.COLSEQ";
		}
	}
	if (dialect == ODBCDialect::DAMENG) {
		if (category == "columnProperties") {
			return "SELECT OWNER,TABLE_NAME,COLUMN_NAME,CAST(COLUMN_ID AS VARCHAR(20)),CAST(COLUMN_ID AS VARCHAR(20)),"
			       "CASE WHEN NULLABLE='Y' THEN 'true' WHEN NULLABLE='N' THEN 'false' ELSE '' "
			       "END,DATA_DEFAULT,DATA_TYPE "
			       "FROM ALL_TAB_COLUMNS WHERE OWNER=" +
			       schema + " AND TABLE_NAME=" + table + " ORDER BY COLUMN_ID";
		}
		if (category == "constraintDetails") {
			return "SELECT c.OWNER,c.TABLE_NAME,c.CONSTRAINT_NAME,c.CONSTRAINT_TYPE,"
			       "COALESCE(CAST(k.POSITION AS VARCHAR(20)),''),COALESCE(k.COLUMN_NAME,''),"
			       "c.SEARCH_CONDITION,c.STATUS,c.VALIDATED,COALESCE(c.INDEX_NAME,''),c.DEFERRABLE,c.DEFERRED "
			       "FROM ALL_CONSTRAINTS c LEFT JOIN ALL_CONS_COLUMNS k ON k.OWNER=c.OWNER "
			       "AND k.CONSTRAINT_NAME=c.CONSTRAINT_NAME WHERE c.OWNER=" +
			       schema + " AND c.TABLE_NAME=" + table +
			       " AND c.CONSTRAINT_TYPE IN ('P','U','R','C') ORDER BY c.CONSTRAINT_NAME,k.POSITION";
		}
		if (category == "indexDetails") {
			return "SELECT i.TABLE_OWNER,i.TABLE_NAME,i.INDEX_NAME,CAST(ABS(k.COLUMN_POSITION) AS VARCHAR(20)),"
			       "k.COLUMN_NAME,e.COLUMN_EXPRESSION,'false',CASE WHEN k.COLUMN_POSITION<0 THEN 'unknown' ELSE "
			       "k.DESCEND END,'unknown',"
			       "CASE WHEN i.PARTITIONED='YES' THEN '' ELSE i.STATUS END,"
			       "CASE WHEN i.PARTITIONED='YES' THEN '' WHEN i.FUNCIDX_STATUS='DISABLED' THEN 'false' "
			       "WHEN i.STATUS='VALID' THEN 'true' ELSE '' END,i.INDEX_TYPE,"
			       "CASE WHEN i.UNIQUENESS='UNIQUE' THEN 'true' ELSE 'false' END,'','' "
			       "FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS k ON k.INDEX_OWNER=i.OWNER AND k.INDEX_NAME=i.INDEX_NAME "
			       "LEFT JOIN ALL_IND_EXPRESSIONS e ON e.INDEX_OWNER=i.OWNER AND e.INDEX_NAME=i.INDEX_NAME "
			       "AND e.COLUMN_POSITION=ABS(k.COLUMN_POSITION) WHERE i.TABLE_OWNER=" +
			       schema + " AND i.TABLE_NAME=" + table + " ORDER BY i.INDEX_NAME,ABS(k.COLUMN_POSITION)";
		}
	}
	if (dialect == ODBCDialect::HANA) {
		if (category == "columnDetails") {
			return "SELECT SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,GENERATION_TYPE,'',GENERATED_ALWAYS_AS,DEFAULT_VALUE "
			       "FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=" +
			       schema + " AND TABLE_NAME=" + table + " ORDER BY POSITION";
		}
		if (category == "constraintDetails") {
			return "SELECT SCHEMA_NAME,TABLE_NAME,CONSTRAINT_NAME,CASE WHEN IS_PRIMARY_KEY='TRUE' THEN 'P' "
			       "WHEN IS_UNIQUE_KEY='TRUE' THEN 'U' ELSE 'C' END,POSITION,"
			       "COLUMN_NAME,CHECK_CONDITION,'','','','','' FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME=" +
			       schema + " AND TABLE_NAME=" + table +
			       " UNION ALL SELECT SCHEMA_NAME,TABLE_NAME,CONSTRAINT_NAME,'F',"
			       "POSITION,COLUMN_NAME,'',IS_ENFORCED,IS_VALIDATED,'','','' "
			       "FROM SYS.REFERENTIAL_CONSTRAINTS WHERE SCHEMA_NAME=" +
			       schema + " AND TABLE_NAME=" + table + " ORDER BY 3,5";
		}
		if (category == "indexDetails") {
			return "SELECT i.SCHEMA_NAME,i.TABLE_NAME,i.INDEX_NAME,CAST(k.POSITION AS VARCHAR(20)),"
			       "k.COLUMN_NAME,'','false',k.ASCENDING_ORDER,'unknown','','',i.INDEX_TYPE,"
			       "CASE WHEN i.CONSTRAINT IN ('PRIMARY KEY','UNIQUE') THEN 'true' ELSE 'false' END,"
			       "CASE WHEN i.CONSTRAINT='PRIMARY KEY' THEN 'true' ELSE 'false' END,'' "
			       "FROM SYS.INDEXES i JOIN SYS.INDEX_COLUMNS k ON k.SCHEMA_NAME=i.SCHEMA_NAME "
			       "AND k.TABLE_NAME=i.TABLE_NAME AND k.INDEX_NAME=i.INDEX_NAME WHERE i.SCHEMA_NAME=" +
			       schema + " AND i.TABLE_NAME=" + table + " ORDER BY i.INDEX_NAME,k.POSITION";
		}
	}
	throw NotImplementedException("ODBC vendor metadata category is unavailable");
}

} // namespace duckdb
