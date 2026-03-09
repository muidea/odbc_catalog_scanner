
duckdb.exe -unsigned


load odbc_scanner;


ATTACH 'Driver={SQL Server};Server=192.168.235.182,1433;Database=master;UID=sa;PWD=Supos1304;' AS "fqe_MSSL2000" (TYPE odbc_scanner);

ATTACH 'Driver={SQL Server};Server=192.168.18.14\LWLTEST,55954;Database=master;UID=sa;PWD=Supos1304;' AS "fqe_MSSL2016" (TYPE odbc_scanner);

SELECT * FROM fqe_mssl2016.dbo.alltypestable;

SELECT * FROM fqe_mssl2000.dbo.t1 UNION SELECT * FROM fqe_mssl2000.dbo.t2;