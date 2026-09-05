# odbc_catalog_scanner

`odbc_catalog_scanner` is a read-only DuckDB storage extension for SAP HANA, Dameng, and IBM Db2 through their
ODBC drivers. It is intentionally limited to these dialects and does not accept arbitrary ODBC connection strings
or raw remote SQL.

## Build

```bash
make release
make test_release
```

## Configuration

Credentials are supplied through a named DuckDB secret. The secret value is not accepted in the `ATTACH` path.

```sql
LOAD odbc_catalog_scanner;

CREATE TEMPORARY SECRET hana_source (
    TYPE odbc_catalog,
    DIALECT 'hana',
    DRIVER 'HDBODBC',
    HOST 'hana.example',
    PORT 30015,
    DATABASE 'SYSTEMDB',
    USER 'reader',
    PASSWORD 'secret'
);

ATTACH '' AS hana_catalog (
    TYPE odbc_catalog,
    READ_ONLY,
    SECRET hana_source,
    DIALECT 'hana',
    SCHEMA 'APP'
);

SELECT * FROM hana_catalog.APP.example_table;
SELECT * FROM odbc_catalog_metadata('hana_catalog', 'APP');
```

Valid `DIALECT` values are `hana`, `dameng`, and `db2`. The value specified by `ATTACH` must equal the value in
the secret. `SCHEMA` is optional; when omitted, tables must be referenced with an explicit schema unless DuckDB
can resolve one from the session catalog context.

The extension exposes catalog table scans, `odbc_catalog_metadata`, and `odbc_catalog_clear_cache`. Create,
alter, insert, update, delete, raw-query execution, and writable attachment are not exposed.

## Runtime settings

- `odbc_catalog_connection_limit`: maximum active connections for each attached catalog; default `64`.
- `odbc_catalog_connection_cache`: whether returned connections are cached; default `true`.
- `odbc_catalog_pool_wait_timeout_millis`: maximum connection-pool wait time; default `30000`.

The extension links to unixODBC on Linux. Install and register the database vendor's ODBC driver separately.
