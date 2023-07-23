# duckdb

DuckDB with spatial extension

Things I've noted:

1.  DuckDB spatial doesn't seem able to load GeoParquet's 'real' geometry directly (yet??). Need to convert it from WKB first:
    ddb.sql("CREATE TABLE grid AS SELECT * EXCLUDE geometry, ST_GEOMFROMWKB(geometry) GEO FROM grid")

2.  
