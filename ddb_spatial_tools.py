import duckdb as ddb
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


ddb.load_extension('spatial')

def read_points_from_parquet_file(parquet_file, table_name):
    """
    Reads points from a parquet file and creates a table in DuckDB with the given table name.

    Args:
    - parquet_file (str): The path to the parquet file to read from.
    - table_name (str): The name of the table to create in DuckDB.

    Returns:
    - True if the table was successfully created, False otherwise.
    """
    try:
        ddb.sql(f"""
        CREATE TABLE {table_name} AS
        SELECT *, ST_GEOMFROMWKB(geometry) GEO, ST_Transform((ST_FlipCoordinates(GEO)), 'epsg:4326', 'esri:102001') AS AEAC
        FROM '{parquet_file}'
        """)
        logging.info(f"Created table '{table_name}' from {parquet_file}")
        return True
    except Exception as e:
        logging.error(f"Failed to create table '{table_name}' from {parquet_file}")
        logging.error(e)
        if 'already exists' in str(e):
            # Ask the user if they want to overwrite the table
            overwrite = input(f"Table '{table_name}' already exists. Overwrite? (y/n): ")
            if overwrite.lower() == 'y':
                ddb.sql(f"DROP TABLE {table_name}")
                logging.info(f"Dropped table '{table_name}'")
                read_points_from_parquet_file(parquet_file, table_name)
            else:
                logging.info(f"Fine, I won't overwrite '{table_name}'")

def create_vector_grid(ddb_table, geometry_column, grid_size, crs_code):
    """
    Creates a vector grid based on the bounding box of a DuckDB table.

    Args:
    - ddb_table (str): The name of the DuckDB table to create the vector grid from.
    - geometry_column (str): The name of the geometry column in the DuckDB table.
    - grid_size (float): The size of the grid cells in the vector grid.

    Returns:
    - A GeoDataFrame containing the vector grid.
    """
    import geopandas as gpd
    from shapely.geometry import Polygon
    import numpy as np


    xmin = ddb.sql(f'SELECT MIN(ST_X({geometry_column})) FROM {ddb_table}').df().iloc[0,0]
    ymin = ddb.sql(f'SELECT MIN(ST_Y({geometry_column})) FROM {ddb_table}').df().iloc[0,0]
    xmax = ddb.sql(f'SELECT MAX(ST_X({geometry_column})) FROM {ddb_table}').df().iloc[0,0]
    ymax = ddb.sql(f'SELECT MAX(ST_Y({geometry_column})) FROM {ddb_table}').df().iloc[0,0]

    height = grid_size
    width = grid_size

    cols = list(np.arange(xmin, xmax + width, width))
    rows = list(np.arange(ymin, ymax + height, height))

    polygons = []
    for x in cols[:-1]:
        for y in rows[:-1]:
            polygons.append(Polygon([(x,y), (x+width, y), (x+width, y+height), (x, y+height)]))

    grid = gpd.GeoDataFrame({'geometry':polygons})
    grid.set_crs(crs=f'{crs_code}', inplace=True)

    return grid

def validate_geoparquet_file(file_path):
    """
    Validates a GeoParquet file.
    Requires the validate_geoparquet.py script to be in the same directory.
    See https://gdal.org/drivers/vector/parquet.html#validation-script
    and
    https://github.com/OSGeo/gdal/blob/master/swig/python/gdal-utils/osgeo_utils/samples/validate_geoparquet.py
    """

    import os
    os.system(f"python3 validate_geoparquet.py --check-data {file_path}")