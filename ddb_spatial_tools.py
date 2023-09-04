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

def local_MarCad_csv_to_parquet(file_path):
    import os
    import duckdb as ddb
    ddb.load_extension('spatial')

    csv = file_path
    source = ddb.sql(f"SELECT *, ST_AsWKB(ST_POINT(LON, LAT)) AS geometry FROM read_csv_auto('{csv}', parallel=false)")


    prq = os.path.splitext(csv)[0] + '.parquet'
    ddb.sql(f"COPY source TO '{prq}' (FORMAT PARQUET)")

def marCad_parquet_to_gpq(parquet_file, gpq_file, max_rows=None):
    """
    Converts a MarCad Parquet file to a GeoParquet file.
    ---->> THIS SEEMS TOO SLOW TO BE USEFUL
    """
    from osgeo import ogr
    # Register all OGR drivers
    #ogr.RegisterAll()

    ogr.UseExceptions()



    # Open source Parquet file
    src_ds = ogr.Open(parquet_file, 0)
    if src_ds is None:
        print("Could not open source dataset")
        exit(1)

    # Get the source layer
    src_layer = src_ds.GetLayer()

    #for f in src_layer:
        #print(f.GetField('geom'))#, f.GetGeometryRef().ExportToWkt())

    # Create destination Parquet file
    dest_driver = ogr.GetDriverByName("Parquet")
    if dest_driver is None:
        print("Parquet driver is not available")
        exit(1)

    dest_ds = dest_driver.CreateDataSource(gpq_file)
    if dest_ds is None:
        print("Could not create destination dataset")
        exit(1)

    # Create destination layer
    dest_layer = dest_ds.CreateLayer("layer_name", geom_type=ogr.wkbPoint, options=['GEOMETRY_NAME=geometry', 'COMPRESSION=SNAPPY']) #geom_type=src_layer.GetGeomType())

    # Get a list of the fields in the source layer and add them to the destination layer
    src_defn = src_layer.GetLayerDefn()
    for i in range(src_defn.GetFieldCount()):
        if src_defn.GetFieldDefn(i).GetName() == 'geometry':
            continue
        field_defn = src_defn.GetFieldDefn(i)
        print(field_defn.GetName(), ":", field_defn.GetTypeName())
        dest_layer.CreateField(field_defn)

    # Loop through features to copy them
    src_feature = src_layer.GetNextFeature()
    #print(src_feature)

    if max_rows is None:
        limit = src_layer.GetFeatureCount()
    else:
        limit = max_rows

    count = 0
    while src_feature and count < limit:
        print(count, end='\r')
        #print(src_feature.GetField('geometry'))
        dest_feature = ogr.Feature(dest_layer.GetLayerDefn())
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint_2D(src_feature.GetField('LON'), src_feature.GetField('LAT')) # SHOULD BE ABLE TO USE EXISTING GEOM COLUMN??
        #point.AddGeometry
        #dest_feature.SetFrom(src_feature)
        dest_feature.SetGeometry(point)
        for i in range(src_feature.GetFieldCount()-1):
            dest_feature.SetField(i, src_feature.GetField(i))

        dest_layer.CreateFeature(dest_feature)
        src_feature = src_layer.GetNextFeature()
        #if max_rows is not None:
        count += 1

    # Close datasets
    src_ds = None
    dest_ds = None