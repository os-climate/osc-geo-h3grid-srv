# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-05-13 by davis.broda@brodagroupsoftware.com
import string
import logging

import geopandas
import numpy
import pandas
import rasterio
import xarray
from geopandas import GeoDataFrame

# Add the source to sys.path (this is a short-term fix)
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '../..', 'src'))
print(parent_dir)
sys.path.append(parent_dir)

from geoserver.geomesh import Geomesh
from loader.loader_factory import LoaderFactory
from geoserver.metadata import MetadataDB
from cli.visualizer import HexGridVisualizer

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(module)s:%(funcName)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


temp_dir = "./tmp/load_all_flood/"

tiff_dir = "./data/geo_data/flood/europe_flood_data/data/"
tiff_datasets = [
    {
        "tiff_path": f"{tiff_dir}River_flood_depth_1971_2000_hist_0010y.tif",
        "dataset_prefix": "flood_depth_10_year"
    },
    {
        "tiff_path": f"{tiff_dir}River_flood_depth_1971_2000_hist_0030y.tif",
        "dataset_prefix": "flood_depth_30_year"
    },
    {
        "tiff_path": f"{tiff_dir}River_flood_depth_1971_2000_hist_0100y.tif",
        "dataset_prefix": "flood_depth_100_year"
    },
    {
        "tiff_path": f"{tiff_dir}River_flood_depth_1971_2000_hist_0300y.tif",
        "dataset_prefix": "flood_depth_300_year"
    },
    {
        "tiff_path": f"{tiff_dir}River_flood_depth_1971_2000_hist_1000y.tif",
        "dataset_prefix": "flood_depth_1000_year"
    }

]

shapefile = "./data/shapefiles/WORLD/world-administrative-boundaries.shp"

countries = {
    "germany": {
        "min_lat": 46,
        "max_lat": 56,
        "min_long": 4,
        "max_long": 17,
        "region": "Germany",
        "max_res": 7,
    },
    "france": {
        "min_lat": 41.29,
        "max_lat": 50.97,
        "min_long": -5.91,
        "max_long": 10.14,
        "region": "France",
        "max_res": 7,
    },
    "uk": {
        "min_lat": 50.24,
        "max_lat": 58.95,
        "min_long": -8.70,
        "max_long": 3.68,
        "region": "U.K. of Great Britain and Northern Ireland",
        "max_res": 7,
    },
    "ireland": {
        "min_lat": 51.46,
        "max_lat": 55.61,
        "min_long": -11.13,
        "max_long": -5.50,
        "region": "Ireland",
        "max_res": 7,
    },
    "spain": {
        "min_lat": 35.80,
        "max_lat": 44.19,
        "min_long": -9.76,
        "max_long": 5.89,
        "region": "Spain",
        "max_res": 7,
    },
    "portugal": {
        "min_lat": 36.63,
        "max_lat": 42.38,
        "min_long": -9.98,
        "max_long": -6.54,
        "region": "Portugal",
        "max_res": 7,
    },
    "belgium": {
        "min_lat": 49.25,
        "max_lat": 51.55,
        "min_long": 2.19,
        "max_long": 6.62,
        "region": "Belgium",
        "max_res": 7,
    },
    "netherlands": {
        "min_lat": 50.56,
        "max_lat": 53.71,
        "min_long": 2.82,
        "max_long": 7.73,
        "region": "Netherlands",
        "max_res": 7,
    },
    "poland": {
        "min_lat": 48.63,
        "max_lat": 55.03,
        "min_long": 13.62,
        "max_long": 24.76,
        "region": "Poland",
        "max_res": 7,
    },
    "czechia": {
        "min_lat": 48.20,
        "max_lat": 54.24,
        "min_long": 11.52,
        "max_long": 19.12,
        "region": "Czech Republic",
        "max_res": 7,
    },
    "slovakia": {
        "min_lat": 47.61,
        "max_lat": 49.75,
        "min_long": 16.33,
        "max_long": 22.93,
        "region": "Slovakia",
        "max_res": 7,
    },
    "austria": {
        "min_lat": 46.18,
        "max_lat": 49.26,
        "min_long": 8.71,
        "max_long": 17.49,
        "region": "Austria",
        "max_res": 7,
    },
    "croatia": {
        "min_lat": 42.13,
        "max_lat": 46.50,
        "min_long": 12.99,
        "max_long": 19.57,
        "region": "Croatia",
        "max_res": 7,
    },
    "slovenia": {
        "min_lat": 45.24,
        "max_lat": 47.11,
        "min_long": 13.00,
        "max_long": 16.71,
        "region": "Slovenia",
        "max_res": 7,
    },
    "hungary": {
        "min_lat": 45.71,
        "max_lat": 48.67,
        "min_long": 15.54,
        "max_long": 23.61,
        "region": "Hungary",
        "max_res": 7,
    },
    "italy": {
        "min_lat": 36.54,
        "max_lat": 47.20,
        "min_long": 5.97,
        "max_long": 19.43,
        "region": "Italy",
        "max_res": 7,
    }


}


def process_all_tifs() -> None:
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    for tiff_ds in tiff_datasets:
        path = tiff_ds["tiff_path"]
        ds_prefix = tiff_ds["dataset_prefix"]
        for country_name, country in countries.items():
            min_lat = country["min_lat"]
            max_lat = country["max_lat"]
            min_long = country["min_long"]
            max_long = country["max_long"]
            region = country["region"]
            max_res = country["max_res"]

            ds_name = f"{ds_prefix}_{country_name}"
            db_dir = f"{temp_dir}databases"
            conf_dir = f"{temp_dir}conf"
            vis_dir = f"{temp_dir}visualization"
            vis_file = f"{vis_dir}/{ds_name}.html"
            database_out = f"{db_dir}/{ds_name}.duckdb"

            parquet_dir = f"{temp_dir}parquet"
            if not os.path.exists(parquet_dir):
                os.mkdir(parquet_dir)
            parquet_file = f"{parquet_dir}/{ds_name}.parquet"

            if not os.path.exists(parquet_file):
                geo = tiff_to_geodf(path)
                geo_with_cols = fix_columns(geo)
                filtered = filter_tiff_data(
                    geo_with_cols,
                    min_lat,
                    max_lat,
                    min_long,
                    max_long
                )
                write_parquet(filtered, parquet_file)
            else:
                logger.info("Skipping tiff to parquet conversion as file "
                      f"{parquet_file} already exists")

            conf_file = write_loader_conf(
                db_dir,
                conf_dir,
                parquet_file,
                region,
                max_res,
                ds_name
            )
            if not os.path.exists(database_out):
                load_and_interpolate(conf_file)
            else:
                logger.info(f"Skipping interpolation as {database_out}"
                      f" already exists")
            try:
                addmeta(db_dir, ds_name, country_name, path)
            # TODO: for unclear reasons catching a more specific exception
            #  does not work
            except Exception as e:
                logger.info(f"Metadata for dataset {ds_name} already exists")

            if not os.path.exists(vis_file):
                if not os.path.exists(vis_dir):
                    os.mkdir(vis_dir)
                visualize_ds(
                    ds_name,
                    db_dir,
                    vis_file,
                    max_res,
                    min_lat,
                    max_lat,
                    min_long,
                    max_long
                )
            else:
                logger.info(f"Skipping visualization as file {vis_file}"
                      f" already exists")


def visualize_ds(
        ds_name: str,
        db_dir: str,
        out_file: str,
        res: int,
        min_lat: float,
        max_lat: float,
        min_long: float,
        max_long: float
) -> str:
    geo = Geomesh(db_dir)
    ds = geo.bounding_box_get(
        ds_name,
        res,
        min_lat,
        max_lat,
        min_long,
        max_long,
        None,
        None,
        None
    )
    ds_pandas = pandas.json_normalize(ds)
    vis = HexGridVisualizer(
        ds_pandas,
        "value",
        (0, 0, 255),
        min_lat,
        max_lat,
        min_long,
        max_long
    )
    vis.visualize_dataset(res, out_file, None)
    return out_file


def addmeta(
        db_dir: str,
        ds_name: str,
        country_name: str,
        input_file: str
) -> None:
    meta = MetadataDB(db_dir)

    desc = f"Flood data for {country_name} based on file {input_file}"
    value_col = {"value": "REAL"}
    key_col = {"h3_cell": "VARCHAR"}
    ds_type = "h3"

    meta.add_metadata_entry(
        ds_name,
        desc,
        key_col,
        value_col,
        ds_type
    )


def load_and_interpolate(config_path: str):
    loader = LoaderFactory.create_loader(config_path)
    loader.load()


def write_loader_conf(
        db_dir: str,
        conf_dir: str,
        parquet_path: str,
        region: str,
        max_res: int,
        ds_name: str) -> str:
    template = string.Template("""loader_type: ParquetLoader
dataset_name: ${DS_NAME}
dataset_type: h3
database_dir: ${DATABASE_DIR}
interval: one_time
max_resolution: ${MAX_RES}
data_columns: [value]
max_parallelism: 16

file_path: ${PARQUET_PATH}
mode: create

shapefile: ./data/shapefiles/WORLD/world-administrative-boundaries.shp
region: ${REGION}""")

    if not os.path.exists(conf_dir):
        os.mkdir(conf_dir)
    filled = template.safe_substitute(
        {
            "DS_NAME": ds_name,
            "DATABASE_DIR": db_dir,
            "PARQUET_PATH": parquet_path,
            "REGION": region,
            "MAX_RES": max_res
        }
    )

    conf_file = f"{conf_dir}/{ds_name}.yml"

    if not os.path.exists(conf_file):
        with open(conf_file, 'w') as conf_write:
            conf_write.write(filled)
    return conf_file


def write_parquet(geo: GeoDataFrame,
                  parquet_file: str
                  ) -> str:
    geo.to_parquet(parquet_file)
    return parquet_file


def filter_tiff_data(
        geo: GeoDataFrame,
        min_lat: float,
        max_lat: float,
        min_long: float,
        max_long: float,
) -> GeoDataFrame:
    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
        ]
    return out


def fix_columns(geo: GeoDataFrame) -> GeoDataFrame:
    geo['longitude'] = geo.geometry.x
    geo['latitude'] = geo.geometry.y

    out = geo.drop(columns=['x', 'y', 'geometry'])
    return out


def tiff_to_geodf(tiff_file: str) -> GeoDataFrame:
    with rasterio.open(tiff_file) as t_file:
        crs_temp = t_file.crs
        trans = t_file.transform
        no_data_val = t_file.nodatavals[0]
        file_xr = xarray.open_rasterio(t_file).isel(band=0)

    valid_data_mask = file_xr.data != no_data_val
    # values in tiff at points where condition holds
    data_array = file_xr.data[valid_data_mask]

    # indices in tiff where the condition holds
    y_indices, x_indices = numpy.where(valid_data_mask)
    epsg_x, epsg_y = trans * (x_indices, y_indices)

    if not (len(epsg_x) == len(epsg_y) == len(data_array)):
        raise ValueError("Mismatch in array lengths after processing.")

    df = pandas.DataFrame(
        {
            "x": epsg_x,
            "y": epsg_y,
            "value": data_array.flatten()
        }
    )

    logger.info("DataFrame assembled")

    geo = geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.x, df.y),
        crs=crs_temp
    )
    logger.info("Geo DataFrame assembled")

    epsg = 4326
    logger.info(f"GeoDataFrame conversion to CRS epsg:{epsg} (started)")
    out = geo.to_crs(epsg=epsg)
    logger.info(f"GeoDataFrame conversion to CRS epsg:{epsg} (complete)")

    return out


if __name__ == "__main__":
    process_all_tifs()
