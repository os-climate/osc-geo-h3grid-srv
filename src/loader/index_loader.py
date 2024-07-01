import logging

import duckdb
import geopandas
import h3
import numpy
import pandas
import rasterio
import xarray
from geopandas import GeoDataFrame

raw = "./data/geo_data/flood/europe_flood_data/data/River_flood_depth_1971_2000_hist_0010y.tif"

ds_name = "flood_depth_10_year_spain_res9_index"

out = f"./tmp/{ds_name}.duckdb"

resolution = 9

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(module)s:%(funcName)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

class IndexLoaderTemp:

    def load_flood_data(
            self,
            tiff_file: str
    ) -> GeoDataFrame:
        logger.info(f"Loading flood data, tiff_file:{tiff_file}")

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
        logger.info("GeoDataFrame assembled")

        epsg = 4326
        logger.info(f"GeoDataFrame conversion to CRS epsg:{epsg} (started)")
        out = geo.to_crs(epsg=epsg)
        logger.info(f"GeoDataFrame conversion to CRS epsg:{epsg} (complete)")

        return out

    def fix_columns(self, geo: GeoDataFrame) -> GeoDataFrame:
        logger.info("Fixing columns")

        geo['longitude'] = geo.geometry.x
        geo['latitude'] = geo.geometry.y

        out = geo.drop(columns=['x', 'y', 'geometry'])
        return out

    def aggregate_by_cell(self, gdf: GeoDataFrame, res_col: str) -> GeoDataFrame:

        groups = gdf.groupby(res_col)['value']

        with_stats = groups.agg(['min', 'max', 'mean', 'median'])

        return with_stats.reset_index()

    def filter_spain(self, geo: GeoDataFrame) -> GeoDataFrame:
        logger.info("Filtering for Spain")
        min_lat = 35.50
        max_lat = 44.31
        min_long = -9.98
        max_long = 4.71

        out = geo[
            (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
            (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
            ]
        return out


    def process(self, raw_tiff_path: str, res: int):
        gdf = self.load_flood_data(raw_tiff_path)
        gdf_fix_col = self.fix_columns(gdf)

        cell_col = f"res{res}"

        def to_cell(row):
            lat = row['latitude']
            long = row['longitude']
            return h3.geo_to_h3(lat, long, res)

        gdf_fix_col[cell_col] = gdf_fix_col.apply(to_cell, axis='columns')
        # gdf_fix_col.drop(columns=['latitude', 'longitude'])

        spain = self.filter_spain(gdf_fix_col)


        with_stats = self.aggregate_by_cell(spain, cell_col)
        # ignore IDE saying this is unused. It is referenced in the sql text
        #  and pandas will pick it up via reflection


        connection = duckdb.connect(database=out)

        sql = f"CREATE TABLE {ds_name}" \
              f" as select * from with_stats"

        connection.sql(
            sql
        )


if __name__ == "__main__":
    loader = IndexLoaderTemp()

    loader.process(raw, 9)

    print("done")
