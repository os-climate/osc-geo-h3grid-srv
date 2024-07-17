import logging
import os
from dataclasses import dataclass
from typing import List, Dict, Optional

import geopandas
import numpy
import pandas
import rasterio
import xarray
from geopandas import GeoDataFrame
from pandas import DataFrame

from common import const
from loader.reading_step import ReadingStep

# Set up logging
logging.basicConfig(level=logging.INFO, format=const.LOGGING_FORMAT)
logger = logging.getLogger(__name__)


@dataclass
class GeotiffReaderConf:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    file_path: str

    # only_handle a single band/field for now
    data_field: str

    min_lat: float = -90
    max_lat: float = 90
    min_long: float = -180
    max_long: float = 180




class GeotiffReader(ReadingStep):
    def __init__(self, conf_dict: Dict[str, str]):
        self.conf = GeotiffReaderConf(**conf_dict)

        pass

    def validate_conf(self, conf: GeotiffReaderConf):
        if not os.path.exists(conf.file_path):
            raise ValueError(
                f"file {conf.file_path} specified in ParquetFileReader conf"
                f" does not exist"
            )

    def read(self) -> DataFrame:

        raw_geo = self._read_raw_rasterio(self.conf.file_path)
        with_fields = self._fix_columns(raw_geo)
        filtered = self._filter_bounding_box(with_fields)
        return filtered

    def get_data_cols(self) -> List[str]:
        return [self.conf.data_field]

    def get_key_cols(self) -> List[str]:
        return []
        pass

    def _read_raw_rasterio(self, file_path: str) -> GeoDataFrame:
        # TODO: figure out how to handle multiple bands
        logger.info(f"loading geotiff file {file_path}")

        with rasterio.open(file_path) as t_file:
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
                self.conf.data_field: data_array.flatten()
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

    def _fix_columns(self, geo: GeoDataFrame) -> GeoDataFrame:
        logger.info("Fixing columns")

        geo['longitude'] = geo.geometry.x
        geo['latitude'] = geo.geometry.y

        out = geo.drop(columns=['x', 'y', 'geometry'])
        return out

    def _filter_bounding_box(self, geo: GeoDataFrame) -> GeoDataFrame:
        logger.info("filtering to bounding box with dimensions: "
                    f"latitude: [{self.conf.min_lat}, {self.conf.max_lat}]"
                    f"longitude: [{self.conf.min_long}, {self.conf.max_long}]")
        out = geo[
            (geo['latitude'] > self.conf.min_lat) &
            (geo['latitude'] < self.conf.max_lat) &
            (geo['longitude'] > self.conf.min_long) &
            (geo['longitude'] < self.conf.max_long)
            ]
        return out
