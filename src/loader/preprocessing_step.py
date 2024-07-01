import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional

import geopandas
import pandas
from pandas import DataFrame
from shape.shape import Shape
from common.const import LOGGING_FORMAT

# Set up logging

logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class PreprocessingStep(ABC):
    @abstractmethod
    def __init__(self, conf_dict: Dict[str, str]):
        pass

    @abstractmethod
    def run(self, input_df: DataFrame) -> DataFrame:
        pass


@dataclass
class ShapefileFilterConf:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    shapefile_path: str
    region: Optional[str] = None


class ShapefileFilter(PreprocessingStep):

    def __init__(self, conf_dict: Dict[str, str]):
        logger.debug(f"creating ShapefileFilter with conf {conf_dict}")
        self.conf = ShapefileFilterConf(**conf_dict)
        self.validate_conf(self.conf)

    def validate_conf(self, conf: ShapefileFilterConf):
        if not os.path.exists(conf.shapefile_path):
            raise ValueError(
                f"shapefile {conf.shapefile_path} specified in ShapefileFilter"
                f" conf does not exist.")
        shape = Shape(self.conf.shapefile_path)
        if conf.region is not None and \
                not shape.contains_region(conf.region):
            raise ValueError(f"shapefile {conf.shapefile_path} did not contain"
                             f"specified region {conf.region}")

    def run(self, input_df: DataFrame) -> DataFrame:
        logger.info("running ShapeFileFilter")
        shape = Shape(self.conf.shapefile_path)
        epsg = 4326

        logger.info("temporarily converting DataFrame to GeoDataFrame for"
                    " shapefile filtering.")
        in_as_geodf = geopandas.GeoDataFrame(
            input_df,
            geometry=geopandas.points_from_xy(
                input_df.longitude,
                input_df.latitude
            ),
            crs=epsg
        )

        logger.info("filtering based on shapefile")
        out_geo = shape.dataframe_points_within_shape(
            in_as_geodf, self.conf.region)

        out_geo = out_geo.drop(columns=['geometry', 'index_right', 'name'])

        return pandas.DataFrame(out_geo)
