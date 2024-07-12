# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

import duckdb
import h3
import pandas
from pandas import DataFrame

from common import duckdbutils
from loader import interpolator

LOADING_MODES = [
    "insert",
    "create"
]

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

DEFAULT_NUM_NEIGHBORS = 3
DEFAULT_POWER = 2


class AbstractLoaderConfig(ABC):
    """
    Class contains all values that are used in AbstractLoader,
    and is intended to be overridden if additional values
    are needed for implementations
    """
    loader_type: str

    dataset_name: str
    dataset_type: str
    database_dir: str

    interval: str
    max_resolution: int

    data_columns: List[str]

    year_column: Optional[str] = None
    month_column: Optional[str] = None
    day_column: Optional[str] = None

    shapefile: Optional[str] = None
    region: Optional[str] = None

    mode: str

    max_parallelism: int = 4

    def get_time_cols(self) -> List[str]:
        acc = [self.year_column, self.month_column, self.day_column]
        return list(filter(
            lambda x: x is not None,
            acc
        ))


class AbstractLoader(ABC):

    @abstractmethod
    def get_raw_dataset(self) -> DataFrame:
        pass

    @abstractmethod
    def get_config(self) -> AbstractLoaderConfig:
        pass

    @abstractmethod
    def load(self) -> None:
        pass

    def _get_shapefile_info(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns information about the shapefile used to
        :return: Tuple of shapefile path, and region name, if they exist
        :rtype: Tuple[Optional[str], Optional[str]]
        """
        conf = self.get_config()
        return conf.shapefile, conf.region

    def validate_config(self):
        conf = self.get_config()

        if conf.dataset_name is None:
            raise ValueError("mandatory parameter dataset_name is empty")
        if conf.dataset_type is None:
            raise ValueError("mandatory parameter dataset_type is empty")

        if conf.database_dir is None:
            raise ValueError("mandatory parameter database_dir is empty")
        elif not os.path.exists(conf.database_dir):
            raise ValueError(
                f"file {conf.database_dir} does not exist")
        elif os.path.isfile(conf.database_dir):
            raise ValueError(
                f"database_dir: {conf.database_dir} was a file, not a directory"
            )

        if conf.interval is None:
            raise ValueError("mandatory parameter interval is empty")
        # TODO: validate that interval string is on valid interval list

        if conf.max_resolution is None:
            raise ValueError("mandatory parameter max_resolution is empty")
        if conf.max_resolution > 15 or conf.max_resolution < 0:
            raise ValueError(
                "h3 resolutions must be between 0 and 15. provided resolution"
                f" was {conf.max_resolution}"
            )

        if conf.data_columns is None or len(conf.data_columns) == 0:
            raise ValueError("mandatory parameter data_columns is empty")

        if conf.mode not in LOADING_MODES:
            raise ValueError(
                f"loading mode {conf.mode} is not valid. valid modes are "
                f"{LOADING_MODES}"
            )



    def to_h3_dataset(self, mode: str):
        logger.info("loading dataset as h3 dataset")

        dataset = self.get_raw_dataset()
        # dataset assumed to have longitude, latitude columns

        meta = self.get_config()
        intplr = interpolator.Interpolator(geo_out_db_dir=meta.database_dir)

        for resolution in range(0, meta.max_resolution + 1):
            table_name = meta.dataset_name + f"_{resolution}"
            db_name = meta.dataset_name + ".duckdb"
            db_path = os.path.join(meta.database_dir, db_name)
            connection = duckdb.connect(database=db_path)
            exists = duckdbutils.duckdb_check_table_exists(
                connection, table_name)

            sql = ""
            if exists:
                if mode == "create":
                    raise ValueError(
                        f"table {table_name} already exists."
                        f"cannot insert into table in 'create' mode")
                elif mode == "insert":
                    if len(meta.get_time_cols()) == 0:
                        # TODO: also check that if time cols are present,
                        #  that the data does not overlap existing values
                        raise ValueError(
                            "Cannot insert into a h3 dataset without specifying"
                            " at least one time column."
                        )
                    sql = f"INSERT INTO {table_name} BY NAME" \
                          f" SELECT * FROM interpolated"
            else:
                sql = f"CREATE TABLE {table_name}" \
                      f" as select * from interpolated"

            this_res_ds = pandas.DataFrame(dataset)  # copy to prevent changes
            shapefile, region = self._get_shapefile_info()

            logger.info(f"interpolating for resolution: {resolution}")
            # IDE says this is unused, but it is referred to by name in the sql
            #  variable, which is able to find it by name
            interpolated = intplr.interpolate_df(
                input_data=this_res_ds,
                cols_to_interpolate=meta.data_columns,
                time_cols=meta.get_time_cols(),
                resolution=resolution,
                num_neighbors=DEFAULT_NUM_NEIGHBORS,
                power=DEFAULT_POWER,
                shapefile=shapefile,
                region=region,
                max_parallelism=meta.max_parallelism
            )
            if interpolated.columns is None or len(interpolated.columns) == 0:
                # handle case here where nothing returned due to shapefile reasons
                #  can happen with small regions at very low resolutions
                logger.warning("could not generate interpolation for"
                               f"resolution {resolution}")
            else:
                connection.sql(
                    sql
                )



    def to_point_dataset(
            self,
            mode: str
    ):
        logger.info("loading dataset as point dataset")
        meta = self.get_config()
        dataset = self.get_raw_dataset()
        # dataset assumed to have latitude, longitude columns

        table_name = meta.dataset_name

        db_name = meta.dataset_name + ".duckdb"
        db_path = os.path.join(meta.database_dir, db_name)
        connection = duckdb.connect(database=db_path)

        exists = duckdbutils.duckdb_check_table_exists(
            connection, table_name)
        sql = ""
        if exists:
            if mode == "create":
                raise ValueError(
                    f"table {table_name} already exists."
                    f"cannot insert into table in 'create' mode")
            elif mode == "insert":
                sql = f"INSERT INTO {table_name} BY NAME" \
                      f" SELECT * FROM dataset"
        else:
            sql = f"CREATE TABLE {table_name}" \
                  f" as select * from dataset"

        for resolution in range(0, meta.max_resolution + 1):
            logger.info(f"getting cells for res {resolution}")
            cell_col = f"res{resolution}"

            def to_cell(row):
                lat = row['latitude']
                long = row['longitude']
                return h3.geo_to_h3(lat, long, resolution)

            dataset[cell_col] = dataset.apply(to_cell, axis='columns')

        connection.sql(
            sql
        )
