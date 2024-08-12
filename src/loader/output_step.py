import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional, List

import duckdb
import pandas
from pandas import DataFrame
import pandas.io.sql

from common import duckdbutils, const

from common.const import LOGGING_FORMAT
from geoserver.metadata import MetadataDB

# Set up logging

logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)



class OutputStep(ABC):

    @abstractmethod
    def __init__(self, conf_dict: Dict[str, str]):
        pass

    @abstractmethod
    def write(self, in_df: DataFrame) -> None:
        pass

    @abstractmethod
    def _create_metadata(self, df: DataFrame) -> None:
        pass


@dataclass
class LocalDuckdbOutputStepConf:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    # parameters used to create the data storage db
    database_dir: str
    dataset_name: str
    mode: str = "create"
    key_columns: List[str] = ()

    # Metadata parameters
    description: str = ""
    dataset_type: str = "h3_index"



class LocalDuckdbOutputStep(OutputStep):

    SUPPORTED_DS_TYPES = ["h3_index", "point"]

    ALLOWED_MODES = [
        "create",
        "insert"
    ]

    def __init__(self, conf_dict: Dict[str, str]):
        self.conf = LocalDuckdbOutputStepConf(**conf_dict)
        self._vaidate_conf(self.conf)

    def _vaidate_conf(self, conf: LocalDuckdbOutputStepConf):
        if conf.database_dir is None:
            raise ValueError(
                "database_dir was not provided. database_dir is a mandatory"
                " parameter for LocalDuckdbOutputStep")
        if conf.dataset_name is None:
            raise ValueError(
                "dataset_name was not provided. dataset_name is a mandatory"
                " parameter for LocalDuckdbOutputStep")

        if conf.mode not in self.ALLOWED_MODES:
            raise ValueError(
                f"mode {conf.mode} is not allowed in "
            )
        if conf.dataset_type not in self.SUPPORTED_DS_TYPES:
            raise ValueError(
                f"dataset_type {conf.dataset_type} is not a supported type."
                f" supported types are: {self.SUPPORTED_DS_TYPES}"
            )

    def write(self, in_df: DataFrame) -> None:
        logger.info("running LocalDuckDbOutputStep")
        table_name = self.conf.dataset_name
        db_name = self.conf.dataset_name + ".duckdb"
        db_path = os.path.join(self.conf.database_dir, db_name)
        connection = duckdb.connect(database=db_path)

        exists = duckdbutils.duckdb_check_table_exists(
            connection, table_name)

        sql = ""
        if exists:
            if self.conf.mode == "create":
                raise ValueError(
                    f"table {table_name} already exists."
                    f"cannot insert into table in 'create' mode")
            elif self.conf.mode == "insert":
                sql = f"INSERT INTO {table_name} BY NAME" \
                      f" SELECT * FROM in_df"
        else:
            keys = list(self.conf.key_columns)
            if const.CELL_COL in in_df.columns:
                keys.append(const.CELL_COL)

            sql = pandas.io.sql.get_schema(in_df, table_name, keys=keys)
            logger.info(f"creating table {table_name}"
                        f" in local database at {db_path}")
            connection.sql(
                sql
            )
            sql = f"INSERT INTO {table_name} BY NAME" \
                  f" SELECT * FROM in_df"

        logger.info(f"writing to local database at {db_path}")
        connection.sql(
            sql
        )

        self._create_metadata(in_df)


    def _create_metadata(self, df: DataFrame) -> None:
        meta_db = MetadataDB(self.conf.database_dir)

        ds_name = self.conf.dataset_name
        description = self.conf.description

        table_name = self.conf.dataset_name
        schema_str = pandas.io.sql.get_schema(df, table_name,)
        all_cols = self._get_cols_from_schema_str(schema_str)
        k_col_names = list(self.conf.key_columns)
        if const.CELL_COL in df.columns:
            k_col_names.append(const.CELL_COL)

        key_cols = dict(
            [(k, v) for k,v in all_cols.items() if k in k_col_names]
        )

        value_cols = dict(
            [(k, v) for k,v in all_cols.items() if k not in k_col_names]
        )

        meta_db.add_metadata_entry(
            ds_name,
            description,
            key_cols,
            value_cols,
            self.conf.dataset_type
        )

    def _get_cols_from_schema_str(self, schema_str: str) -> Dict[str,str]:
        all_str_rows = schema_str.split("\n")
        all_str_rows.pop(0)  # contains create table <name>
        all_str_rows.pop()  # last row contains closing )

        out = {}
        for row in all_str_rows:
            splits = row.strip().split(' ')
            name = splits[0].replace("\"", "").strip()
            d_type = splits[1].replace(",", "").strip()
            out[name] = d_type

        return out