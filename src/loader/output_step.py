import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict

import duckdb
from pandas import DataFrame

from common import duckdbutils

from common.const import LOGGING_FORMAT

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


@dataclass
class LocalDuckdbOutputStepConf:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    database_dir: str
    dataset_name: str
    mode: str = "create"


class LocalDuckdbOutputStep(OutputStep):
    def __init__(self, conf_dict: Dict[str, str]):
        self.conf = LocalDuckdbOutputStepConf(**conf_dict)

    allowed_modes = [
        "create",
        "insert"
    ]

    def _vaidate_conf(self, conf: LocalDuckdbOutputStepConf):
        if conf.database_dir is None:
            raise ValueError(
                "database_dir was not provided. database_dir is a mandatory"
                " parameter for LocalDuckdbOutputStep")
        if conf.dataset_name is None:
            raise ValueError(
                "dataset_name was not provided. dataset_name is a mandatory"
                " parameter for LocalDuckdbOutputStep")

        if conf.mode not in self.allowed_modes:
            raise ValueError(
                f"mode {conf.mode} is not allowed in "
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
            sql = f"CREATE TABLE {table_name}" \
                  f" as select * from in_df"

        logger.info(f"writing to local database at {db_path}")
        connection.sql(
            sql
        )