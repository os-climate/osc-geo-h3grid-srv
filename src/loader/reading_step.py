import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas
from pandas import DataFrame

from common.const import LATITUDE_COL, LONGITUDE_COL, YEAR_COL, MONTH_COL, \
    DAY_COL

latitude_col = "latitude"


class ReadingStep(ABC):

    @abstractmethod
    def __init__(self, conf_dict: Dict[str, str]):
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    @abstractmethod
    def get_data_cols(self) -> List[str]:
        pass

    @abstractmethod
    def get_key_cols(self) -> List[str]:
        pass


@dataclass
class ParquetFileReaderConf:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    file_path: str
    data_columns: List[str]

    key_columns: List[str] = ()


class ParquetFileReader(ReadingStep):

    def __init__(self, conf_dict: Dict[str, str]):
        self.conf = ParquetFileReaderConf(**conf_dict)
        self.validate_conf(self.conf)
        pass

    def validate_conf(self, conf: ParquetFileReaderConf):
        if not os.path.exists(conf.file_path):
            raise ValueError(
                f"file {conf.file_path} specified in ParquetFileReader conf"
                f" does not exist"
            )

    def read(self) -> DataFrame:
        file_path = self.conf.file_path
        df = pandas.read_parquet(file_path)
        if LATITUDE_COL not in df.columns:
            raise ValueError(
                f"loaded dataset did not include expected column"
                f" {LATITUDE_COL}. Columns were: {df.columns}"
            )
        if LONGITUDE_COL not in df.columns:
            raise ValueError(
                f"loaded dataset did not include expected column"
                f" {LONGITUDE_COL}. Columns were: {df.columns}"
            )

        for col in self.conf.data_columns:
            if col not in df.columns:
                raise ValueError(
                    f"data columns {col} specified in 'data_columns'"
                    f" element of ParquetFileReaderConf did not exist"
                    f" in the loaded data."
                )

        keep_cols = self.conf.data_columns.copy()
        keep_cols.append(LATITUDE_COL)
        keep_cols.append(LONGITUDE_COL)

        keep_cols.extend(self.conf.key_columns)

        drop_cols = df.columns.difference(keep_cols)
        if len(drop_cols) > 0:
            df = df.drop(columns=drop_cols)

        return df

    def get_data_cols(self) -> List[str]:
        return self.conf.data_columns

    def get_key_cols(self) -> List[str]:
        return list(self.conf.key_columns)
