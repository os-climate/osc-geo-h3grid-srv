# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import csv
import os.path
from dataclasses import dataclass
from typing import List, Dict

from pandas import DataFrame

from geoserver.bgsexception import WrongColumnNumberException, \
    InvalidColumnTypeException, \
    BgsException, BgsNotFoundException, WrongFileTypeException, ConfigException
from geoserver.loader.abstract_loader import AbstractLoaderConfig, \
    AbstractLoader

SUPPORTED_DATA_TYPES = [
    "float",
    "str",
    "int"
]


@dataclass
class CSVLoaderConfig(AbstractLoaderConfig):

    def __init__(self, **entries):
        self.__dict__.update(entries)

    file_path: str

    has_header_row: bool

    columns: Dict[str, str]  # list of column names in order, mapped to type

    pass


class CSVLoader(AbstractLoader):

    name: str = "CSVLoader"

    def __init__(
            self,
            config: CSVLoaderConfig
    ):
        for t in config.columns.values():
            if t not in SUPPORTED_DATA_TYPES:
                raise InvalidColumnTypeException(
                    f"column type {t} is not a supported type."
                    f" supported types are {SUPPORTED_DATA_TYPES}"
                )

        self.config = config
        self.validate_config()
        self.dataset = None

    def get_config(self) -> CSVLoaderConfig:
        return self.config

    def load(self) -> None:
        conf = self.get_config()
        out: List[Dict] = []
        with open(conf.file_path) as csvfile:
            reader = csv.reader(csvfile)
            is_first = True
            for row in reader:
                if conf.has_header_row and is_first:
                    # skip header row
                    is_first = False
                    continue
                else:
                    is_first = False

                if len(row) != len(conf.columns):
                    raise WrongColumnNumberException(
                        f"configuration expects {len(conf.columns)} columns,"
                        f" but row had {len(row)} columns."
                    )
                zipped = zip(conf.columns.keys(), list(row))

                row_as_dict = {}
                for name, s in zipped:
                    row_as_dict[name] = self._cast_str_to_type(
                        s,
                        conf.columns[name]
                    )

                out.append(row_as_dict)

        # IDE says that type is wrong. It is not.
        # noinspection PyTypeChecker
        dataset = DataFrame.from_dict(out)

        self.dataset = dataset

        if conf.dataset_type == "h3":
            super().to_h3_dataset(conf.mode)
        elif conf.dataset_type == "point":
            super().to_point_dataset(conf.mode)

    def validate_config(self) -> None:
        conf = self.get_config()

        if not os.path.exists(conf.file_path):
            raise BgsNotFoundException(f"file {conf.file_path} does not exist")

        if os.path.isdir(conf.file_path):
            raise WrongFileTypeException(
                f"file {conf.file_path} is a directory, not a file"
            )

        for name, col_type in conf.columns.items():
            if col_type not in SUPPORTED_DATA_TYPES:
                raise InvalidColumnTypeException(
                    f"column type {col_type} for column {name}"
                    f" is not a supported type. supported types are"
                    f" {SUPPORTED_DATA_TYPES}"
                )

        for column_name in conf.data_columns:
            if column_name not in conf.columns.keys():
                raise ConfigException(
                    f"column {column_name} in data_columns is not"
                    f" present in columns configuration element."
                )

        super().validate_config()

    def get_raw_dataset(self) -> DataFrame:
        if self.dataset is None:
            raise BgsNotFoundException(
                "dataset not yet loaded. Call the load() method before"
                " attempting to use the dataset.")
        return self.dataset

    def _cast_str_to_type(
            self,
            s: str,
            type_str: str
    ):
        if type_str not in SUPPORTED_DATA_TYPES:
            raise InvalidColumnTypeException(
                f"cannot cast str to {type_str} as type is unsupported."
                f" supported types are {SUPPORTED_DATA_TYPES}"
            )
        if type_str == "str":
            return s
        elif type_str == "int":
            return int(s)
        elif type_str == "float":
            return float(s)
        else:
            raise BgsException(
                f"conversion of supported type {type_str} not yet implemented"
            )