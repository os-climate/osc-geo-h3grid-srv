# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
from typing import Any, Dict
import yaml

from geoserver.bgsexception import InvalidConfException, \
    InvalidArgumentException, BgsNotFoundException
from geoserver.loader import AbstractLoader, CSVLoader, CSVLoaderConfig
from geoserver.loader.parquet_loader import ParquetLoader, ParquetLoaderConfig

SUPPORTED_LOADER_TYPES = [
    CSVLoader.name
]


class LoaderFactory:

    @staticmethod
    def create_loader(file_path: str) -> AbstractLoader:
        try:
            conf_dict = LoaderFactory._load_dict_from_yml(file_path)
        except FileNotFoundError as e:
            raise BgsNotFoundException("") from e

        l_type = conf_dict.get("loader_type")
        if l_type is None:
            raise InvalidConfException(
                f"mandatory parameter loader_type not found in conf file:"
                f" {file_path}")
        elif l_type == CSVLoader.name:
            return LoaderFactory._create_csv_loader(conf_dict)
        elif l_type == ParquetLoader.name:
            return LoaderFactory._create_parquet_loader(conf_dict)
        else:
            raise InvalidArgumentException(
                f"loader type {l_type} is not supported. Supported"
                f" types are: {SUPPORTED_LOADER_TYPES}")
        pass

    @staticmethod
    def _load_dict_from_yml(file_path: str) -> Dict[str, Any]:
        with open(file_path, "r") as c_file:
            d: Dict[str, Any] = yaml.safe_load(c_file)
        return d

    @staticmethod
    def _create_csv_loader(conf_dict: Dict[str, Any]) -> CSVLoader:
        conf = CSVLoaderConfig(**conf_dict)
        return CSVLoader(conf)

    @staticmethod
    def _create_parquet_loader(conf_dict: Dict[str, Any]) -> ParquetLoader:
        conf = ParquetLoaderConfig(**conf_dict)
        return ParquetLoader(conf)

