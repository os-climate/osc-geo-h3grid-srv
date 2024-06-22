# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-04-26 by davis.broda@brodagroupsoftware.com
import os
from dataclasses import dataclass

import pandas
from pandas import DataFrame

from loader.abstract_loader import AbstractLoader, AbstractLoaderConfig

@dataclass
class ParquetLoaderConfig(AbstractLoaderConfig):

    def __init__(self, **entries):
        self.__dict__.update(entries)

    file_path: str


class ParquetLoader(AbstractLoader):
    name: str = "ParquetLoader"

    def __init__(
            self,
            config: ParquetLoaderConfig
    ):
        self.config = config
        self.validate_config()
        self.dataset = None

    def get_config(self) -> ParquetLoaderConfig:
        return self.config

    def validate_config(self) -> None:
        conf = self.get_config()

        if not os.path.exists(conf.file_path):
            raise ValueError(
                f"file {conf.file_path} does not exist")

        if os.path.isdir(conf.file_path):
            raise ValueError(
                f"file {conf.file_path} is a directory, not a file"
            )

    def load(self) -> None:
        conf = self.get_config()
        file_path = conf.file_path
        df = pandas.read_parquet(file_path)

        self.dataset = df

        if conf.dataset_type == "h3":
            super().to_h3_dataset(conf.mode)
        elif conf.dataset_type == "point":
            super().to_point_dataset(conf.mode)

    def get_raw_dataset(self) -> DataFrame:
        if self.dataset is None:
            raise ValueError(
                "dataset not yet loaded. Call the load() method before"
                " attempting to use the dataset.")
        return self.dataset