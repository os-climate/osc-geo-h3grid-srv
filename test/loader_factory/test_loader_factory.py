# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-05-29 by davis.broda@brodagroupsoftware.com
import gc
import os
import shutil
import time

import pytest

from loader.loader_factory import LoaderFactory
from loader.csvloader import CSVLoader
from loader.parquet_loader import ParquetLoader

tmp_folder = "./test/test_data/loader_factory/tmp"

@pytest.fixture()
def database_dir():
    if os.path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)
    os.mkdir(tmp_folder)

    yield tmp_folder

    gc.collect()
    time.sleep(0.1)
    if os.path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)


class TestLoaderFactory:

    def test_csv_config_makes_csv_loader(self, database_dir):
        config_path = "./test/test_data/loader_factory/csv_loader_type.yml"
        loader = LoaderFactory.create_loader(config_path)

        assert type(loader) == CSVLoader

    def test_parquet_config_makes_parquet_loader(self, database_dir):
        config_path = "./test/test_data/loader_factory/parquet_loader_type.yml"
        loader = LoaderFactory.create_loader(config_path)

        assert type(loader) == ParquetLoader

    def test_exception_on_unknown_type(self, database_dir):
        config_path = "./test/test_data/loader_factory/unknown_loader_type.yml"
        with pytest.raises(ValueError):
            LoaderFactory.create_loader(config_path)

    def test_exception_on_conf_not_exist(self):
        config_path = "./test/test_data/loader_factory/not_exist_conf.yml"
        with pytest.raises(Exception):
            LoaderFactory.create_loader(config_path)


    def test_exception_on_db_dir_not_exist(self):
        config_path = "./test/test_data/loader_factory/no_db_dir.yml"
        with pytest.raises(ValueError):
            LoaderFactory.create_loader(config_path)