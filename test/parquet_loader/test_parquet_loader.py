# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-05-24 by davis.broda@brodagroupsoftware.com
import gc
import shutil
import time
import unittest
import os

import duckdb
import h3
import pytest as pytest

from loader.loader_factory import LoaderFactory

tmp_folder = "./test/test_data/parquet_loader/tmp"


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


class TestParquetLoader:

    def test_h3_correct_cell_number(self, database_dir):
        config_path = "./test/test_data/parquet_loader/can_usa_h3.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(database_dir, db_name)

        connection = duckdb.connect(database_path)

        h0_count = connection.execute(
            f"select count(*) from {ds_name}_0"
        ).fetchone()
        h1_count = connection.execute(
            f"select count(*) from {ds_name}_1"
        ).fetchone()
        h2_count = connection.execute(
            f"select count(*) from {ds_name}_2"
        ).fetchone()

        assert 122 == h0_count[0]
        assert 842 == h1_count[0]
        assert 5882 == h2_count[0]

    def test_h3_does_interpolation(self, database_dir):
        config_path = "./test/test_data/parquet_loader/can_usa_h3.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(database_dir, db_name)

        connection = duckdb.connect(database_path)

        cell = h3.geo_to_h3(51, -102.25, 2)

        interp = connection.execute(
            f"select value2 from {ds_name}_2 where h3_cell = '{cell}'"
        ).fetchone()[0]

        # at 51,-102 value is 90, at 51, -102.5 value is 80
        is_correct = 80 < interp < 90

        assert is_correct, f"value {interp} was not between 90 and 80"

    def test_point_maps_lat_long_to_cells(self, database_dir):
        config_path = "./test/test_data/parquet_loader/can_usa_point.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(database_dir, db_name)

        connection = duckdb.connect(database_path)

        results = connection.execute(
            f"select latitude, longitude, res0, res1, res2 from {ds_name}"
        ).fetchall()

        for result in results:
            lat = result[0]
            long = result[1]

            for res in range(0, 3):
                expected_cell = h3.geo_to_h3(lat, long, res)
                actual_cell = result[2 + res]
                assert actual_cell == expected_cell,\
                    f"actual cell {actual_cell} did not match expected cell" \
                    f" {expected_cell} for lat long: ({lat},{long}), in " \
                    f" resolution {res}"

    def test_insert_into_existing_table(self, database_dir):
        config_path = "./test/test_data/parquet_loader/can_usa_point.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(database_dir, db_name)

        connection = duckdb.connect(database_path)

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        initial_count = point_count[0]

        insert_config = \
            "./test/test_data/parquet_loader/can_usa_insert_point.yml"
        loader = LoaderFactory.create_loader(insert_config)
        loader.load()

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        assert point_count[0] == initial_count + 1,\
            f"expected {initial_count + 1} points, but found {point_count[0]}"

    def test_fails_create_if_table_exists(self, database_dir):
        config_path = "./test/test_data/parquet_loader/can_usa_point.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(database_dir, db_name)

        connection = duckdb.connect(database_path)

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        assert point_count[0] > 0

        def trycreate():
            LoaderFactory.create_loader(config_path).load()

        with pytest.raises(ValueError):
            trycreate()

    def test_insert_creates_if_no_existing_table(self, database_dir):
        insert_config = \
            "./test/test_data/parquet_loader/can_usa_insert_point.yml"

        loader = LoaderFactory.create_loader(insert_config)
        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"
        database_path = os.path.join(database_dir, db_name)

        connection = duckdb.connect(database_path)

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        assert 1 == point_count[0], "expected 1 point, got {point_count[0]}"