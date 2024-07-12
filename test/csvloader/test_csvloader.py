# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import gc
import os
import shutil
import time
import unittest

import duckdb
import h3

from loader.loader_factory import LoaderFactory


class TestCSVLoader(unittest.TestCase):
    tmp_folder = "./test/test_data/csvloader/tmp"

    def setUp(self) -> None:
        if os.path.exists(self.tmp_folder):
            shutil.rmtree(self.tmp_folder)
        os.mkdir(self.tmp_folder)
        self.database_dir = self.tmp_folder

    def tearDown(self) -> None:
        # needed as databases only release lock on files when garbage collected
        #  without this, the delete operation will fail due to file locks
        gc.collect()
        time.sleep(0.1)
        if os.path.exists(self.tmp_folder):
            shutil.rmtree(self.tmp_folder)

    def test_h3_correct_cell_number(self):
        config_path = "./test/test_data/csvloader/h3_no_header_conf.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(self.database_dir, db_name)

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

        self.assertEqual(122, h0_count[0])
        self.assertEqual(842, h1_count[0])
        self.assertEqual(5882, h2_count[0])

    def test_h3_does_interpolation(self):
        config_path = "./test/test_data/csvloader/h3_no_header_conf.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(self.database_dir, db_name)

        connection = duckdb.connect(database_path)

        cell = h3.geo_to_h3(50.25, 50.25, 2)

        interp = connection.execute(
            f"select mydata from {ds_name}_2 where h3_cell = '{cell}'"
        ).fetchone()[0]

        # at 50,50 value is 0, at 51,51 value is 1000
        is_correct = 0 < interp < 1000

        self.assertTrue(is_correct)

    def test_point_dataset(self):
        config_path = "./test/test_data/csvloader/point_no_header_conf.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(self.database_dir, db_name)

        connection = duckdb.connect(database_path)

        h0_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        self.assertEqual(5, h0_count[0])

    def test_point_maps_lat_long_to_cells(self):
        config_path = "./test/test_data/csvloader/point_no_header_conf.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(self.database_dir, db_name)

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
                self.assertEqual(actual_cell, expected_cell)

    def test_insert_into_existing_table(self):
        config_path = "./test/test_data/csvloader/point_no_header_conf.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(self.database_dir, db_name)

        connection = duckdb.connect(database_path)

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        self.assertEqual(5, point_count[0])

        insert_config = "./test/test_data/csvloader/" \
                        "point_no_header_insert_conf.yml"
        loader = LoaderFactory.create_loader(insert_config)
        loader.load()

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        self.assertEqual(6, point_count[0])


    def test_fails_create_if_table_exists(self):
        config_path = "./test/test_data/csvloader/point_no_header_conf.yml"
        loader = LoaderFactory.create_loader(config_path)

        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"

        database_path = os.path.join(self.database_dir, db_name)

        connection = duckdb.connect(database_path)

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        self.assertTrue(point_count[0] > 0)

        def trycreate():
            LoaderFactory.create_loader(config_path).load()

        self.assertRaises(
            ValueError,
            trycreate
        )

    def test_insert_creates_if_no_existing_table(self):
        insert_config = "./test/test_data/csvloader/" \
                        "point_no_header_insert_conf.yml"

        loader = LoaderFactory.create_loader(insert_config)
        loader.load()

        conf_obj = loader.get_config()
        ds_name = conf_obj.dataset_name
        db_name = f"{ds_name}.duckdb"
        database_path = os.path.join(self.database_dir, db_name)

        connection = duckdb.connect(database_path)

        point_count = connection.execute(
            f"select count(*) from {ds_name}"
        ).fetchone()

        self.assertEqual(1, point_count[0])

    def test_fails_on_invalid_config(self):
        config_path = "./test/test_data/csvloader/invalid/invalid_type_conf.yml"

        def trycreate():
            LoaderFactory.create_loader(config_path)

        self.assertRaises(
            ValueError,
            trycreate
        )

    def test_fail_on_file_path_not_exist(self):
        config_path = "./test/test_data/csvloader/invalid/invalid_file_path.yml"

        def trycreate():
            LoaderFactory.create_loader(config_path)

        self.assertRaises(
            ValueError,
            trycreate
        )
