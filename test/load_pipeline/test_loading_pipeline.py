import gc
import os
import shutil
import time
from typing import List, Tuple, Dict

import duckdb
import pytest
from pandas import DataFrame

from loader.aggregation_step import MinAggregation, MaxAggregation
from loader.load_pipeline import LoadingPipeline
from loader.output_step import LocalDuckdbOutputStep
from loader.postprocessing_step import MultiplyValue
from loader.preprocessing_step import PreprocessingStep
from loader.reading_step import ParquetFileReader

data_dir = "./test/test_data/loading_pipeline/"


tmp_folder = f"{data_dir}/tmp"

@pytest.fixture()
def database_dir():
    if os.path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)
    os.mkdir(tmp_folder)

    yield tmp_folder

    # gc + delay is necessary as without manual call tests may complete
    #  before db instances are cleaned up. This causes a file lock to persist
    #  that prevents cleanup of the temp directory.
    gc.collect()
    time.sleep(0.1)
    if os.path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)


def read_temp_db(dataset_name: str) -> List[Tuple]:
    db_path = f"{tmp_folder}/{dataset_name}.duckdb"
    table = f"{dataset_name}"
    connection = duckdb.connect(db_path)

    out = connection.execute(
        f"select * from {table}"
    ).fetchall()

    return out

class AddOnePre(PreprocessingStep):

    def __init__(self, conf_dict: Dict[str, str]):
        pass

    def run(self, input_df: DataFrame) -> DataFrame:
        for col in input_df:
            if col == 'latitude' or col == 'longitude':
                continue
            input_df[col] = input_df[col] + 1
        return input_df


class TestLoadingPipeline:



    def test_read_out_only(self, database_dir):
        parquet_file = data_dir + "/2_cell_agg.parquet"
        dataset = "read_out_only"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        pipeline = LoadingPipeline(
            read_step, [], [], [], output_step, 1
        )

        pipeline.run()

        out = read_temp_db(dataset)

        # the same as the raw data in the initial file
        expected = {
            (50, 50, 10, 100),
            (50.1, 50.1, 0, 0),
            (50.2, 50.2, 2, 20),
            (-50, -50, 10, 100),
            (-50.1, -50.1, 0, 0),
            (-50.2, -50.2, 2, 20),
        }

        assert set(out) == expected

    def test_read_out_preprocess(self, database_dir):
        parquet_file = data_dir + "/2_cell_agg.parquet"
        dataset = "read_out_only"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        pre_step = AddOnePre({})

        pipeline = LoadingPipeline(
            read_step, [pre_step], [], [], output_step,1
        )

        pipeline.run()

        out = read_temp_db(dataset)

        # the same as the raw data in the initial file
        expected = {
            (50, 50, 11, 101),
            (50.1, 50.1, 1, 1),
            (50.2, 50.2, 3, 21),
            (-50, -50, 11, 101),
            (-50.1, -50.1, 1, 1),
            (-50.2, -50.2, 3, 21),
        }

        assert set(out) == expected

    def test_read_out_aggregate(self, database_dir):
        parquet_file = data_dir + "/2_cell_agg.parquet"
        dataset = "read_out_only"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        agg_steps = [
            MinAggregation({}),
            MaxAggregation({})
        ]

        pipeline = LoadingPipeline(
            read_step, [], agg_steps, [], output_step, 1
        )

        pipeline.run()

        out = read_temp_db(dataset)

        # the same as the raw data in the initial file
        expected = {
            ('8110bffffffffff', 0, 10, 0, 100),
            ('81defffffffffff', 0, 10, 0, 100),
        }

        assert set(out) == expected

    def test_fail_if_agg_but_no_res(self, database_dir):
        parquet_file = data_dir + "/2_cell_agg.parquet"
        dataset = "read_out_only"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        agg_steps = [
            MinAggregation({}),
            MaxAggregation({})
        ]

        with pytest.raises(ValueError):
            LoadingPipeline(
                read_step, [], agg_steps, [], output_step, None
            )

    def test_read_out_post(self, database_dir):
        parquet_file = data_dir + "/2_cell_agg.parquet"
        dataset = "read_out_only"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        post_step = MultiplyValue({"multiply_by": 2})

        pipeline = LoadingPipeline(
            read_step, [], [], [post_step], output_step, 1
        )

        pipeline.run()

        out = read_temp_db(dataset)

        # the same as the raw data in the initial file
        expected = {
            (50, 50, 10 * 2, 100 * 2),
            (50.1, 50.1, 0 * 2, 0 * 2),
            (50.2, 50.2, 2 * 2, 20 * 2),
            (-50, -50, 10 * 2, 100 * 2),
            (-50.1, -50.1, 0 * 2, 0 * 2),
            (-50.2, -50.2, 2 * 2, 20 * 2),
        }

        assert set(out) == expected

    def test_full_pipeline(self, database_dir):
        parquet_file = data_dir + "/2_cell_agg.parquet"
        dataset = "full_pipeline"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        pre_step = AddOnePre({})

        agg_steps = [
            MinAggregation({}),
            MaxAggregation({})
        ]
        post_step = MultiplyValue({"multiply_by": 2})


        pipeline = LoadingPipeline(
            read_step, [pre_step], agg_steps, [post_step], output_step, 1
        )

        pipeline.run()

        out = read_temp_db(dataset)

        def f(i: int):
            return (i + 1) * 2
        # the same as the raw data in the initial file
        expected = {
            ('8110bffffffffff', f(0), f(10), f(0), f(100)),
            ('81defffffffffff', f(0), f(10), f(0), f(100)),
        }

        assert set(out) == expected

    def test_additional_key_cols(self, database_dir):
        parquet_file = data_dir + "with_company.parquet"
        dataset = "read_out_only"

        read_step = ParquetFileReader({
            "file_path": parquet_file,
            "data_columns": ["value1", "value2"],
            "key_columns": ["company"]
        })

        output_step = LocalDuckdbOutputStep({
            "database_dir": database_dir,
            "dataset_name": dataset,
            "mode": "create"
        })

        agg_steps = [
            MinAggregation({}),
            MaxAggregation({})
        ]

        pipeline = LoadingPipeline(
            read_step, [], agg_steps, [], output_step, 1
        )

        pipeline.run()

        out = read_temp_db(dataset)

        # the same as the raw data in the initial file
        expected = {
            ('company1', '8110bffffffffff', 0, 10, 0, 100),
            ('company2', '8110bffffffffff', 2, 2, 20, 20),
            ('company1', '81defffffffffff', 0, 10, 0, 100),
            ('company2', '81defffffffffff', 2, 2, 20, 20)
        }

        assert set(out) == expected
