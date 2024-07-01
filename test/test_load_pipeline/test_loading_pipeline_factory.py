from loader.aggregation_step import MaxAggregation, MinAggregation
from loader.load_pipeline import LoadingPipelineFactory, LoadingPipeline
from loader.output_step import LocalDuckdbOutputStep
from loader.postprocessing_step import MultiplyValue
from loader.preprocessing_step import ShapefileFilter
from loader.reading_step import ParquetFileReader
from test_load_pipeline.test_loading_pipeline import AddOnePre

data_dir = "./test/test_data/loading_pipeline/"


class TestLoadingPipelineFactory:


    def test_load_conf_read_out_only(self):
        conf_file = data_dir + "read_out_only.yml"
        actual = LoadingPipelineFactory.create_from_conf_file(conf_file)

        expected = LoadingPipeline(
            ParquetFileReader({
                "file_path": "./test/test_data/loading_pipeline/2_cell_agg.parquet",
                "data_columns": ["value1", "value2"]
            }),
            [],
            [],
            [],
            LocalDuckdbOutputStep({
                "database_dir": "./test/test_data/loading_pipeline/tmp",
                "dataset_name": "read_out_only",
                "mode": "create"
            })
        )

        assert isinstance(actual.reading_step, ParquetFileReader)
        assert actual.reading_step.conf == expected.reading_step.conf

        assert isinstance(actual.outputStep, LocalDuckdbOutputStep)
        assert actual.outputStep.conf == expected.outputStep.conf



    def test_load_conf_all_steps(self):
        conf_file = data_dir + "all_steps.yml"
        actual = LoadingPipelineFactory.create_from_conf_file(conf_file)

        expected = LoadingPipeline(
            ParquetFileReader({
                "file_path": "./test/test_data/loading_pipeline/2_cell_agg.parquet",
                "data_columns": ["value1", "value2"]
            }),
            [ShapefileFilter({
                "shapefile_path": "./test/test_data/loading_pipeline/"
                                  "Germany_Cuba_Box/Germany_Cuba_box.shp",
                "region": "Cuba"

            })],
            [
                MinAggregation({}),
                MaxAggregation({})
            ],
            [MultiplyValue({"multiply_by": 2})],
            LocalDuckdbOutputStep({
                "database_dir": "./test/test_data/loading_pipeline/tmp",
                "dataset_name": "read_out_only",
                "mode": "create"
            }),
            res=1
        )

        assert isinstance(actual.reading_step, ParquetFileReader)
        assert actual.reading_step.conf == expected.reading_step.conf

        assert len(actual.preprocess_steps) == len(expected.preprocess_steps)
        assert isinstance(actual.preprocess_steps[0], ShapefileFilter)
        assert actual.preprocess_steps[0].conf ==\
               expected.preprocess_steps[0].conf

        assert len(actual.aggregation_steps) == len(expected.aggregation_steps)
        assert isinstance(actual.aggregation_steps[0], MinAggregation)
        assert isinstance(actual.aggregation_steps[1], MaxAggregation)

        assert len(actual.postprocess_steps) == len(actual.postprocess_steps)
        assert isinstance(actual.postprocess_steps[0], MultiplyValue)
        assert actual.postprocess_steps[0].conf == \
               expected.postprocess_steps[0].conf

        assert isinstance(actual.outputStep, LocalDuckdbOutputStep)
        assert actual.outputStep.conf == expected.outputStep.conf
