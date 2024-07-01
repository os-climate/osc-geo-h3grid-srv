import pytest
from pandas import DataFrame

from loader.aggregation_step import MinAggregation, CellAggregationStep, \
    MaxAggregation, MeanAggregation, MedianAggregation, CountWithinBounds


@pytest.fixture()
def agg_df() -> DataFrame:
    data = [
        [50, 50, 10, 100],
        [50.1, 50.1, 0, 0],
        [50.2, 50.2, 2, 20],
        [-50, -50, 10, 100],
        [-50.1, -50.1, 0, 0],
        [-50.2, -50.2, 2, 20],
    ]
    df = DataFrame(
        data,
        columns=['latitude', 'longitude', 'value1', 'value2']
    )
    return df


class TestCellAggregationStep:

    def test_output_name_format_one_built_in_agg(self, agg_df):
        agg_step = MinAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert "value1_min" in out.columns
        assert "value2_min" in out.columns

    def test_cell_col_in_output(self, agg_df):
        agg_step = MinAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert "cell" in out.columns

    def test_lat_long_not_in_output(self, agg_df):
        agg_step = MinAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert len(out.columns) == 3
        assert "latitude" not in out.columns
        assert "longitude" not in out.columns

    def test_output_name_format_multiple_built_in_agg(self, agg_df):
        agg_min = MinAggregation({})
        agg_max = MaxAggregation({})
        agg_mean = MeanAggregation({})
        agg_median = MedianAggregation({})
        agg_steps = [agg_min, agg_max, agg_mean, agg_median]
        all_agg = CellAggregationStep(agg_steps, 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)
        all_expected_cols = {
            "cell",
            "value1_min",
            "value2_min",
            "value1_max",
            "value2_max",
            "value1_mean",
            "value2_mean",
            "value1_median",
            "value2_median"
        }

        assert set(out.columns.tolist()) == all_expected_cols

    def test_output_name_format_one_custom_agg(self, agg_df):
        agg_step = CountWithinBounds({"min": 3})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert "value1_within_bounds_3_none" in out.columns
        assert "value2_within_bounds_3_none" in out.columns

    def test_output_name_format_multiple_custom_agg(self, agg_df):
        agg_step_min = CountWithinBounds({"min": 3})
        agg_step_max = CountWithinBounds({"max": 5})
        agg_list = [agg_step_min, agg_step_max]
        all_agg = CellAggregationStep(agg_list, 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert "value1_within_bounds_3_none" in out.columns
        assert "value2_within_bounds_3_none" in out.columns
        assert "value1_within_bounds_none_5" in out.columns
        assert "value2_within_bounds_none_5" in out.columns

    def test_fail_if_output_col_names_reused(self, agg_df):
        # min aggregation always uses a fixed output name, so having
        # two of them forces name reuse
        agg_step = MinAggregation({})
        agg_step_2 = MinAggregation({})
        agg_list = [agg_step, agg_step_2]
        with pytest.raises(ValueError):
            all_agg = CellAggregationStep(agg_list, 1, ['value1', 'value2'])

    def test_aggregates_to_expected_num_cells(self, agg_df):
        agg_step = MinAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert len(out) == 2

