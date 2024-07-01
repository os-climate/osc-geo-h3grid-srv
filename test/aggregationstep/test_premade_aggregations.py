import pytest
from pandas import DataFrame

from loader.aggregationstep import MinAggregation, CellAggregationStep, \
    MaxAggregation, MeanAggregation, MedianAggregation, CountWithinBounds


@pytest.fixture()
def agg_df() -> DataFrame:
    data = [
        [50, 50, 10, 100],
        [50.1, 50.1, 0, 0],
        [50.2, 50.2, 2, 20],
        [-50, -50, 20, 1000],
        [-50.1, -50.1, 10, -1000],
        [-50.2, -50.2, 12, 300],
    ]  # USA
    df = DataFrame(
        data,
        columns=['latitude', 'longitude', 'value1', 'value2']
    )
    return df


class TestPremadeAggregations:

    def test_min_agg_gives_min(self, agg_df):
        # should be two cell
        agg_step = MinAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert len(out) == 2  # 2 rows/cells in aggregation
        assert out['value1_min'].tolist() == [0, 10]
        assert out['value2_min'].tolist() == [0, -1000]

    def test_max_agg_gives_max(self, agg_df):
        # should be two cell
        agg_step = MaxAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert len(out) == 2  # 2 rows/cells in aggregation
        assert out['value1_max'].tolist() == [10, 20]
        assert out['value2_max'].tolist() == [100, 1000]

    def test_mean_agg_gives_mean(self, agg_df):
        # should be two cell
        agg_step = MeanAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert len(out) == 2  # 2 rows/cells in aggregation
        assert out['value1_mean'].tolist() == [4.0, 14.0]
        assert out['value2_mean'].tolist() == [40.0, 100.0]

    def test_median_agg_gives_median(self, agg_df):
        # should be two cell
        agg_step = MedianAggregation({})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert len(out) == 2  # 2 rows/cells in aggregation
        assert out['value1_median'].tolist() == [2, 12]
        assert out['value2_median'].tolist() == [20, 300]

    def test_with_bounds_min_only(self, agg_df):
        agg_step = CountWithinBounds({"min": 3})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert out["value1_within_bounds_3_none"].tolist() == [1, 3]
        assert out["value2_within_bounds_3_none"].tolist() == [2, 2]

    def test_with_bounds_max_only(self, agg_df):
        agg_step = CountWithinBounds({"max": 3})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert out["value1_within_bounds_none_3"].tolist() == [2, 0]
        assert out["value2_within_bounds_none_3"].tolist() == [1, 1]

    def test_with_bounds_both_min_max(self, agg_df):
        agg_step = CountWithinBounds({"max": 12, "min":1})
        all_agg = CellAggregationStep([agg_step], 1, ['value1', 'value2'])

        out = all_agg.run(agg_df)

        assert out["value1_within_bounds_1_12"].tolist() == [2, 2]
        assert out["value2_within_bounds_1_12"].tolist() == [0, 0]
