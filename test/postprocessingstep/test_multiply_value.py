import pandas
import pytest
from pandas import DataFrame

from loader.postprocessing_step import MultiplyValue


@pytest.fixture()
def cuba_ger_df() -> DataFrame:
    data = [
        [22.2, -79.5, 10, 100],  # cuba
        [53, 10, 7, 70]  # germany
    ]  # USA
    df = pandas.DataFrame(
        data,
        columns=['latitude', 'longitude', 'value1', 'value2']
    )
    return df


class TestMultiplyValue:

    def test_multiply_effects_data_cols(self, cuba_ger_df):
        conf = {
            "multiply_by": 5.5
        }
        mb = MultiplyValue(conf)
        out_df = mb.run(cuba_ger_df)

        assert out_df['value1'].tolist() == [10 * 5.5, 7 * 5.5]
        assert out_df['value2'].tolist() == [100 * 5.5, 70 * 5.5]

    def test_multiply_does_not_effect_lat_long(self, cuba_ger_df):
        conf = {
            "multiply_by": 5.5
        }
        mb = MultiplyValue(conf)
        out_df = mb.run(cuba_ger_df)

        assert out_df['latitude'].tolist() == [22.2, 53]
        assert out_df['longitude'].tolist() == [-79.5, 10]

    def test_fail_if_no_multiply_by_conf_value(self):
        conf = {}
        with pytest.raises(Exception):
            MultiplyValue(conf)
