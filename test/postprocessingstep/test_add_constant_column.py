import pandas
import pytest
from pandas import DataFrame

from loader.postprocessing_step import AddConstantColumn


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


class TestAddConstantColumn:

    def test_constant_column_added(self, cuba_ger_df):
        col_name = "my_col"
        conf = {
            "column_name": col_name,
            "column_value": "some_str"
        }
        add = AddConstantColumn(conf)
        out = add.run(cuba_ger_df)

        assert col_name in out.columns

    def test_constant_column_str_value(self, cuba_ger_df):
        col_name = "my_col"
        col_val = "some_str"
        conf = {
            "column_name": col_name,
            "column_value": col_val
        }
        add = AddConstantColumn(conf)
        out = add.run(cuba_ger_df)

        assert col_name in out.columns
        assert out[col_name].tolist() == [col_val, col_val]

    def test_constant_column_int_value(self, cuba_ger_df):
        col_name = "my_col"
        col_val = 6000
        conf = {
            "column_name": col_name,
            "column_value": col_val
        }
        add = AddConstantColumn(conf)
        out = add.run(cuba_ger_df)

        assert col_name in out.columns
        assert out[col_name].tolist() == [col_val, col_val]

    def test_constant_column_float_value(self, cuba_ger_df):
        col_name = "my_col"
        col_val = 50.42
        conf = {
            "column_name": col_name,
            "column_value": col_val
        }
        add = AddConstantColumn(conf)
        out = add.run(cuba_ger_df)

        assert col_name in out.columns
        assert out[col_name].tolist() == [col_val, col_val]

    def test_no_other_columns_changed(self, cuba_ger_df):
        col_name = "my_col"
        col_val = "mystr"
        conf = {
            "column_name": col_name,
            "column_value": col_val
        }
        add = AddConstantColumn(conf)
        out = add.run(cuba_ger_df)

        assert col_name in out.columns
        assert out['latitude'].tolist() == [22.2, 53]
        assert out['longitude'].tolist() == [-79.5, 10]
        assert out['value1'].tolist() == [10, 7]
        assert out['value2'].tolist() == [100, 70]
