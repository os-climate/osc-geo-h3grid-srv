import pandas
import pytest
from pandas import DataFrame

from loader.preprocessing_step import ShapefileFilter

test_dir = "./test/test_data/preprocessingstep/"


@pytest.fixture()
def cuba_ger_df() -> DataFrame:
    data = [
        [22.2, -79.5, 10, 100],  # cuba
        [20.4, -76.3, 9, 90],  # cuba
        [22.5, -83.8, 9, 80],  # cuba
        [50, 9.7, 8, 80],  # germany
        [53, 10, 7, 70]  # germany
    ]  # USA
    df = pandas.DataFrame(
        data,
        columns=['latitude', 'longitude', 'value1', 'value2']
    )
    return df


class TestShapefileFilter:

    def test_filters_by_whole_shapefile(self, cuba_ger_df):
        conf = {
            "shapefile_path": test_dir + "Germany_Box/Germany_Box.shp"
        }

        sff = ShapefileFilter(conf)
        out = sff.run(cuba_ger_df)
        expected = {
            (50, 9.7, 8, 80),
            (53, 10, 7, 70)
        }

        assert len(out) == 2
        actual = set(tuple(i) for i in out.values.tolist())
        assert actual == expected

    def test_filter_by_region_in_shapefile(self, cuba_ger_df):
        conf = {
            "shapefile_path": test_dir +
                              "Germany_Cuba_Box/Germany_Cuba_Box.shp",
            "region": "Cuba"
        }

        sff = ShapefileFilter(conf)
        out = sff.run(cuba_ger_df)
        expected = {
            (22.2, -79.5, 10, 100),
            (20.4, -76.3, 9, 90),
            (22.5, -83.8, 9, 80),
        }

        assert len(out) == 3
        actual = set(tuple(i) for i in out.values.tolist())
        assert actual == expected

    def test_fail_if_shapefile_does_not_exist(self):
        conf = {
            "shapefile_path": test_dir +
                              "Germany_Cuba_Box/file_does_not_exist.shp",
            "region": "Cuba"
        }

        with pytest.raises(ValueError):
            ShapefileFilter(conf)

    def test_fail_if_region_does_not_exist(self):
        conf = {
            "shapefile_path": test_dir +
                              "Germany_Cuba_Box/file_does_not_exist.shp",
            "region": "NotRealCountry"
        }

        with pytest.raises(ValueError):
            ShapefileFilter(conf)
