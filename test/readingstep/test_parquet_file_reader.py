import pytest

from loader.reading_step import ParquetFileReader

test_dir = "./test/test_data/readingstep/"
# 2_value.parquet has columns: latitude, longitude, value1, value2
# 2_value_year_month_day has columns:
#  latitude, longitude, value1, value2, year, month, day

class TestParquetFileReader:
    two_value_conf = {
        "file_path": test_dir + "2_value.parquet",
        "data_columns": ["value1", "value2"]
    }

    two_value_date_mandatory_conf = {
        "file_path": test_dir + "2_val_year_month_day.parquet",
        "data_columns": ["value1", "value2"],
    }

    def test_all_cols_kept(self):

        reader = ParquetFileReader(self.two_value_conf)
        out = reader.read()

        assert set(out.columns) == \
               {"latitude", "longitude", "value1", "value2"}

    def test_rows_as_expected(self):
        reader = ParquetFileReader(self.two_value_conf)
        out = reader.read()
        expectedout = {
            (49, -91, 10, 100),
            (51, -102, 9, 90),
            (51, -102.5, 9, 80),
            (45, -102, 8, 80),
            (43, -118, 7, 70)
        }

        assert len(out) == len(expectedout)
        vals = set(tuple(i) for i in out.values.tolist())
        assert vals == expectedout

    def test_one_col_not_kept(self):
        conf = self.two_value_conf.copy()
        conf["data_columns"] = ["value1"]

        reader = ParquetFileReader(conf)
        out = reader.read()
        assert set(out.columns) == \
               {"latitude", "longitude", "value1"}

    def test_date_cols_dropped_if_not_in_conf(self):
        reader = ParquetFileReader(self.two_value_date_mandatory_conf)
        out = reader.read()

        assert set(out.columns) == \
               {"latitude", "longitude", "value1", "value2"}

    def test_year_kept_if_specified(self):
        conf = self.two_value_date_mandatory_conf.copy()
        conf['year_column'] = 'year'
        reader = ParquetFileReader(conf)
        out = reader.read()

        assert set(out.columns) == \
               {"latitude", "longitude", "value1", "value2",
                "year"}

    def test_year_month_kept_if_specified(self):
        conf = self.two_value_date_mandatory_conf.copy()
        conf['year_column'] = 'year'
        conf['month_column'] = 'month'
        reader = ParquetFileReader(conf)
        out = reader.read()

        assert set(out.columns) == \
               {"latitude", "longitude", "value1", "value2",
                "year", "month"}

    def test_year_month_day_kept_if_specified(self):
        conf = self.two_value_date_mandatory_conf.copy()
        conf['year_column'] = 'year'
        conf['month_column'] = 'month'
        conf['day_column'] = 'day'
        reader = ParquetFileReader(conf)
        out = reader.read()

        assert set(out.columns) == \
               {"latitude", "longitude", "value1", "value2",
                "year", "month", "day"}

    def test_error_if_month_but_not_year(self):
        conf = self.two_value_date_mandatory_conf.copy()
        conf['month_column'] = 'month'

        with pytest.raises(ValueError):
            ParquetFileReader(conf)

    def test_error_if_day_no_month(self):
        conf = self.two_value_date_mandatory_conf.copy()
        conf['year_column'] = 'year'
        conf['day_column'] = 'day'

        with pytest.raises(ValueError):
            ParquetFileReader(conf)

    def test_error_if_day_no_year(self):
        conf = self.two_value_date_mandatory_conf.copy()
        conf['day_column'] = 'day'

        with pytest.raises(ValueError):
            ParquetFileReader(conf)

    def test_error_on_input_file_not_exist(self):
        conf = {
            "file_path": test_dir + "file_does_not_exist.parquet",
            "data_columns": ["value1", "value2"]
        }

        with pytest.raises(ValueError):
            ParquetFileReader(conf)


    def test_error_on_data_col_not_exist(self):
        conf = self.two_value_conf.copy()
        conf["data_columns"] = ["value1", "value2", "not_exist"]

        reader = ParquetFileReader(conf)
        with pytest.raises(ValueError):
            reader.read()

    def test_error_on_lat_not_exist(self):
        conf = {
            "file_path": test_dir + "no_lat.parquet",
            "data_columns": ["value1", "value2"]
        }
        reader = ParquetFileReader(conf)
        with pytest.raises(ValueError):
            reader.read()

    def test_error_on_long_not_exist(self):
        conf = {
            "file_path": test_dir + "no_long.parquet",
            "data_columns": ["value1", "value2"]
        }
        reader = ParquetFileReader(conf)
        with pytest.raises(ValueError):
            reader.read()