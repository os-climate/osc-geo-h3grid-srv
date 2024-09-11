# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-09-11 by 15205060+DavisBroda@users.noreply.github.com
from geoserver.correlator import Correlator

from geoserver.geomesh_router_arguments import LocatedAsset, DatasetArg, \
    AssetFilter

test_dir = "./test/test_data/correlator"


class TestCorrelator:

    def test_output_contains_all_possible_cell_resolutions(self):
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="Berlin",
                latitude=52.52,
                longitude=13.40
            ),
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="greater_than",
                        target_value=-0.1  # should return every point
                    )
                ]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)
        all_cols = out["columns"]
        for i in range(16):
            assert f"cell_{i}" in all_cols

    def test_include_if_no_match_in_dataset(self):
        # If you pass a point outside the geographic region of a dataset
        # still include it, and let the user handle filtering that on
        # their end if they need it removed.
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="NOT_IN_GERMANY",
                latitude=0,
                longitude=0
            )
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="greater_than",
                        target_value=-0.1  # should return every point
                    )
                ]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)
        assert len(out["data"]) == 1

    def test_no_match_in_dataset_has_null_cols(self):
        # If you pass a point outside the geographic region of a dataset
        # still include it, and let the user handle filtering that on
        # their end if they need it removed.
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="NOT_IN_GERMANY",
                latitude=0,
                longitude=0
            )
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="greater_than",
                        target_value=-0.1  # should return every point
                    )
                ]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)
        out_row = out["data"][0]
        null_count = 0
        for col in out_row:
            if col is None:
                null_count += 1
        assert null_count > 1

    def test_filter_works_with_one_filter(self):
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="Berlin",
                latitude=52.52,
                longitude=13.40
                # flood_risk_min    = 0.177
                # flood_risk_max    = 7.451
                # flood_risk_median = 7.131
                # flood_risk_mean   = 5.416
            ),
            LocatedAsset(
                id="Frankfurt",
                latitude=50.11,
                longitude=8.68
                # flood_risk_min    = 0.0261
                # flood_risk_max    = 6.888
                # flood_risk_median = 5.338
                # flood_risk_mean   = 4.228
            ),
            LocatedAsset(
                id="Brunsbüttel",
                latitude=53.89,
                longitude=9.13
                # flood_risk_min    = 0.170
                # flood_risk_max    = 3.313
                # flood_risk_median = 1.536
                # flood_risk_mean   = 1.471
            )
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="greater_than",
                        target_value=4  # should filter out Brunsbüttel
                    )
                ]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)
        assert len(out["data"]) == 2
        assert "Berlin" in str(out)
        assert "Frankfurt" in str(out)
        assert "Brunsbüttel" not in str(out)

    def test_multiple_filters_one_dataset(self):
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="Berlin",
                latitude=52.52,
                longitude=13.40
                # flood_risk_min    = 0.177
                # flood_risk_max    = 7.451
                # flood_risk_median = 7.131
                # flood_risk_mean   = 5.416
            ),
            LocatedAsset(
                id="Frankfurt",
                latitude=50.11,
                longitude=8.68
                # flood_risk_min    = 0.0261
                # flood_risk_max    = 6.888
                # flood_risk_median = 5.338
                # flood_risk_mean   = 4.228
            ),
            LocatedAsset(
                id="Brunsbüttel",
                latitude=53.89,
                longitude=9.13
                # flood_risk_min    = 0.170
                # flood_risk_max    = 3.313
                # flood_risk_median = 1.536
                # flood_risk_mean   = 1.471
            )
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="lesser_than",
                        target_value=7  # should filter out Berlin
                    ),
                    AssetFilter(
                        column="flood_risk_min",
                        filter_type="greater_than",
                        target_value=0.1  # should filter out Brunsbüttel
                    )
                ]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)
        assert len(out["data"]) == 1
        assert "Berlin" not in str(out)
        assert "Frankfurt" not in str(out)
        assert "Brunsbüttel" in str(out)

    def test_multiple_datasets_no_filters(self):
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="Berlin",
                latitude=52.52,
                longitude=13.40
                # flood_risk_min    = 0.177
                # flood_risk_max    = 7.451
                # flood_risk_median = 7.131
                # flood_risk_mean   = 5.416
            ),
            LocatedAsset(
                id="Frankfurt",
                latitude=50.11,
                longitude=8.68
                # flood_risk_min    = 0.0261
                # flood_risk_max    = 6.888
                # flood_risk_median = 5.338
                # flood_risk_mean   = 4.228
            ),
            LocatedAsset(
                id="Brunsbüttel",
                latitude=53.89,
                longitude=9.13
                # flood_risk_min    = 0.170
                # flood_risk_max    = 3.313
                # flood_risk_median = 1.536
                # flood_risk_mean   = 1.471
            )
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[]
            ),
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_1000y_germany",
                filters=[]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)
        # expected col number is 16 'cell_X' cols,
        # id, lat, long
        # 2x h3cell,
        # 2x (max, min, median, mode),
        # 2x (lat, long, scenario, risk window, date range)
        # = 16 + 3 + 2 + 2*4 + 2*5 = 39

        assert len(out['columns']) == 39
        assert len(out["data"]) == 3  # no filters applied so all returned

    def test_multiple_datasets_one_filter_each(self):
        correlator = Correlator(test_dir)

        assets = [
            LocatedAsset(
                id="Berlin",
                latitude=52.52,
                longitude=13.40
                # flood_risk_min 10y    = 0.177
                # flood_risk_max 10y    = 7.451
                # flood_risk_min 1000y  = 0.024
                # flood_risk_max 1000y  = 9.061
            ),
            LocatedAsset(
                id="Frankfurt",
                latitude=50.11,
                longitude=8.68
                # flood_risk_min 10y    = 0.0261
                # flood_risk_max 10y    = 6.888
                # flood_risk_min 1000y  = 0.0075
                # flood_risk_max 1000y  = 8.233
            ),
            LocatedAsset(
                id="Brunsbüttel",
                latitude=53.89,
                longitude=9.13
                # flood_risk_min 10y    = 0.170
                # flood_risk_max 10y    = 3.313
                # flood_risk_min 1000y  = 0.138
                # flood_risk_max 1000y  = 3.767

            ),
            LocatedAsset(
                id="Bremen",
                latitude=53.08,
                longitude=8.803
                # flood_risk_min 10y    = 0.713
                # flood_risk_max 10y    = 7.190
                # flood_risk_min 1000y  = 0.036
                # flood_risk_max 1000y  = 8.280
            ),
            LocatedAsset(
                id="Cologne",
                latitude=50.95,
                longitude=6.95
                # flood_risk_min 10y    = None
                # flood_risk_max 10y    = None
                # flood_risk_min 1000y  = 0.0156
                # flood_risk_max 1000y  = 5.806
            )
        ]

        datasets = [
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="lesser_than",
                        target_value=7.3  # should filter out Berlin
                    ),
                    AssetFilter(
                        column="flood_risk_min",
                        filter_type="greater_than",
                        target_value=0.1  # should filter out Frankfurt
                    )
                ]
            ),
            DatasetArg(
                name="tu_delft_river_flood_depth_1971_2000_hist_1000y_germany",
                filters=[
                    AssetFilter(
                        column="flood_risk_max",
                        filter_type="greater_than",
                        target_value=4  # should filter out Brunsbüttel
                    ),
                    AssetFilter(
                        column="flood_risk_min",
                        filter_type="greater_than",
                        target_value=0.03  # should filter out Cologne
                    )
                ]
            )
        ]

        out = correlator.get_correlated_data(assets=assets, datasets=datasets)

        assert "Bremen" in str(out)
        assert "Brunsbüttel" not in str(out)
        assert "Cologne" not in str(out)
        assert "Frankfurt" not in str(out)
        assert "Berlin" not in str(out)
