# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-09-11 by 15205060+DavisBroda@users.noreply.github.com
from cli import cli_geospatial

HOST = "localhost"
PORT = '8000'


# requires a server be started with the following command
# ./bin/start.sh --configuration ./config/config-example.yml
class TestGeomeshCliIntegration:

    def test_filter_multiple_datasets(self):
        assets_file = \
            "./examples/geospatial/filter-assets/germany_5_assets.parquet"
        datasets_file = \
            "./examples/geospatial/filter-assets/germany_datasets.json"

        out = cli_geospatial.execute([
            "--host", HOST,
            "--port", PORT,
            "filter-assets",
            "--asset-file", assets_file,
            "--dataset-file", datasets_file
        ])
        assert "Bremen" in out
        assert "Brunsbüttel" not in out
        assert "Cologne" not in out
        assert "Frankfurt" not in out
        assert "Berlin" not in out

    def test_filter_one_dataset(self):
        assets_file = \
            "./examples/geospatial/filter-assets/germany_5_assets.parquet"
        datasets_file = \
            "./examples/geospatial/filter-assets/germany_one_dataset.json"

        out = cli_geospatial.execute([
            "--host", HOST,
            "--port", PORT,
            "filter-assets",
            "--asset-file", assets_file,
            "--dataset-file", datasets_file,
            "--return-rows", "-1"
        ])
        assert "Bremen" in out
        assert "Brunsbüttel" in out
        assert "Cologne" not in out
        assert "Frankfurt" not in out
        assert "Berlin" not in out

    def test_metadata_endpoint(self):
        out = cli_geospatial.execute([
            "--host", HOST,
            "--port", PORT,
            "showmeta"
        ])

        assert "jamaica_buildings" in out
        assert "tu_delft_river_flood_depth_1971_2000_hist_0010y_europe" in out
        assert "tu_delft_river_flood_depth_1971_2000_hist_0010y_germany" in out
        assert "tu_delft_river_flood_depth_1971_2000_hist_1000y_germany" in out
