# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-04-26 by davis.broda@brodagroupsoftware.com
import logging
import math
import os.path
from typing import List, Set, Tuple, Optional

import h3
from pandas import DataFrame
import folium

from geoserver import dataset_utilities
from geoserver.bgsexception import InvalidArgumentException, \
    BgsAlreadyExistsException

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class Visualizer:

    def __init__(
            self,
            dataset: DataFrame,
            value_col: str,
            rgb: Tuple[int, int, int],
            min_lat: Optional[float],
            max_lat: Optional[float],
            min_long: Optional[float],
            max_long: Optional[float]
    ):
        self.dataset = dataset
        if min_lat is None:
            self.min_lat = dataset['latitude'].min()
        else:
            self.min_lat = min_lat

        if max_lat is None:
            self.max_lat = dataset['latitude'].max()
        else:
            self.max_lat = max_lat

        if min_long is None:
            self.min_long = dataset['longitude'].min()
        else:
            self.min_long = min_long

        if max_long is None:
            self.max_long = dataset['longitude'].max()
        else:
            self.max_long = max_long

        self.value_col = value_col
        self.rgb = rgb

    @staticmethod
    def visualize_h3_cells(cells: List[str], map_path: str) -> str:

        # Create a base map (LAT, LONG)
        starting_location = [43.856, -79.059]
        cell_map = folium.Map(location=starting_location,
                              zoom_start=10)  # Change the default location as needed
        # logger.info(f"cell_map:{cell_map}")

        # Loop through each H3 cell
        for h3_cell in cells:
            # Convert H3 cell to a polygon
            # note that must use geo_json=False to ensure lat/long are
            # in the right order!
            boundary = h3.h3_to_geo_boundary(h3_cell, geo_json=False)
            # logger.info(f"h3_cell:{h3_cell} h3.h3_to_geo(cell):{h3.h3_to_geo(h3_cell)} boundary:{boundary}")

            # Create a folium Polygon and add to the map
            polygon = folium.Polygon(
                locations=boundary,
                weight=0.5,
                # fill=True,
                # fill_color='lightgreen',
                # fill_opacity=0.2
            ).add_to(cell_map)

            center = h3.h3_to_geo(h3_cell)
            tooltip_text = f'H3 Index: {h3_cell}\nLatitude: {center[0]:.6f}, Longitude: {center[1]:.6f}'
            folium.Tooltip(tooltip_text).add_to(polygon)

        cell_map.save(map_path)
        logger.info(f"map_path:{map_path}")

        return map_path

    def visualize_dataset(
            self,
            resolution: int,
            output_file: str,
            threshold: Optional[float],  # between 0 and 1
            ds_type: str = "h3"
    ) -> None:

        if os.path.exists(output_file):
            raise BgsAlreadyExistsException(
                f"output file {output_file} already exists")

        if ds_type == "h3":
            self.draw_h3_ds(resolution, output_file, threshold)
        if ds_type == "point":
            self.draw_point_ds(resolution, output_file, threshold)

    def draw_h3_ds(
            self,
            resolution: int,
            output_file: str,
            threshold: Optional[float],  # between 0 and 1
    ) -> None:
        h3_cells = self._get_h3_in_boundary(resolution)
        num_cells = len(h3_cells)
        logger.info(f"visualizing {num_cells} cells")
        geo_map = self._get_blank_map()

        max_val = self.dataset[self.value_col].max()
        min_value = self.dataset[self.value_col].min()
        std_dev = self.dataset[self.value_col].std()
        avg = self.dataset[self.value_col].mean()
        max_colour_value = avg + 2 * std_dev
        if max_colour_value > max_val:
            max_colour_value = max_val

        scale_width = max_colour_value - min_value

        for index, cell in enumerate(h3_cells):
            if index % 10000 == 0:
                logger.info(f"processing cell {index} of {num_cells}")
            cell_value_list = self.dataset[
                self.dataset['cell'] == cell
                ][self.value_col].values.tolist()
            if cell_value_list is None or len(cell_value_list) == 0:
                continue
            else:
                cell_value = cell_value_list[0]

            # values proportionally lower than minimum
            #  threshold will be discarded
            cell_on_scale = (cell_value - min_value) / scale_width
            if threshold is not None and cell_on_scale < threshold:
                continue

            self._add_cell_to_map(
                cell, cell_value, min_value, max_colour_value, geo_map
            )

        geo_map.save(output_file)
        logger.info(f"created visualization file at map_path:{output_file}")

    def draw_point_ds(
            self,
            resolution: int,
            output_file: str,
            threshold: Optional[float],  # between 0 and 1
            multiple_value_handling: str = 'mean'  # max, mean, min
    ) -> None:
        res_col = "cell"
        ds = self.dataset[[res_col, self.value_col]]
        groups = ds.groupby([res_col])

        working_ds: DataFrame
        # TODO: turn this into a proper enum or something
        if multiple_value_handling == 'mean':
            working_ds = groups.mean()
        elif multiple_value_handling == 'max':
            working_ds = groups.max()
        elif multiple_value_handling == 'min':
            working_ds = groups.min()
        else:
            raise InvalidArgumentException(
                f"multiple_value_handling {multiple_value_handling} was not"
                f" valid. valid options are: [mean, max, min]"
            )
        num_cells = len(working_ds.index)

        geo_map = self._get_blank_map()

        max_val = self.dataset[self.value_col].max()
        min_value = self.dataset[self.value_col].min()
        std_dev = self.dataset[self.value_col].std()
        avg = self.dataset[self.value_col].mean()
        max_colour_value = avg + 2 * std_dev
        if max_colour_value > max_val:
            max_colour_value = max_val

        scale_width = max_colour_value - min_value

        # This relies on the assumption that the dataset is pre-filtered
        #  so only the cells that should be displayed are contained within
        #  the groups at this point

        row_count = 0
        for cell, row in working_ds.iterrows():
            row_count = row_count + 1
            if row_count % 10000 == 0:
                logger.info(f"processing cell {row_count} of {num_cells}")
            val = row[self.value_col]

            # values proportionally lower than minimum
            #  threshold will be discarded
            cell_on_scale = (val - min_value) / scale_width
            if threshold is not None and cell_on_scale < threshold:
                continue

            self._add_cell_to_map(
                cell, val, min_value, max_colour_value, geo_map)

        geo_map.save(output_file)
        logger.info(f"created visualization file at map_path:{output_file}")

    def _add_cell_to_map(
            self,
            cell: str,
            cell_value: float,
            min_val: float,
            max_val: float,
            geo_map: folium.Map
    ):
        boundary = h3.h3_to_geo_boundary(cell, geo_json=False)
        new_rgb = self._adjust_rgb(max_val, min_val, cell_value)

        hex_color = self.rgb_to_hex(new_rgb)
        opacity = self.scale_opacity(
            max_val, min_val, cell_value,
            0.8, 0
        )

        polygon = folium.Polygon(
            locations=boundary,
            weight=0,
            color=f'#{hex_color}',
            opacity=opacity,
            fill=True,
            fill_color=f'#{hex_color}',
            fill_opacity=opacity,
        ).add_to(geo_map)

        center = h3.h3_to_geo(cell)
        tooltip_text = f'value: {cell_value}\n' \
                       f'H3 Index: {cell}\n' \
                       f'Latitude: {center[0]:.6f}, ' \
                       f'Longitude: {center[1]:.6f} ' \
                       f'Color: {hex_color}'
        folium.Tooltip(tooltip_text).add_to(polygon)

    def _get_blank_map(self) -> folium.Map:
        lat_range = self.max_lat - self.min_lat
        long_range = self.max_long - self.min_long
        # +1 at end because most screens are larger than the 256 pixels zoom
        #  level is based around
        zoom_level_lat = -math.log2(lat_range / 180) + 1
        zoom_level_long = -math.log2(long_range / 360) + 1
        zoom = math.ceil(min(zoom_level_lat, zoom_level_long))

        geo_map = folium.Map(
            location=self._get_map_center(),
            min_lon=math.floor(self.min_long),
            min_lat=math.floor(self.min_lat),
            max_lon=math.ceil(self.max_long),
            max_lat=math.ceil(self.max_lat),
            width="100%",
            height="100%",
            max_bounds=True,
            png_enabled=True,
            min_zoom=zoom,
            prefer_canvas=True
        )
        sw = [self.min_lat, self.min_long]
        ne = [self.max_lat, self.max_long]
        geo_map.fit_bounds(bounds=[sw, ne])
        return geo_map

    def _get_h3_in_boundary(self, res: int) -> Set[str]:

        from shapely import Polygon
        from shapely.geometry import shape
        bot_left = (self.min_lat, self.min_long)
        top_left = (self.max_lat, self.min_long)
        top_right = (self.max_lat, self.max_long)
        bot_right = (self.min_lat, self.max_long)
        coords = (bot_left, top_left, top_right, bot_right, bot_left)
        boundary_poly = Polygon(coords)

        geojson = shape(boundary_poly).__geo_interface__
        overlap_cells = h3.polyfill(geojson, res)
        return set(overlap_cells)

    def _get_map_center(self) -> Tuple[float, float]:
        avg_lat = (self.min_lat + self.max_lat) / 2
        avg_long = (self.min_long + self.max_long) / 2
        return avg_lat, avg_long

    def _adjust_rgb(
            self,
            max: float,
            min: float,
            current: float
    ) -> Tuple[int, int, int]:
        if current > max:
            return self.rgb
        elif current < min:
            return 0, 0, 0

        adjustment_factor = (current - min) / (max - min)

        new_r = int(math.floor(self.rgb[0] * adjustment_factor))
        new_g = int(math.floor(self.rgb[1] * adjustment_factor))
        new_b = int(math.floor(self.rgb[2] * adjustment_factor))

        return new_r, new_g, new_b

    def rgb_to_hex(self, rgb: Tuple[int, int, int]) -> str:
        def limit_to_range(v: int) -> int:
            if v < 0:
                return 0
            elif v > 255:
                return 255
            else:
                return v

        return ''.join('{:02X}'.format(limit_to_range(n)) for n in rgb)

    def scale_opacity(
            self,
            max_data: float,
            min_data: float,
            current_data: float,
            max_opacity: float = 1.0,
            min_opacity: float = 0.0
    ) -> float:
        if current_data > max_data:
            return max_opacity
        elif current_data < min_data:
            return min_opacity

        adjustment_factor_data = (current_data - min_data) / (
                    max_data - min_data)

        return min_opacity + (
                    max_opacity - min_opacity) * adjustment_factor_data
