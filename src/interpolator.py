# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
import os
import time
from typing import List, Tuple, Dict, Any, Optional

import duckdb
import h3
import numpy as np
import pandas
from pandas import DataFrame
from scipy.spatial import cKDTree

import executor
import geomesh
import shape

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

VALID_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP",
                "OCT", "NOV", "DEC"]
INT_TO_MONTH = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC"
}


class Interpolator:

    def __init__(
            self,
            geo_out_db_dir: str
    ):
        """
        Initialize class

        :param geo_out_db_dir:
            The directory where databases containing processed data
            will be created
        :type geo_out_db_dir: str
        """

        self.geo_out_db_dir = geo_out_db_dir

        if not os.path.exists(self.geo_out_db_dir):
            os.makedirs(self.geo_out_db_dir)

    def interpolate_df(
            self,
            input_data: DataFrame,
            cols_to_interpolate: List[str],
            time_cols: List[str],
            resolution: int,
            num_neighbors: int,
            power: int,
            shapefile: Optional[str] = None,
            region: Optional[str] = None,
            max_parallelism: int = 4
    ) -> DataFrame:
        # TODO: will need to split up the DF before interpolating
        #  so that everything not being interpolated can be
        #  held constant, without change

        dfs_out = []
        if len(time_cols) > 0:
            # IDE says type is wrong below. ignore it. type is correct
            # noinspection PyTypeChecker
            time_groups: Dict[Tuple,DataFrame] = dict(list(
                input_data.groupby(time_cols, sort=False)
            ))
            for time_elements, time_group in time_groups.items():
                out_section = self._interpolate_sub_df(
                    time_group,
                    cols_to_interpolate,
                    resolution,
                    num_neighbors,
                    power,
                    max_processes=max_parallelism,
                    shapefile=shapefile,
                    region=region
                )
                for time_col, time_value in zip(time_cols, list(time_elements)):
                    out_section[time_col] = time_value
                dfs_out.append(out_section)
        else:
            dfs_out.append(self._interpolate_sub_df(
                input_data,
                cols_to_interpolate,
                resolution,
                num_neighbors,
                power,
                max_processes=max_parallelism,
                shapefile=shapefile,
                region=region
            ))

        return pandas.concat(dfs_out)

    def _interpolate_sub_df(
            self,
            df: DataFrame,
            cols_to_interpolate: List[str],
            resolution: int,
            num_neighbors: int,
            power: int,
            use_parallel: bool = True,
            max_processes: int = 4,
            shapefile: Optional[str] = None,
            region: Optional[str] = None
    ) -> DataFrame:
        latitudes = df['latitude'].tolist()
        longitudes = df['longitude'].tolist()
        data_lists: Dict[str, List] = {}
        for data_col in cols_to_interpolate:
            data_list = df[data_col].tolist()
            data_lists[data_col] = data_list

        # tree data structure allows easy search for geographically
        #  nearby data.
        tree = cKDTree(list(zip(latitudes, longitudes)))


        if shapefile is not None:
            buffer = geomesh.Geomesh.get_buffer(resolution)
            shp = shape.Shape(shapefile)
            cells = list(shp.get_h3_in_shape(
                buffer,
                resolution,
                reverse_coords=True,
                region=region,
                min_latitude=geomesh.MIN_LAT,
                max_latitude=geomesh.MAX_LAT,
                min_longitude=geomesh.MIN_LONG,
                max_longitude=geomesh.MAX_LONG
            ))
        else:
            cells = self._get_all_cells_for_res(resolution)

        logger.info(f"Calculating number of cells:{len(cells)}")

        if use_parallel:
            chunk_size = 2000
            items = self._execute_interpolation_parallel(
                max_processes,
                chunk_size,
                data_lists,
                tree,
                cells,
                num_neighbors,
                power)
        else:
            items = self._execute_interpolation_singlethread(
                data_lists,
                tree,
                cells,
                num_neighbors,
                power)

        return pandas.DataFrame.from_dict(items)

    def _execute_interpolation_parallel(
            self,
            max_processes: int,
            chunk_size: int,
            data_points: Dict[str, List[float]],
            tree: cKDTree,
            cells: List[str],
            num_neighbors: int,
            power: int):

        segments = [cells[i:i + chunk_size] for i in
                    range(0, len(cells), chunk_size)]
        segments_len = [len(segment) for segment in segments]
        logger.info(
            f"cells:{len(cells)} segments:{len(segments)} lengths:{segments_len}")

        # Create entries for parallel execution
        entries = []
        for index, segment_cells in enumerate(segments):
            logger.info(f"segment_cells:{len(segment_cells)}")
            entry = {
                "count": len(segments_len),
                "index": index,
                "data_cols": data_points,
                "tree": tree,
                "segment_cells": segment_cells,
                "num_neighbors": num_neighbors,
                "power": power,
            }
            entries.append(entry)

        interpolator = executor.Executor(self._interpolate_segment_kwargs,
                                         max_processes)
        items = interpolator.process_data(entries)

        return items

    def _execute_interpolation_singlethread(
            self,
            data_points: Dict[str, List[float]],
            tree: cKDTree,
            cells: List[str],
            num_neighbors: int,
            power: int) -> List[Dict[str, Any]]:

        chunk_size = 10000
        segments = [cells[i:i + chunk_size] for i in
                    range(0, len(cells), chunk_size)]
        segments_len = [len(segment) for segment in segments]
        logger.info(
            f"cells:{len(cells)} segments:{len(segments)} lengths:{segments_len}")

        items = []
        for segment_cells in segments:
            logger.info(f"segment_cells:{len(segment_cells)}")

            xitems = self._interpolate_segment(
                data_points,
                tree,
                segment_cells,
                num_neighbors,
                power)
            items.extend(xitems)
        return items

    def _interpolate_single_point(
            self,
            lat: float,
            lon: float,
            data_points_sets: List[List[float]],
            tree: cKDTree,
            num_neighbors: int,
            power: int = 2
    ) -> List[float | None]:
        """
        Interpolate temperature at a given latitude and longitude using IDW.

        :param lat: Latitude of the point for interpolation.
        :param lon: Longitude of the point for interpolation.
        :param data_points_sets:
            Lists of data values at known points.
            each entry within th top-level list will generate one
            interpolated value in the output
        :param num_neighbors: Number of nearest neighbors to consider for interpolation.
        :param power: Power parameter for IDW.
        :return: Interpolated values, or Nones if value could not be calculated
        """

        # logger.info(f"Interpolating lat:{lat} lon:{lon} temperatures:{len(temperatures)} num_neighbors:{num_neighbors}")

        # Query the KDTree for nearest neighbors which return indices
        # into temperature and distances between the point and the nearest
        # neighbour (the cKDTree contains indices into latitude and longitude
        # if you need to find the actual neightbour lat/lon)
        # logger.info(f"\n\nFinding nearest neighbours for lat:{lat} lon:{lon}")
        distances, indices = tree.query((lat, lon), k=num_neighbors)
        # logger.info(f"distances:{distances} indices:{indices}")

        # Handle the case of single nearest neighbor
        if np.isscalar(distances):
            distances, indices = [distances], [indices]

        # Compute weights based on inverse distance
        weights = 1 / np.power(distances, power)

        interpolated_temps = []
        for data_list in data_points_sets:
            # Calculate weighted sum of temperatures
            weighted_temperatures = np.sum(
                weights * np.array(data_list)[indices])
            weighted_sum = np.sum(weights)

            # Calculate the normalized temperature.
            # Note sometimes neighbours may not have available temperatures
            # when there are no nearby weather stations). In this case return -1.

            if weighted_sum == 0 or weighted_temperatures == 0:
                interpolated_temps.append(None)
                logger.debug(
                    f"Invalid IDW calculation weighted_temperatures:{weighted_temperatures} weighted_sum{weighted_sum}")
            else:
                interpolated_temps.append(weighted_temperatures / weighted_sum)

        return interpolated_temps

    def _interpolate_segment(
            self,
            data_cols: Dict[str, List[float]],
            tree: cKDTree,
            cells,
            num_neighbors: int,
            power: int) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for cell in cells:
            cell_lat, cell_long = h3.h3_to_geo(cell)

            interpolated_values = []
            # Check progressively more neighbors until valid temp returned
            for i in range(1, 5):
                num_neighbors_current = num_neighbors * i
                interpolated_values = self._interpolate_single_point(
                    cell_lat,
                    cell_long,
                    list(data_cols.values()),
                    tree,
                    num_neighbors_current,
                    power)
                missing_vals = None in interpolated_values
                if not missing_vals:
                    break

            if None in interpolated_values:
                logger.info(
                    f"Invalid temperature cell: {cell}"
                    f" num_neighbors:v {num_neighbors * 4}")

            item: Dict[str, Any] = {
                "cell": cell,
                "latitude": cell_lat,
                "longitude": cell_long
            }
            zipped = zip(list(data_cols.keys()), interpolated_values)
            for col_name, col_value in zipped:
                item[col_name] = col_value
            items.append(item)
        return items

    def _interpolate_segment_kwargs(self, **kwargs):
        index = kwargs.get('index', None)
        logger.info(f"index:{index}")
        count = kwargs.get('count', None)
        logger.info(f"count: {count}")
        pct = round(index / count, 4)
        logger.info(f"Started execution index:{index} of {count} pct:{pct}")

        data_cols = kwargs.get('data_cols', None)
        tree = kwargs.get('tree', None)
        segment_cells = kwargs.get('segment_cells', None)
        num_neighbors = kwargs.get('num_neighbors', None)
        power = kwargs.get('power', None)

        items = self._interpolate_segment(
            data_cols,
            tree,
            segment_cells,
            num_neighbors,
            power)

        logger.info(f"Finished execution index:{index} of {count} pct:{pct}")
        return items

    def _get_all_cells_for_res(
            self,
            resolution: int
    ) -> List[str]:
        # Get all base cells at resolution 0
        base_cells = h3.get_res0_indexes()
        all_cells = set()
        for base_cell in base_cells:
            children = h3.h3_to_children(base_cell, resolution)
            all_cells.update(children)
        return list(all_cells)