# Copyright 2023 Broda Group Software Inc.
#
# Created: 2023-10-16 by eric.broda@brodagroupsoftware.com

import logging
import math
import os
from math import sqrt
from typing import Tuple, List, Any, Dict, Optional, Set

import duckdb
import h3
from pydantic import BaseModel, Field
from shapely.geometry import Polygon

import re

import geoserver.metadata as metadata
from geoserver import dataset_utilities
from geoserver.bgsexception import DBDirNotExistsException,\
    DataSetNotRegisteredException, \
    IntervalInvalidException, InvalidArgumentException, \
    OperationUnsupportedException
from geoserver.visualizer import Visualizer
from geoserver.metadata import MetadataDB
from geoserver.shape import Shape

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

NUM_NEIGHBOURS = 3
KM_PER_DEGREE = 110  # Each degree is about 110 KM

MIN_LAT, MAX_LAT = -60.0, 85.0  # Excluding Antarctica
MIN_LONG, MAX_LONG = -180.0, 180.0  # Full range of longitudes


class CellDataRow(BaseModel):
    cell: str = Field(description="The cell that this data row represents")

    latitude: float = Field(
        description="The latitude of the center of the cell"
                    " this data row represents")

    longitude: float = Field(
        description="The longitude of the center of the cell"
                    " this data row represents")

    values: Dict[str, Any] = Field(
        description="Any data values associated with this row's cell."
                    " Keys represent the column the data element comes from,"
                    " with the value being what was present in that column.")


class PointDataRow(BaseModel):
    latitude: float = Field(
        description="The latitude of the point this data row represents")

    longitude: float = Field(
        description="The longitude of the point this data row represents")

    values: Dict[str, Any] = Field(
        description="Any data values associated with this row's cell."
                    " Keys represent the column the data element comes from,"
                    " with the value being what was present in that column.")

    cells: Dict[str, str] = Field(
        description="The cells this point is contained within,"
                    " at various resolutions")


class Geomesh:
    # Total number of cells at a given resolution
    CELLS_AT_RESOLUTION = [
        122,                # 0
        842,                # 1
        5882,               # 2
        41162,              # 3
        288122,             # 4
        2016842,            # 5
        14117882,           # 6
        98825162,           # 7
        691776122,          # 8
        4842432842,         # 9
        33897029882,        # 10
        237279209162,       # 11
        1660954464122,      # 12
        11626681248842,     # 13
        81386768741882,     # 14
        569707381193162     # 15
    ]

    # Average cell area in km2 at a given resolution
    CELLS_KM2_AT_RESOLUTION = [
        4357449.42,     # 0  (radius: 1,387,019,227,683m, or 1,387,019km)
        609788.44,      # 1  (radius:   194,101,689,503m)
        86801.78,       # 2  (radius:    27,629,864,839m)
        12393.43,       # 3  (radius:     3,944,952,774m)
        1770.35,        # 4  (radius:       563,519,160m)
        252.90,         # 5  (radius:        80,501,798m)
        36.129,         # 6  (radius:        11,500,237m)
        5.161293360,    # 7  (radius:         1,642,890m)
        0.737327598,    # 8  (radius:           234,698m)
        0.105332513,    # 9  (radius:            33,528m)
        0.015047502,    # 10 (radius:             4,789m)
        0.002149643,    # 11 (radius:               684m)
        0.000307092,    # 12 (radius:                97m)
        0.000043870,    # 13 (radius:                13m)
        0.000006267,    # 14 (radius:                 2m)
        0.000000895     # 15 (radius:               0.3m)
    ]

    def __init__(
            self,
            geo_out_db_dir: str | None
    ):
        """
        Initialize class

        :param geo_out_db_dir:
            The directory where databases containing processed data
            will be created
        :type geo_out_db_dir: str
        """

        self.geo_out_db_dir = geo_out_db_dir

        # some commands don't need database, so allow None in that case
        if geo_out_db_dir is not None:
            if not os.path.exists(self.geo_out_db_dir):
                logger.info(
                    f"database directory {self.geo_out_db_dir} did not"
                    f"exist. Creating this directory now.")
                os.makedirs(self.geo_out_db_dir)
            self.metadb = MetadataDB(self.geo_out_db_dir)



    def shapefile_get(
            self,
            dataset_name: str,
            shapefile: str,
            region: Optional[str],
            resolution: int,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[CellDataRow]:
        """
        Retrieve data within the region(s) defined by a shapefile.

        :param dataset_name: The name of the dataset to retrieve for
        :type dataset_name: str
        :param shapefile:
            The path to a local .shp shapefile containing the polygons
            for which data is to be retrieved
        :type shapefile: str
        :param region:
            The name of a region within the shapefile to get data for.
            If None, all polygons within the shapefile will be included
            in returned data.
        :type region: Optional[str]
        :param resolution: The h3 resolution level to get data for
        :type resolution: int
        :param year: The year to retrieve data for.
        :type year: Optional[int]
        :param month: The month to retrieve data for
        :type month: Optional[int]
        :param day: The day to retrieve data for
        :type day: Optional[int]
        :return: The data in the specified area
        :rtype: List[Dict[str, Any]]
        """

        if not self.metadb.ds_meta_exists(dataset_name):
            raise DataSetNotRegisteredException(
                f"dataset {dataset_name} not registered"
                f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        ds_type = meta["dataset_type"]

        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type, resolution
        )

        buffer = Geomesh.get_buffer(resolution)

        shape = Shape(shapefile)

        cells = list(shape.get_h3_in_shape(
            buffer,
            resolution,
            True,
            region,
            MIN_LONG,
            MAX_LONG,
            MIN_LAT,
            MAX_LAT
        ))

        if not os.path.exists(self.geo_out_db_dir):
            raise DBDirNotExistsException(
                "db dir: {self.geo_out_db_dir} does not exist")

        col_names: List[str] = meta["value_columns"]["key"]
        value_columns = ", ".join(col_names)

        out_db_path = self._get_db_path(dataset_name)
        connection = duckdb.connect(database=out_db_path)

        time_filter, time_params = self._get_time_filters(
            meta["interval"], year, month, day)

        cells_per_part = 20000
        if len(cells) > cells_per_part:
            num_parts = math.ceil(float(len(cells)) / float(20000))

            cells_split = []
            for index in range(num_parts):
                cells_split.append(
                    cells[
                        index * cells_per_part:
                        cells_per_part * (index + 1)
                    ])
        else:
            cells_split = [list(cells)]

        data = []

        if ds_type == "h3":
            cell_column = "cell"
        elif ds_type == "point":
            cell_column = f"cell{resolution}"
        else:
            raise OperationUnsupportedException(
                "only h3 and point dataset types are supported"
                " for retrieving values within radius."
                f" Provided type was: {ds_type}"
            )


        for cell_part in cells_split:
            part_str = ""
            for cell in cell_part:
                part_str = part_str + f"'{cell}',"
            part_str = part_str[:-1]
            in_clause = f"""
                   {cell_column} IN ({part_str})
                """

            full_where = self._combine_where_clauses([time_filter, in_clause])

            sql = f"""
                SELECT {cell_column}, latitude, longitude, {value_columns}
                FROM {table_name}
                {full_where} 
            """

            res = connection.execute(sql, time_params).fetchall()

            for res_row in res:
                data.append(res_row)

        # format output as a json object
        out = self._row_to_cell_out(data, col_names)
        return out

    def shapefile_get_point(
            self,
            dataset_name: str,
            shapefile: str,
            region: Optional[str],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[PointDataRow]:
        """
        Retrieve data within the region(s) defined by a shapefile.

        :param dataset_name: The name of the dataset to retrieve for
        :type dataset_name: str
        :param shapefile:
            The path to a local .shp shapefile containing the polygons
            for which data is to be retrieved
        :type shapefile: str
        :param region:
            The name of a region within the shapefile to get data for.
            If None, all polygons within the shapefile will be included
            in returned data.
        :type region: Optional[str]
        :param year: The year to retrieve data for.
        :type year: Optional[int]
        :param month: The month to retrieve data for
        :type month: Optional[int]
        :param day: The day to retrieve data for
        :type day: Optional[int]
        :return: The data in the specified area
        :rtype: List[Dict[str, Any]]
        """

        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        val_col_names: List[str] = meta["value_columns"]["key"]
        ds_type = meta["dataset_type"]

        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type
        )

        # TODO: figure out how to determine buffer resolution here
        #  as for the moment I just set a default and moved on
        buffer = Geomesh.get_buffer(3)

        shape = Shape(shapefile)

        if not os.path.exists(self.geo_out_db_dir):
            raise DBDirNotExistsException(
                "db dir: {self.geo_out_db_dir} does not exist")

        if not self.metadb.ds_meta_exists(dataset_name):
            raise DataSetNotRegisteredException(
                f"dataset {dataset_name} not registered in metadata.")

        out_db_path = self._get_db_path(dataset_name)
        connection = duckdb.connect(database=out_db_path)

        time_filter, time_params = self._get_time_filters(
            meta["interval"], year, month, day)

        # get maximum lat/long for a given shapefile region to get the database
        #  to filter out as many datapoints as possible before we have to
        #  perform the more expensive checks on exact inclusion
        (min_long, min_lat, max_long, max_lat) = shape.get_max_lat_long(region)

        lat_long_filter = f"""
            latitude BETWEEN {min_lat} AND {max_lat}
            AND 
            longitude BETWEEN {min_long} AND {max_long} 
        """

        full_where = self._combine_where_clauses([time_filter, lat_long_filter])
        value_columns = ", ".join(val_col_names)


        col_list = connection.execute(f"describe {table_name}").fetchall()
        all_col_names = list(map(
            lambda c: c[0],
            col_list
        ))
        cell_col_names = list(filter(
            lambda cn: re.match("res[0-9]*", cn) is not None,
            all_col_names
        ))
        cell_column = ", ".join(cell_col_names)

        sql = f"""
            SELECT {cell_column}, latitude, longitude, {value_columns}
            FROM {table_name}
            {full_where} 
        """

        raw_result: List[Tuple] = connection \
            .execute(sql, time_params).fetchall()

        point_rows = self._row_to_point_out(
            raw_result, val_col_names, cell_col_names)

        filter(
            lambda row: shape.point_within_shape(
                row.latitude, row.longitude, region
            ),
            point_rows
        )

        return point_rows


    def cell_get_radius_h3(
            self,
            dataset_name: str,
            cell: str,
            radius: float,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[CellDataRow]:
        """
        Retrieve GISS geo data within a specified radius of a specific
        h3 cell, specified by cell ID

        :param dataset_name: The name of the dataset to retrieve data for
        :type dataset_name: str
        :param cell: The ID of the central cell
        :type cell: str
        :param radius:
            The radius to retrieve data for.
            If radius is -1 or >40075 (circumference of earth), all values
            will be retrieved
        :type radius: float
        :param year: The year to retrieve data for.
        :type year: int
        :param month: The month to retrieve data for
        :type month: str
        :param day: The day to retrieve data for
        :type day: Optional[int]
        :return: The data within specified radius
        :rtype: List[Dict[str, Any]]
        """
        lat, long = self._to_latlon(cell)
        return self.lat_long_get_radius_h3(
            dataset_name,
            lat,
            long,
            radius,
            h3.h3_get_resolution(cell),
            year,
            month,
            day
        )

    def cell_get_radius_point(
            self,
            dataset_name: str,
            cell: str,
            radius: float,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[PointDataRow]:
        lat, long = self._to_latlon(cell)
        return self.lat_long_get_radius_point(
            dataset_name,
            lat,
            long,
            radius,
            year,
            month,
            day
        )

    def lat_long_get_radius_point(
            self,
            dataset_name: str,
            latitude: float,
            longitude: float,
            radius: float,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[PointDataRow]:
        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        val_col_names: List[str] = meta["value_columns"]["key"]
        ds_type = meta["dataset_type"]

        if ds_type != "point":
            raise InvalidArgumentException(
                f"dataset {dataset_name} is not a point dataset. Instead it"
                f"was {ds_type}"
            )

        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type, None
        )

        out_db_path = self._get_db_path(dataset_name)
        connection = duckdb.connect(database=out_db_path)

        col_list = connection.execute(f"describe {table_name}").fetchall()
        all_col_names = list(map(
            lambda c: c[0],
            col_list
        ))
        cell_col_names = list(filter(
            lambda cn: dataset_utilities.col_name_is_point_res_col(cn),
            all_col_names
        ))

        raw: List[Tuple] = self._lat_long_get_radius(
            dataset_name,
            latitude,
            longitude,
            radius,
            None,
            year,
            month,
            day
        )

        out = self._row_to_point_out(
            rows=raw,
            val_col_names=val_col_names,
            cell_col_names=cell_col_names
        )
        return out

    def lat_long_get_radius_h3(
            self,
            dataset_name: str,
            latitude: float,
            longitude: float,
            radius: float,
            resolution: Optional[int],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[CellDataRow]:
        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        val_col_names: List[str] = meta["value_columns"]["key"]
        ds_type = meta["dataset_type"]

        if ds_type != "h3":
            raise InvalidArgumentException(
                f"dataset {dataset_name} is not an h3 dataset. Instead it"
                f"was {ds_type}"
            )

        raw: List[Tuple] = self._lat_long_get_radius(
            dataset_name,
            latitude,
            longitude,
            radius,
            resolution,
            year,
            month,
            day
        )

        out = self._row_to_cell_out(raw, val_col_names)
        return out

    def _lat_long_get_radius(
            self,
            dataset_name: str,
            latitude: float,
            longitude: float,
            radius: float,
            resolution: Optional[int],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[Tuple]:
        """
        Retrieve GISS geo data within a specified radius of a specific
        geographic point, specified by longitude/latitude.

        :param dataset_name: the name of the dataset to retrieve data from
        :type dataset_name: str
        :param latitude: The latitude of the central point
        :type latitude: float
        :param longitude: The longitude of the central point
        :type longitude: float
        :param radius:
            The radius to retrieve data for.
            If radius is -1 or >40075 (circumference of earth), all values
            will be retrieved
        :type radius: float
        :param resolution: The h3 resolution level to retrieve for
        :type resolution: int
        :param year:The year to retrieve data for.
        :type year: int
        :param month: The month to retrieve data for
        :type month: Optional[int]
        :param day: The day to retrieve data for
        :type day: Optional[int]
        :return: The data within specified radius
        :rtype: List[Dict[str, Any]]
        """

        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        val_col_names: List[str] = meta["value_columns"]["key"]
        ds_type = meta["dataset_type"]

        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type, resolution
        )

        out_db_path = self._get_db_path(dataset_name)
        connection = duckdb.connect(database=out_db_path)

        time_filter, time_params = self._get_time_filters(
            meta["interval"], year, month, day)

        value_columns = ", ".join(val_col_names)

        if ds_type == "h3":
            cell_column = "cell"
        elif ds_type == "point":
            col_list = connection.execute(f"describe {table_name}").fetchall()
            all_col_names = list(map(
                lambda c: c[0],
                col_list
            ))
            cell_col_names = list(filter(
                lambda cn: dataset_utilities.col_name_is_point_res_col(cn),
                all_col_names
            ))
            cell_column = ", ".join(cell_col_names)
        else:
            raise OperationUnsupportedException(
                "only h3 and point dataset types are supported"
                " for retrieving values within radius."
                f" Provided type was: {ds_type}"
            )

        # 40075 is circumference of earth, so any larger retrieves everything
        if radius == -1 or radius >= 40075:
            radius_where = None
        elif resolution is not None and \
                radius < self._get_min_radius(resolution):
            # if res is none, this is point dataset, which has no min radius
            raise Exception(f"radius must be at least"
                            f" {self._get_min_radius(resolution)}"
                            f" at resolution {resolution}")
        elif resolution is None and radius < 0:
            raise Exception(f"radius cannot be negative. radius was {radius}")
        else:
            radius_where = self._get_within_radius_where_clause(
                "latitude",
                "longitude",
                latitude,
                longitude,
                radius
            )
        full_where = self._combine_where_clauses([time_filter, radius_where])
        sql = f"""
                SELECT {cell_column}, latitude, longitude, {value_columns}
                FROM {table_name}
                {full_where}
            """

        raw_result: List[Tuple] = connection\
            .execute(sql, time_params).fetchall()

        return raw_result


    def cell_id_to_value_h3(
            self,
            dataset_name: str,
            cell: str,
            year: int,
            month: Optional[int],
            day: Optional[int]
    ) -> List[CellDataRow]:
        """
        Retrieve geo data for a specific cell in a dataset

        :param dataset_name: The name of the dataset to retrieve data for
        :type dataset_name: str
        :param cell: The ID of the cell
        :type cell: str
        :param year: The year to retrieve data for.
        :type year: int
        :param month: The month to retrieve data for
        :type month: Optional[int]
        :param day: The day to retrieve data for
        :type day: Optional[int]
        :return: The data for specified cell
        :rtype: Dict[str, Any]
        """

        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        col_names: List[str] = meta["value_columns"]["key"]
        value_columns = ", ".join(col_names)
        ds_type = meta["dataset_type"]
        if ds_type != "h3":
            raise OperationUnsupportedException(
                "the dataset specified was not an h3 dataset. This dataset:"
                f" {dataset_name} is of type: {ds_type}"

            )

        # Get resolution from cell (cell string is built using
        # an explicit resolution)
        resolution = h3.h3_get_resolution(cell)

        db_name = f"{dataset_name}.duckdb"
        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type, resolution
        )

        out_db_path = os.path.join(self.geo_out_db_dir, db_name)
        connection = duckdb.connect(database=out_db_path)

        params = [cell]

        time_filter, time_params = self._get_time_filters(
            meta["interval"], year, month, day)
        for param in time_params:
            params.append(param)

        cell_where = "cell = ?"
        full_where = self._combine_where_clauses([cell_where, time_filter])

        sql = f"""
            SELECT cell, latitude, longitude, {value_columns}
            FROM {table_name}
            {full_where}
        """

        row: Tuple = connection.execute(sql, params).fetchone()
        out = self._row_to_cell_out([row], col_names)


        return out

    def cell_id_to_value_point(
            self,
            dataset_name: str,
            cell: str,
            year: int,
            month: Optional[int],
            day: Optional[int]
    ) -> List[PointDataRow]:
        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        col_names: List[str] = meta["value_columns"]["key"]
        value_columns = ", ".join(col_names)
        ds_type = meta["dataset_type"]
        if ds_type != "point":
            raise OperationUnsupportedException(
                "the dataset specified was not an h3 dataset. This dataset:"
                f" {dataset_name} is of type: {ds_type}"

            )

        # Get resolution from cell (cell string is built using
        # an explicit resolution)
        resolution = h3.h3_get_resolution(cell)

        db_name = f"{dataset_name}.duckdb"
        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type, resolution
        )

        out_db_path = os.path.join(self.geo_out_db_dir, db_name)
        connection = duckdb.connect(database=out_db_path)

        params = [cell]

        time_filter, time_params = self._get_time_filters(
            meta["interval"], year, month, day)
        for param in time_params:
            params.append(param)

        cell_col = dataset_utilities.get_point_res_col(resolution)
        cell_where = f"{cell_col} = ?"
        full_where = self._combine_where_clauses([cell_where, time_filter])

        col_list = connection.execute(f"describe {table_name}").fetchall()
        all_col_names = list(map(
            lambda c: c[0],
            col_list
        ))
        cell_col_names = list(filter(
            lambda cn: dataset_utilities.col_name_is_point_res_col(cn),
            all_col_names
        ))
        all_cell_column = ", ".join(cell_col_names)

        sql = f"""
            SELECT {all_cell_column}, latitude, longitude, {value_columns}
            FROM {table_name}
            {full_where}
        """

        rows = connection.execute(sql, params).fetchall()

        out = self._row_to_point_out(rows, col_names, cell_col_names)
        return out

    def lat_long_to_value(
            self,
            dataset_name: str,
            latitude: float,
            longitude: float,
            resolution: int,
            year: int,
            month: Optional[int],
            day: Optional[int]
    ) -> List[CellDataRow]:
        """
        Retrieve GISS geo data for the cell that contains a specified point.


        :param dataset_name: The name of the dataset to retrieve data for
        :type dataset_name: str
        :param latitude: The latitude of the point
        :type latitude: float
        :param longitude: The longitude of the point
        :type longitude: float
        :param resolution: The h3 resolution level to retrieve for
        :type resolution: int
        :param year:
            The year to retrieve data for.
        :type year: int
        :param month:
            The month to retrieve data for
        :type month: str
        :param day: The day to retrieve data for
        :type day: Optional[int]
        :return: The data for the cell that contains the point
        :rtype: Dict[str, Any]
        """

        cell = h3.geo_to_h3(latitude, longitude, resolution)
        return self.cell_id_to_value_h3(
            dataset_name,
            cell,
            year,
            month,
            day
        )

    def filter(
            self, shapefile: str, resolution: int = 0, tolerance: float = 0.1
    ) -> List[str]:
        """
        Filter all H3 cells to get only cells that are considered "land"
        based upon the input shapefile

        :param shapefile:
            Path to a shapefile that will determine which
            cells are included
        :type shapefile: str
        :param resolution: The h3 resolution level to calculate for
        :type resolution: int
        :param tolerance: TODO: CURRENTLY UNUSED, PURPOSE UNCLEAR
        :type tolerance: float
        :return: List of cell IDs for cells that passed the filter
        :rtype: List[str]
        """

        total_cells = self.CELLS_AT_RESOLUTION[resolution]
        cell_km2 = self.CELLS_KM2_AT_RESOLUTION[resolution]
        logger.info(
            f"Total cells using resolution:{resolution} total_cells:{total_cells} cell_km2:{cell_km2}")

        s = Shape(shapefile)
        buffer = Geomesh.get_buffer(resolution)
        cells_included = s.get_h3_in_shape(
            buffer,
            resolution,
            True,
            min_latitude=MIN_LAT,
            max_latitude=MAX_LAT,
            min_longitude=MIN_LONG,
            max_longitude=MAX_LONG
        )

        logger.info(f"number of cells after filter :{len(cells_included)}")
        return list(cells_included)

    def visualize(self, cells: List[str], map_path: str) -> str:
        """
        Create static map of cells
        """
        logger.info(f"Visualizing cells:{len(cells)}")
        return self._visualize_map(cells, map_path)

    def bounding_box_get(
            self,
            dataset_name: str,
            resolution: int,
            min_lat: float,
            max_lat: float,
            min_long: float,
            max_long: float,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int]
    ) -> List[Dict[str, Any]]:
        if not self.metadb.ds_meta_exists(dataset_name):
            raise Exception(f"dataset {dataset_name} not registered"
                            f" in metadata.")

        meta = self.metadb.get_ds_metadata(dataset_name)
        col_names: List[str] = meta["value_columns"]["key"]
        ds_type = meta["dataset_type"]

        table_name = self._table_name_from_ds_type(
            dataset_name, ds_type, resolution
        )

        ds_db_path = self._get_db_path(dataset_name)
        connection = duckdb.connect(database=ds_db_path)

        col_names: List[str] = meta["value_columns"]["key"]
        value_columns = ", ".join(col_names)

        cells = list(self._get_h3_in_boundary(
            resolution,
            min_lat,
            max_lat,
            min_long,
            max_long,
        ))

        time_filter, time_params = self._get_time_filters(
            meta["interval"], year, month, day)

        cells_per_part = 20000
        if len(cells) > cells_per_part:
            num_parts = math.ceil(float(len(cells)) / float(20000))

            cells_split = []
            for index in range(num_parts):
                cells_split.append(
                    cells[
                    index * cells_per_part:
                    cells_per_part * (index + 1)
                    ])
        else:
            cells_split = [list(cells)]

        data = []

        if ds_type == "h3":
            cell_column = "cell"
        elif ds_type == "point":
            cell_column = dataset_utilities.get_point_res_col(resolution)
        else:
            raise OperationUnsupportedException(
                "only h3 and point dataset types are supported"
                " for retrieving values within radius."
                f" Provided type was: {ds_type}"
            )

        for cell_part in cells_split:
            part_str = ""
            for cell in cell_part:
                part_str = part_str + f"'{cell}',"
            part_str = part_str[:-1]
            in_clause = f"""
                          {cell_column} IN ({part_str})
                       """

            full_where = self._combine_where_clauses([time_filter, in_clause])

            sql = f"""
                       SELECT {cell_column}, latitude, longitude, {value_columns}
                       FROM {table_name}
                       {full_where} 
                   """

            res = connection.execute(sql, time_params).fetchall()

            for res_row in res:
                data.append(res_row)

        # format output as a json object
        out = []
        for row in data:
            num_val_cols = len(col_names)
            out_json = {
                "cell": row[0],
                "latitude": row[1],
                "longitude": row[2],
            }
            for i in range(0, num_val_cols):
                index = i + 3
                out_json[col_names[i]] = row[index]
            out.append(out_json)
        return out

    #####
    # INTERNAL
    #####

    def _row_to_cell_out(
            self,
            rows: List[Tuple],
            val_col_names: List[str]
    ):
        out = []
        for row in rows:
            num_val_cols = len(val_col_names)
            values_dict = {}
            for i in range(0, num_val_cols):
                index = i + 3
                values_dict[val_col_names[i]] = row[index]
            out.append(
                CellDataRow(
                    cell=row[0],
                    latitude=row[1],
                    longitude=row[2],
                    values=values_dict
                )
            )
        return out

    def _row_to_point_out(
            self,
            rows: List[Tuple],
            val_col_names: List[str],
            cell_col_names: List[str]
    ) -> List[PointDataRow]:
        out = []
        for row in rows:
            num_cell_cols = len(cell_col_names)
            cell_cols = {}
            values = {}

            for i in range(0, num_cell_cols):
                cell_cols[cell_col_names[i]] = row[i]

            for i in range(0, len(row) - (num_cell_cols + 2)):
                values[val_col_names[i]] = row[num_cell_cols + 2 + i]

            out.append(PointDataRow(
                latitude=row[num_cell_cols],
                longitude=row[num_cell_cols + 1],
                cells=cell_cols,
                values=values
            ))
        return out

    def _get_h3_in_boundary(
            self,
            res: int,
            min_lat: float,
            max_lat: float,
            min_long: float,
            max_long: float,
    ) -> Set[str]:

        from shapely import Polygon
        from shapely.geometry import shape
        bot_left = (min_lat, min_long)
        top_left = (max_lat, min_long)
        top_right = (max_lat, max_long)
        bot_right = (min_lat, max_long)
        coords = (bot_left, top_left, top_right, bot_right, bot_left)
        boundary_poly = Polygon(coords)

        geojson = shape(boundary_poly).__geo_interface__
        overlap_cells = h3.polyfill(geojson, res)
        return set(overlap_cells)

    def _get_time_filters(
            self,
            interval: str,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int],

    ) -> Tuple[Optional[str], List[Any]]:
        if interval not in metadata.VALID_META_INTERVALS:
            raise IntervalInvalidException(
                f"recieved invalid interval: {interval}. Valid intervals"
                f" are {metadata.VALID_META_INTERVALS}"
            )

        has_year = ["yearly", "monthly", "daily"]

        f = None
        params = []
        if interval in has_year:
            if year is None:
                raise IntervalInvalidException(
                    "No year was provided. Year must be provided for"
                    f" interval: {interval}"
                )
            else:
                params = [year]
                f = "year = ?"

        if interval == "monthly" or interval == "daily":
            if month is None:
                raise IntervalInvalidException(
                    "No month was provided. Month must be provided for"
                    f" interval: {interval}"
                )
            else:
                f = f"{f} AND month = ?"
                params.append(month)

        if interval == "daily":
            if day is None:
                raise IntervalInvalidException(
                    "No day was provided. Day must be provided for"
                    f" interval: {interval}"
                )
            else:
                f = f"{f} AND day = ?"
                params.append(day)

        return f, params

    def _table_name_from_ds_type(
            self,
            dataset_name: str,
            ds_type: str,
            resolution: Optional[int] = None
    ):
        if ds_type not in metadata.VALID_DATASET_TYPES:
            raise InvalidArgumentException(
                f"dataset type: {ds_type} is not a valid type."
                f" Valid types: {metadata.VALID_DATASET_TYPES}"
            )

        if ds_type == "h3":
            if resolution is None:
                raise InvalidArgumentException(
                    "resolution parameter cannot be None for h3 dataset"
                )
            table_name = dataset_name + f"_{resolution}"
        elif ds_type == "point":
            table_name = dataset_name
        else:
            raise InvalidArgumentException(
                f"dataset type: {ds_type} not yet implemented"
            )
        return table_name

    def _combine_where_clauses(self, clauses: List[Optional[str]]) -> str:
        joined = " AND ".join(
            filter(
                lambda x: x is not None,
                clauses
            )
        )
        return f"WHERE {joined}"

    def _get_min_radius(self, resolution: int) -> float:
        """
        It is possibble to pick a radius small enough that - with the right
        selection of centerpoint - it will include no cell centroids. As
        centriods are used to calculate inclusion in return data, this gets
        the minimum radius for which at least one centroid must be within
        range.

        :param resolution: the resolution level to get the radius for
        :type resolution: int
        :return: the minimum radius with a guaranteed cell inclusion, in km
        :rtype: int
        """
        if resolution < 0 or resolution > 15:
            raise Exception("resolution must be between 0 and 15")
        area = self.CELLS_KM2_AT_RESOLUTION[resolution]

        # A hexagon's log diagonal is 1/2 the length of its side, so
        # the radius desired is equal to the side length.
        #
        # the equation for a hexagon's area is:
        # A = 3/2 * sqrt(3) * (side_length)^2
        # rearranging for side length gives below result
        radius = math.sqrt(2 * area / (3 * sqrt(3)))

        return radius

    def _get_within_radius_where_clause(
            self,
            lat_col: str,
            long_col: str,
            center_lat: float,
            center_long: float,
            radius_km: float
    ) -> str:
        """
        Get a WHERE clause that will retrieve all rows that have a position
        (latitude/longitude) that is within a specified radius of a
        central point. Note that the "WHERE" portion of the where clause is
        excluded to more easily allow combining with other where clauses.

        :param lat_col:
            The name of the column in the database table that contains
            the latitude of data points
        :type lat_col: str
        :param long_col:
            The name of the column in the database table that contains
            the longitude of data points
        :type long_col: str
        :param center_lat: The latitude of the center point
        :type center_lat: float
        :param center_long: The longitude of the center point
        :type center_long: float
        :param radius_km:
            Only rows inside this radius (in kilometers) of the central point
            will be returned
        :type radius_km: float
        :return: The where clause, ready to be inserted into an SQL statement
        :rtype: str
        """

        # Copied from StackOverflow post by Rajeev Shenoy
        #  https://stackoverflow.com/a/7783777/2813306
        #  Which is in turn based off of information found here:
        #   https://www.movable-type.co.uk/scripts/latlong.html
        where = f"""
            (
                acos(sin({lat_col} * 0.0175) * sin({center_lat} * 0.0175)
               + cos({lat_col} * 0.0175) * cos({center_lat} * 0.0175) *
                 cos(({center_long} * 0.0175) - ({long_col} * 0.0175))
              ) * 6371 <= {radius_km}
            )
        """

        return where

    def _to_latlon(self, cell: str) -> Tuple[float, float]:
        """
        Convert an H3 cell into longitude and latitude

        :param cell: The cell to get lat/long for
        :type cell: str

        :return: The latitude, longitude of the cell center
        :rtype: Tuple[float, float]
        """

        # logger.info(f"Using cell:{cell}")
        coordinates = h3.h3_to_geo(cell)
        # logger.info(f"result:{result}")

        return coordinates

    @staticmethod
    def get_buffer(resolution: int, multiplier: float = 1.5) -> float:

        cell_km2 = Geomesh.CELLS_KM2_AT_RESOLUTION[resolution]
        cell_radius_km = math.sqrt(cell_km2 / math.pi)

        buffer = 0
        if resolution >= 2:
            buffer = cell_radius_km / KM_PER_DEGREE * multiplier
        logger.info(f"buffer degrees:{buffer} km:{buffer * KM_PER_DEGREE}")

        return buffer


    def _calculate_overlap(self, gdf, cell):
        resolution = h3.h3_get_resolution(cell)
        cell_km2 = Geomesh.CELLS_KM2_AT_RESOLUTION[resolution]
        cell_polygon = Polygon(h3.h3_to_geo_boundary(cell, geo_json=True))

        intersection_area_km2 = 0
        overlap = 0
        for original_geom in gdf.geometry:
            if original_geom.intersects(cell_polygon):
                original_geom = original_geom.buffer(0)
                cell_polygon = cell_polygon.buffer(0)
                intersection = original_geom.intersection(cell_polygon)
                # logger.info(f"intersection:{intersection}")
                factor = self._calculate_factor(cell)
                intersection_area_km2 += intersection.area * factor
                # logger.info(f"intersection_area_km2:{intersection_area_km2}")
                overlap = intersection_area_km2 / cell_km2

        return overlap

    def _calculate_factor(self, cell):
        # Regular loop to calculate the sum of intersection areas
        cell_polygon = Polygon(h3.h3_to_geo_boundary(cell, geo_json=True))

        # Calculate mean latitude of the polygon
        mean_latitude = sum(
            [point[1] for point in cell_polygon.exterior.coords]) / len(
            cell_polygon.exterior.coords)

        # Compute scale factor
        angle_factor = math.cos(math.radians(mean_latitude))
        scale_factor = KM_PER_DEGREE * KM_PER_DEGREE * angle_factor

        return scale_factor

    def _visualize_map(self, cells: List[str], map_path: str) -> str:
        return Visualizer.visualize_h3_cells(cells, map_path)


    def _get_db_path(self, db_name: str) -> str:
        return os.path.join(self.geo_out_db_dir, f"{db_name}.duckdb")