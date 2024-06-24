# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import json
import logging
import os.path
from typing import List, Optional, Dict, Any, Tuple

import pandas

from geoserver.geomesh import Geomesh
from geoserver import geomesh_router, metadata
from geoserver import point_router
import visualizer
from common import httputils


# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


# Abstract class
class CliExecGeospatial:

    def __init__(self, config: dict):
        """Create CLI"""
        logger.info(f"Using config:{config}")
        self.host = config["host"]
        self.port = config["port"]

    #####
    # FILTERING
    #####

    def initialize(
            self,
            database_dir: str) -> bool:
        """
        Create a temperature data base as demo data

        TODO: intended to be replaced with more reusable database stuff
            at a later date

        returns true id db created, returns false if db already exists
        """
        if os.path.exists(database_dir):
            logger.info(f"Database already exists at location {database_dir}")
            return False
        os.makedirs(database_dir)
        return True

    def show_cell_radius(
            self,
            dataset: str,
            cell: str,
            radius: float,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int],
            type: str
    ):
        logger.info(f"Showing cell:{cell}")

        params = {
            "cell": cell,
            "radius": radius
        }
        if year is not None:
            params["year"] = year
        if month is not None:
            params["month"] = month
        if day is not None:
            params["day"] = day

        if type == "h3":
            service = f"{geomesh_router.GEO_ENDPOINT_PREFIX}/cell/radius/{dataset}"
        else:
            service = f"{point_router.POINT_ENDPOINT_PREFIX}/cell/radius/{dataset}"

        method = "POST"
        response = httputils.httprequest(
            self.host, self.port, service, method, obj=params)
        return response

    def show_cell_point(
            self,
            dataset: str,
            cell: str,
            year: Optional[int],
            month: Optional[int],
            day: Optional[int],
            type: str
    ):

        params = {
            "cell": cell,
        }
        if year is not None:
            params["year"] = year
        if month is not None:
            params["month"] = month
        if day is not None:
            params["day"] = day

        if type == "h3":
            service = f"{geomesh_router.GEO_ENDPOINT_PREFIX}/cell/point/{dataset}"
        else:
            service = f"{point_router.POINT_ENDPOINT_PREFIX}/cell/point/{dataset}"
        method = "POST"
        response = httputils.httprequest(
            self.host, self.port, service, method, obj=params)
        return response

    def show_latlong_radius(
            self,
            dataset: str,
            latitude: float,
            longitude: float,
            radius: float,
            resolution: Optional[int],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int],
            type: str
    ):

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "radius": radius,
            "year": year
        }

        if resolution is not None:
            params["resolution"] = resolution
        if month is not None:
            params["month"] = month
        if day is not None:
            params["day"] = day

        if type == "h3":
            service = f"{geomesh_router.GEO_ENDPOINT_PREFIX}/latlong/radius/{dataset}"
        else:
            service = f"{point_router.POINT_ENDPOINT_PREFIX}/latlong/radius/{dataset}"
        logger.info(f"calling server at url {service}")
        method = "POST"
        response = httputils.httprequest(
            self.host, self.port, service, method, obj=params)
        return response

    def show_latlong_point(
            self,
            dataset: str,
            latitude: float,
            longitude: float,
            resolution: Optional[int],
            year: int,
            month: Optional[int],
            day: Optional[int]
    ):

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "year": year
        }

        if resolution is not None:
            params["resolution"] = resolution
        if month is not None:
            params["month"] = month
        if day is not None:
            params["day"] = day

        service = f"{geomesh_router.GEO_ENDPOINT_PREFIX}/latlong/point/{dataset}"
        method = "POST"
        response = httputils.httprequest(self.host, self.port, service, method,
                                         obj=params)
        return response

    def show_shapefile(
            self,
            dataset: str,
            shapefile: str,
            region: Optional[str],
            resolution: int,
            year: int,
            month: Optional[int],
            day: Optional[int],
            type: str
    ):
        params = {
            "shapefile": shapefile,
            "resolution": resolution,
            "year": year
        }
        if region is not None:
            params["region"] = region
        if month is not None:
            params["month"] = month
        if day is not None:
            params["day"] = day

        if type == "h3":
            service = f"{geomesh_router.GEO_ENDPOINT_PREFIX}/shapefile/{dataset}"
        else:
            service = f"{point_router.POINT_ENDPOINT_PREFIX}/shapefile/{dataset}"
        method = "POST"

        response = httputils.httprequest(self.host, self.port, service, method,
                                         obj=params)
        return response

    def add_meta(
            self,
            database_dir: str,
            dataset_name: str,
            description: str,
            value_columns: Dict[str, str],
            interval: str,
            dataset_type: str
    ) -> str:
        meta = metadata.MetadataDB(database_dir)
        return meta.add_metadata_entry(
            dataset_name,
            description,
            value_columns,
            interval,
            dataset_type
        )

    def show_meta(
            self,
            database_dir: str
    ) -> List[Dict[str, Any]]:
        meta = metadata.MetadataDB(database_dir)
        return meta.show_meta()

    def filter(self, shapefile: str, resolution: int, tolerance: float) -> List:
        geomesh = Geomesh(None)
        result = geomesh.filter(shapefile, resolution=resolution,
                                tolerance=tolerance)
        return result

    def visualize(self, cells_path: str, map_path: str) -> str:
        cells = None
        with open(cells_path, 'r') as file:
            cells = json.load(file)

        geomesh = Geomesh(None)
        result = geomesh.visualize(cells, map_path)
        return result

    def visualize_dataset(
            self,
            database_dir: str,
            resolution: int,
            dataset: str,
            value_column: str,
            max_color_rgb: Tuple[int, int, int],  # hex rgb values
            output_file: str,
            min_lat: float,
            max_lat: float,
            min_long: float,
            max_long: float,
            threshold: Optional[float],
            year: Optional[int],
            month: Optional[int],
            day: Optional[int],
            ds_type: str,
            visualizer_type: str = "HexGridVisualizer"
    ):
        geo = Geomesh(database_dir)
        ds = geo.bounding_box_get(
            dataset,
            resolution,
            min_lat,
            max_lat,
            min_long,
            max_long,
            year,
            month,
            day
        )
        ds_pandas = pandas.json_normalize(ds)

        if visualizer_type == "HexGridVisualizer":
            vis = visualizer.HexGridVisualizer(
                ds_pandas,
                value_column,
                max_color_rgb,
                min_lat,
                max_lat,
                min_long,
                max_long
            )
            vis.visualize_dataset(resolution, output_file, threshold, ds_type)

        elif visualizer_type == "PointLocationVisualizer":
            if ds_type != "point":
                raise ValueError(
                    "PointLocationVisualizer visualizer type is not compatible"
                    f" with dataset type {ds_type}"
                )
            vis = visualizer.PointLocationVisualizer(
                ds_pandas,
                value_column,
                min_lat,
                max_lat,
                min_long,
                max_long
            )
            vis.visualize_dataset(output_file)

        print(f"created visualization file at location: {output_file}")