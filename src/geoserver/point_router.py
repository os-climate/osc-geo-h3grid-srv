# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
from typing import Optional, List
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from geoserver.geomesh import Geomesh, PointDataRow
from .route_constants import API_PREFIX
from . import state

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOGGING_LEVEL = logging.INFO
logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

router = APIRouter()

POINT_ENDPOINT_PREFIX = API_PREFIX + "/datasets" +  "/point"

class PointLatLongRadiusArgs(BaseModel):

    latitude: float = Field(description="The latitude of the central point")

    longitude: float = Field(description="The longitude of the central point")

    radius: float = Field(
        description="The radius to retrieve around the central point")

    year: Optional[int] = Field(
        None,description="The year to retrieve data for")

    month: Optional[int] = Field(
        None,description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")

@router.post(POINT_ENDPOINT_PREFIX + "/latlong/radius/{dataset}")
async def point_latlong_radius_post(
        dataset: str,
        params: PointLatLongRadiusArgs
) -> List[PointDataRow]:
    """
    Retrieves all data from specified point dataset that fall within a radius
    of a specified central point.
    :return: The data within specified radius
    :rtype: List[PointDataRow]
    """
    logger.info(f"Retrieving point lat-lon radius, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.lat_long_get_radius_point(
            dataset,
            params.latitude,
            params.longitude,
            params.radius,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PointCellRadiusArgs(BaseModel):
    cell: str
    """The ID of the central cell"""

    radius: float
    """The radius to retrieve around the central point"""

    year: Optional[int] = None
    """The year to retrieve data for"""

    month: Optional[int] = None,
    """The month to retrieve data for"""

    day: Optional[int] = None
    """The day to retrieve data for"""


@router.post(POINT_ENDPOINT_PREFIX + "/cell/radius/{dataset}")
async def point_cell_radius_post(
        dataset: str,
        params: PointCellRadiusArgs
) -> List[PointDataRow]:
    """
    Retrieve GISS geo data within a specified radius of a specific
    h3 cell, specified by cell ID

    :return: The data within specified radius
    :rtype: List[Dict[str, Any]]
    """
    logger.info(f"Retrieving point cell radius, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.cell_get_radius_point(
            dataset,
            params.cell,
            params.radius,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PointCellPointArgs(BaseModel):
    cell: str
    """The ID of the central cell"""

    year: Optional[int] = None
    """The year to retrieve data for"""

    month: Optional[int] = None,
    """The month to retrieve data for"""

    day: Optional[int] = None
    """The day to retrieve data for"""


@router.post(POINT_ENDPOINT_PREFIX + "/cell/point/{dataset}")
async def point_cell_point(
        dataset: str,
        params: PointCellPointArgs
) -> List[PointDataRow]:
    """
    Retrieve all points that fall within a specific cell

    :return: The data for specified cell
    :rtype: Dict[str, Any]
    """

    logger.info(f"Retrieving point cell, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.cell_id_to_value_point(
            dataset,
            params.cell,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class GeomeshShapefileArgs(BaseModel):
    shapefile: str
    """
    The shapefile to use. Must be a local file on the filesystem
    of the server.
    """

    region: Optional[str] = None
    """
    The region within the shapefile to use. Only data within this
    specific region will be retrieved. Intended for use in cases where
    multiple entities are defined in one file (ex. a file containing
    the boundaries of every country in the world), but only a single one
    should be used.
    """

    year: Optional[int] = None
    """The year to retrieve data for"""

    month: Optional[int] = None,
    """The month to retrieve data for"""

    day: Optional[int] = None
    """The day to retrieve data for"""


@router.post(POINT_ENDPOINT_PREFIX + "/shapefile/{dataset}")
async def geomesh_shapefile(
        dataset: str,
        params: GeomeshShapefileArgs
) -> List[PointDataRow]:

    logger.info(f"Retrieving point shapefile, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.shapefile_get_point(
            dataset,
            params.shapefile,
            params.region,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
