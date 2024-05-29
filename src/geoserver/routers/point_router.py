# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from geoserver import state
from geoserver.bgsexception import BgsException
from geoserver.geomesh import Geomesh
from geoserver.routers import API_PREFIX

router = APIRouter()

ENDPOINT_PREFIX = API_PREFIX + "/point"


class PointLatLongRadiusArgs(BaseModel):

    latitude: float
    """The latitude of the central point"""

    longitude: float
    """The longitude of the central point"""

    radius: float
    """The radius to retrieve around the central point"""

    year: Optional[int] = None
    """The year to retrieve data for"""

    month: Optional[int] = None,
    """The month to retrieve data for"""

    day: Optional[int] = None
    """The day to retrieve data for"""

@router.post(ENDPOINT_PREFIX + "/latlong/radius/{dataset}")
async def point_latlong_radius_post(
        dataset: str,
        params: PointLatLongRadiusArgs
):
    """
    Retrieves all data from specified point dataset that fall within a radius
    of a specified central point.
    :return: The data within specified radius
    :rtype: List[Dict[str, Any]]
    """

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.lat_long_get_radius(
            dataset,
            params.latitude,
            params.longitude,
            params.radius,
            None,  # point datasets do not need resolution
            params.year,
            params.month,
            params.day
        )
    except BgsException as e:
        raise HTTPException(status_code=400, detail=str(e))

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


@router.post(ENDPOINT_PREFIX + "/cell/radius/{dataset}")
async def point_cell_radius_post(
        dataset: str,
        params: PointCellRadiusArgs
):
    """
    Retrieve GISS geo data within a specified radius of a specific
    h3 cell, specified by cell ID

    :return: The data within specified radius
    :rtype: List[Dict[str, Any]]
    """
    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.cell_get_radius(
            dataset,
            params.cell,
            params.radius,
            params.year,
            params.month,
            params.day,
            type="point"
        )
    except BgsException as e:
        raise HTTPException(status_code=400, detail=str(e))


class PointCellPointArgs(BaseModel):
    cell: str
    """The ID of the central cell"""

    year: Optional[int] = None
    """The year to retrieve data for"""

    month: Optional[int] = None,
    """The month to retrieve data for"""

    day: Optional[int] = None
    """The day to retrieve data for"""


@router.post(ENDPOINT_PREFIX + "/cell/point/{dataset}")
async def point_cell_point(
        dataset: str,
        params: PointCellPointArgs
):
    """
    Retrieve geo data for a specific cell

    :return: The data for specified cell
    :rtype: Dict[str, Any]
    """
    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.cell_id_to_value(
            dataset,
            params.cell,
            params.year,
            params.month,
            params.day
        )
    except BgsException as e:
        raise HTTPException(status_code=400, detail=str(e))


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


@router.post(ENDPOINT_PREFIX + "/shapefile/{dataset}")
async def geomesh_shapefile(
        dataset: str,
        params: GeomeshShapefileArgs
):
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
    except BgsException as e:
        raise HTTPException(status_code=400, detail=str(e))
