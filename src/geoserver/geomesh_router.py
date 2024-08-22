# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
from typing import Optional, Dict, List
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from geoserver.geomesh import Geomesh, CellDataRow
from .route_constants import API_PREFIX
from . import state

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOGGING_LEVEL = logging.INFO
logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

router = APIRouter()

GEO_ENDPOINT_PREFIX = API_PREFIX + "/geomesh"


class GeomeshLatLongRadiusArgs(BaseModel):
    latitude: float = Field(description="The latitude of the central point")

    longitude: float = Field(description="The longitude of the central point")

    radius: float = Field(
        description="The radius to retrieve around the central point"
    )

    resolution: int = Field(
        3,
        description="The h3 resolution level to retrieve data for"
    )

    year: Optional[int] = Field(
        None, description="The year to retrieve data for")

    month: Optional[int] = Field(
        None, description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")

@router.post(GEO_ENDPOINT_PREFIX + "/latlong/radius/{dataset}")
async def geomesh_latlong_radius_post(
        dataset: str,
        params: GeomeshLatLongRadiusArgs
) -> List[CellDataRow]:
    """
    Retrieves data for all GISS H3 cells that fall within a radius
    of a specified central point.
    :return: The data within specified radius
    :rtype: List[CellDataRow]
    """
    logger.info(f"Retrieving data for lat-lon radius, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.lat_long_get_radius_h3(
            dataset,
            params.latitude,
            params.longitude,
            params.radius,
            params.resolution,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class GeomeshLatLongPointArgs(BaseModel):
    latitude: float = Field(description="The latitude of the central point")

    longitude: float = Field(description="The longitude of the central point")

    resolution: int = Field(
        7, description="The resolution to retrieve data for")

    year: Optional[int] = Field(
        None, description="The year to retrieve data for")

    month: Optional[int] = Field(
        None, description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")


@router.post(GEO_ENDPOINT_PREFIX + "/latlong/point/{dataset}")
async def geomesh_latlong_point_post(
        dataset: str,
        params: GeomeshLatLongPointArgs
) -> List[CellDataRow]:
    """
    Retrieve GISS geo data for the cell that contains a specified point.

    :return: The data for the cell that contains the point
    :rtype: Dict[str, Any]
    """

    logger.info(f"Retrieving data for lat-lon point, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        resp = geo.lat_long_to_value(
            dataset,
            params.latitude,
            params.longitude,
            params.resolution,
            params.year,
            params.month,
            params.day
        )
        return resp
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class GeomeshCellRadiusArgs(BaseModel):
    cell: str = Field(description="The ID of the central cell")

    radius: float = Field(
        description="The radius to retrieve around the central point")

    year: Optional[int] = Field(
        None, description="The year to retrieve data for")

    month: Optional[int] = Field(
        None, description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")


@router.post(GEO_ENDPOINT_PREFIX + "/cell/radius/{dataset}")
async def geomesh_cell_radius_post(
        dataset: str,
        params: GeomeshCellRadiusArgs
) -> List[CellDataRow]:
    """
    Retrieve GISS geo data within a specified radius of a specific
    h3 cell, specified by cell ID

    :return: The data within specified radius
    :rtype: List[Dict[str, Any]]
    """

    logger.info(f"Retrieving data for cell radius, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.cell_get_radius_h3(
            dataset,
            params.cell,
            params.radius,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class GeomeshCellPointArgs(BaseModel):
    cell: str = Field(description="The ID of the central cell")

    year: Optional[int] = Field(
        None, description="The year to retrieve data for")

    month: Optional[int] = Field(
        None, description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")


@router.post(GEO_ENDPOINT_PREFIX + "/cell/point/{dataset}")
async def geomesh_cell_point(
        dataset: str,
        params: GeomeshCellPointArgs
) -> List[CellDataRow]:
    """
    Retrieve geo data for a specific cell

    :return: The data for specified cell
    :rtype: Dict[str, Any]
    """

    logger.info(f"Retrieving data for cell point, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.cell_id_to_value_h3(
            dataset,
            params.cell,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class GeomeshShapefileArgs(BaseModel):
    shapefile: str = Field(
        description="The shapefile to use. Must be a local file on"
                    " the filesystem of the server."
    )

    region: Optional[str] = Field(
        None,
        description="The region within the shapefile to use. Only data within"
                    " this specific region will be retrieved. Intended for"
                    " use in cases where multiple entities are defined in one"
                    " file (ex. a file containing the boundaries of every"
                    " country in the world), but only a single one should"
                    " be used.")

    resolution: int = Field(
        default=7,
        description="The h3 resolution to retrieve data for")

    year: Optional[int] = Field(
        None, description="The year to retrieve data for")

    month: Optional[int] = Field(
        None, description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")


@router.post(GEO_ENDPOINT_PREFIX + "/shapefile/{dataset}")
async def geomesh_shapefile(
        dataset: str,
        params: GeomeshShapefileArgs
) -> List[CellDataRow]:

    logger.info(f"Retrieving data shapefile, dataset:{dataset} params:{params}")

    db_dir = state.get_global("database_dir")
    geo = Geomesh(db_dir)
    try:
        return geo.shapefile_get(
            dataset,
            params.shapefile,
            params.region,
            params.resolution,
            params.year,
            params.month,
            params.day
        )
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
