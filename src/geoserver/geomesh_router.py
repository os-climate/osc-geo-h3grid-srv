# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
import io
from typing import Optional, Dict, List
import logging

import pandas
from fastapi import APIRouter, HTTPException, File, UploadFile
from pydantic import BaseModel, Field

from geoserver.geomesh import Geomesh, CellDataRow
from .correlator import Correlator
from .geomesh_router_arguments import GeomeshShapefileArgs, \
    GeomeshCellPointArgs, GeomeshLatLongRadiusArgs, GeomeshLatLongPointArgs, \
    GeomeshCellRadiusArgs, LocatedAsset, DatasetArg
from .metadata import MetadataDB
from .route_constants import API_PREFIX
from . import state

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOGGING_LEVEL = logging.INFO
logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

router = APIRouter()

GEO_ENDPOINT_PREFIX = API_PREFIX + "/geomesh"


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
    logger.info(
        f"Retrieving data for lat-lon radius, dataset:{dataset} params:{params}")

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

    logger.info(
        f"Retrieving data for lat-lon point, dataset:{dataset} params:{params}")

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

    logger.info(
        f"Retrieving data for cell radius, dataset:{dataset} params:{params}")

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

    logger.info(
        f"Retrieving data for cell point, dataset:{dataset} params:{params}")

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


@router.post(GEO_ENDPOINT_PREFIX + "/filter")
async def filter(
        assets_file: UploadFile,
        datasets_file: UploadFile):
    logger.info(
        f"filter assets_file:{assets_file} datasets_file:{datasets_file}")

    # Ensure both files are JSON
    if assets_file.content_type != "application/octet-stream":
        raise HTTPException(status_code=400,
                            detail="Asset file must be octet-stream")

    if datasets_file.content_type != "application/json":
        raise HTTPException(status_code=400,
                            detail="Datasets file must be JSON")

    try:
        # Read and parse the JSON files
        import json
        assets_content = await assets_file.read()
        pq_file = io.BytesIO(assets_content)

        asset_df = pandas.read_parquet(pq_file)

        datasets_content = await datasets_file.read()
        logger.info(f"datasets_content:{preview_string(datasets_content)}")
        datasets = json.loads(datasets_content)
        dataset_args = [DatasetArg(**dataset) for dataset in datasets]

        db_dir = state.get_global("database_dir")
        correlator = Correlator(db_dir)
        results = correlator.get_correlated_data(asset_df, dataset_args)
        return results

    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


def preview_string(long_string: str, preview_length: int = 1000) -> str:
    total_length = len(long_string)
    # If the string is shorter than or equal to preview_length, return it as is.
    # Otherwise show subset of characters
    if total_length <= preview_length:
        return long_string
    else:
        preview = long_string[:preview_length]
        return f"{preview}... (showing {preview_length} of {total_length} total characters)"


@router.get(GEO_ENDPOINT_PREFIX + "/showmeta")
async def get_metadata():
    logger.info(f"retrieving metadata")
    db_dir = state.get_global("database_dir")
    metadata_db = MetadataDB(db_dir)
    results = metadata_db.show_meta()
    return results
