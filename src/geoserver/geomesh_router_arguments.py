# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-09-11 by 15205060+DavisBroda@users.noreply.github.com
from typing import List, Optional

from pydantic import BaseModel, Field


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


class GeomeshCellPointArgs(BaseModel):
    cell: str = Field(description="The ID of the central cell")

    year: Optional[int] = Field(
        None, description="The year to retrieve data for")

    month: Optional[int] = Field(
        None, description="The month to retrieve data for")

    day: Optional[int] = Field(
        None, description="The day to retrieve data for")


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


class LocatedAsset(BaseModel):
    id: str = Field(
        description="id of this asset. Must be unique within this request"
    )
    latitude: float = Field(description="latitude of this asset")
    longitude: float = Field(description="longitude of this asset")


class AssetFilter(BaseModel):
    column: str = Field(description="the column to be filtered on")
    filter_type: str = Field(
        description="type of filter to perform. "
                    "Valid values: [greater_than, greater_than_or_equal, "
                    "lesser_than, lesser_than_or_equal, equal_to]"
    )
    target_value: float = Field(
        description="The value to be used in the filter. For example in a"
                    "greater_than filter only values greater than the"
                    "target_value will be returned"
    )


class DatasetArg(BaseModel):
    name: str = Field(
        description="The name of the dataset in question"
    )
    filters: List[AssetFilter] = Field(
        description="The filter(s) to apply to this dataset"
    )


class FilterArgs(BaseModel):
    assets: List[LocatedAsset] = Field(
        description="The assets that to be filtered"
    )
    datasets: List[DatasetArg] = Field(
        description="The datasets that are to be filtered on, and their"
                    "associated filters."
    )
