# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-04-26 by davis.broda@brodagroupsoftware.com
import argparse
from typing import Optional

import geopandas
import numpy
import pandas
import rasterio
import xarray
from geopandas import GeoDataFrame

AVAILABLE_FILTERS = [
    "Germany",
    "NW_Germany",
    "North_Germany"
    "France",
    "Belgium",
    "Spain"
]

#TODO: file is not continuous, but only contains 10, 30, 100, 300, 1000, 65535 (no data val)
#  this is because it contains data on which areas are flooded by the 10-year
#  flood, 30-year flood, etc.
def load_flood_data(
        tiff_file: str
) -> GeoDataFrame:
    with rasterio.open(tiff_file) as t_file:
        crs_temp = t_file.crs
        trans = t_file.transform
        no_data_val = t_file.nodatavals[0]
        file_xr = xarray.open_rasterio(t_file).isel(band=0)

    valid_data_mask = file_xr.data != no_data_val
    # values in tiff at points where condition holds
    data_array = file_xr.data[valid_data_mask]

    # indices in tiff where the condition holds
    y_indices, x_indices = numpy.where(valid_data_mask)
    epsg_x, epsg_y = trans * (x_indices, y_indices)

    if not (len(epsg_x) == len(epsg_y) == len(data_array)):
        raise ValueError("Mismatch in array lengths after processing.")

    df = pandas.DataFrame(
        {
            "x": epsg_x,
            "y": epsg_y,
            "value": data_array.flatten()
        }
    )
    print("df assembled")

    geo = geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.x, df.y),
        crs=crs_temp
    )
    print("geo df assembled")

    out = geo.to_crs(epsg=4326)

    return out

def fix_columns(geo: GeoDataFrame) -> GeoDataFrame:
    geo['longitude'] = geo.geometry.x
    geo['latitude'] = geo.geometry.y

    out = geo.drop(columns=['x', 'y', 'geometry'])
    return out

def filter_to_germany(geo: GeoDataFrame) -> GeoDataFrame:
    # boundary box is slightly bigger than germany
    min_lat = 46
    max_lat = 56
    min_long = 4
    max_long = 17

    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
    ]
    return out

def filter_to_rhine(geo: GeoDataFrame) -> GeoDataFrame:
    min_lat = 50.8
    max_lat = 52.2
    min_long = 5.8
    max_long = 8.3

    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
        ]
    return out


def filter_to_north_germany(geo: GeoDataFrame) -> GeoDataFrame:
    min_lat = 53.18
    max_lat = 54.09
    min_long = 8.74
    max_long = 10.62

    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
        ]
    return out

def filter_france(geo: GeoDataFrame) -> GeoDataFrame:
    min_lat = 41.28
    max_lat = 51.05
    min_long = -5.50
    max_long = 10.67

    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
        ]
    return out

def filter_belgium(geo: GeoDataFrame) -> GeoDataFrame:
    min_lat = 49.25
    max_lat = 51.55
    min_long = 2.19
    max_long = 6.62

    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
        ]
    return out

def filter_spain(geo: GeoDataFrame) -> GeoDataFrame:
    min_lat = 35.50
    max_lat = 44.31
    min_long = -9.98
    max_long = 4.71

    out = geo[
        (geo['latitude'] > min_lat) & (geo['latitude'] < max_lat) &
        (geo['longitude'] > min_long) & (geo['longitude'] < max_long)
        ]
    return out

def write_to_output(geo: GeoDataFrame, out_file: str) -> None:
    geo.to_parquet(out_file)

def get_arg_parser():
    parser = argparse.ArgumentParser(description="flood data converter")

    parser.add_argument(
        "--raw",
        required=True,
        help="path to the raw flood data file"

    )
    parser.add_argument(
        "--output",
        required=True,
        help="path to the output file. (should not exist prior to running)"
    )

    parser.add_argument(
        "--filter",
        required=False,
        type=str,
        default="Germany",
        help="which area to extract data for. Will only give a "
             "rough box around the area, rather than more detailed"
             f"shapefile filters. Available selections are {AVAILABLE_FILTERS}"
    )

    return parser


if __name__ == "__main__":
    args = get_arg_parser().parse_args()
    geo = load_flood_data(args.raw)
    right_cols = fix_columns(geo)
    fil = args.filter

    if fil == "Germany":
        print("filtering for germany")
        out = filter_to_germany(right_cols)
    elif fil == "NW_Germany":
        print("filtering for NW germany")
        out = filter_to_rhine(right_cols)
    elif fil == "North_Germany":
        print("filtering to North Germany")
        out = filter_to_north_germany(right_cols)
    elif fil == "France":
        print("filtering for France")
        out = filter_france(right_cols)
    elif fil == "Belgium":
        print("filtering for Belgium")
        out = filter_belgium(right_cols)
    elif fil == "Spain":
        print("filtering for Spain")
        out = filter_spain(right_cols)
    else:
        raise Exception(f"unrecognized filter: {fil}. must"
                        f" select from: {AVAILABLE_FILTERS}")

    write_to_output(out, args.output)

    print(f"Wrote {len(out)} rows to {args.output}")
