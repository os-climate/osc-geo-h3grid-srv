import argparse
import math

import folium
import pandas

# Add the source to sys.path (this is a short-term fix)
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '../../..', 'src'))
print(parent_dir)
sys.path.append(parent_dir)

from geoserver.geomesh import Geomesh
from cli.visualizer import HexGridVisualizer, PointLocationVisualizer

# asset_ds_name = "spain_asset_data"
flood_ds_name = "flood_depth_10_year_spain"
db_dir = "./tmp"
resolution = 7

asset_correlated_path = "./tmp/final_asset_correlation_spain.parquet"


min_lat=35.50
max_lat=44.31
min_long=-9.98
max_long=4.71

def get_blank_map():
    lat_range = max_lat - min_lat
    long_range = max_long - min_long
    zoom_level_lat = -math.log2(lat_range / 180) + 1
    zoom_level_long = -math.log2(long_range / 360) + 1
    zoom = math.ceil(min(zoom_level_lat, zoom_level_long))

    avg_lat = (min_lat + max_lat) / 2
    avg_long = (min_long + max_long) / 2

    geo_map = folium.Map(
        location=(avg_lat, avg_long),
        min_lon=math.floor(min_long),
        min_lat=math.floor(min_lat),
        max_lon=math.ceil(max_long),
        max_lat=math.ceil(max_lat),
        width="100%",
        height="100%",
        max_bounds=True,
        png_enabled=True,
        min_zoom=zoom,
        prefer_canvas=True
    )
    sw = [min_lat, min_long]
    ne = [max_lat, max_long]
    geo_map.fit_bounds(bounds=[sw, ne])
    return geo_map

def get_flood_dataset():
    geo = Geomesh(db_dir)
    ds = geo.bounding_box_get(
        flood_ds_name,
        resolution,
        min_lat,
        max_lat,
        min_long,
        max_long,
        None,
        None,
        None
    )
    ds_pandas = pandas.json_normalize(ds)

    return ds_pandas

def get_asset_dataset():
    raw = pandas.read_parquet(asset_correlated_path)

    print("filtering for non-zero flood depth")
    filtered = raw[raw['value'].notna()]

    return filtered.reset_index()

def generate_visual(out: str):

    map = get_blank_map()

    print("loading flood dataset")
    flood_ds = get_flood_dataset()

    print("adding flood dataset to map")
    hex_vis = HexGridVisualizer(
        flood_ds,
        "value",
        (0,0,255),
        min_lat,
        max_lat,
        min_long,
        max_long
    )
    hex_vis.add_dataset_to_map(map, resolution, 0.0001, ds_type="point")

    print("loading asset dataset")
    asset_ds = get_asset_dataset()

    print("filtering to")

    print("adding asset dataset to map")
    point_vis = PointLocationVisualizer(
        asset_ds,
        "uuid",
        min_lat,
        max_lat,
        min_long,
        max_long
    )

    point_vis.add_dataset_to_map(map)

    map.save(out)
    print(f"created output {out}")






def get_arg_parser():
    parser = argparse.ArgumentParser(description="combined visual generator")

    parser.add_argument(
        "--output",
        required=True,
        help="path to the output file that will be created"

    )

    return parser


if __name__ == "__main__":
    args = get_arg_parser().parse_args()

    generate_visual(args.output)


    print("done")