# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
#####
#
# Command line interpreter (CLI) to interact with a geospatial temporal data mesh
#
#####

import argparse
import logging
import sys
import json
import time
from functools import partial
from typing import Optional

from cli.cliexec_geospatial import CliExecGeospatial

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOGGING_LEVEL = logging.INFO
logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

#####
# Default values for CLI
# Note that all defaults are set in the CLI and tha
# functions in "cliexec" should be required
#
#####
DEFAULT_YEAR = 2022
DEFAULT_MONTH = 12
DEFAULT_RESOLUTION = 5
DEFAULT_NUM_NEIGHBORS = 3
DEFAULT_POWER = 2

#####
# FUNCTIONS
#####

def initialize(parser: argparse.ArgumentParser):

    args = parser.parse_args()

    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })

    cliexec.initialize(args.database_dir)


def addmeta(parser: argparse.ArgumentParser):
    args = parser.parse_args()
    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })

    value_cols = json.loads(args.value_columns)
    key_cols = json.loads(args.key_columns)

    res = cliexec.add_meta(
        args.database_dir,
        args.dataset_name,
        args.description,
        key_cols,
        value_cols,
        args.dataset_type
    )

    print(f"Created Metadata Entry for {res}")


def showmeta(args: argparse.Namespace):
    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })
    out = cliexec.show_meta()

    logger.info(f"retrieved metadata: {out}")
    out_str = json.dumps(out, ensure_ascii=False)
    return out_str


def show(parser: argparse.ArgumentParser):
    start_time = time.time()

    args = parser.parse_args()

    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })

    if not args.cell \
            and not args.latitude \
            and not args.longitude\
            and not args.shapefile:
        usage(parser, "Missing 'cell | latitude |"
                      " longitude | shapefile' parameter")
        sys.exit(1)

    if args.latitude and not args.longitude:
        usage(parser, "Missing 'longitude' parameter")
        sys.exit(1)

    if args.longitude and not args.latitude:
        usage(parser, "Missing 'latitude' parameter")
        sys.exit(1)

    if args.cell:
        if args.radius:
            output = cliexec.show_cell_radius(
                args.dataset,
                args.cell,
                args.radius,
                args.year,
                args.month,
                args.day,
                args.type
            )
        else:
            output = cliexec.show_cell_point(
                args.dataset,
                args.cell,
                args.year,
                args.month,
                args.day,
                args.type
            )
    elif args.shapefile:
        output = cliexec.show_shapefile(
            args.dataset,
            args.shapefile,
            args.region,
            args.resolution,
            args.year,
            args.month,
            args.day,
            args.type
        )
    else:
        if args.radius:
            output = cliexec.show_latlong_radius(
                args.dataset,
                args.latitude,
                args.longitude,
                args.radius,
                args.resolution,
                args.year,
                args.month,
                args.day,
                args.type
            )
        else:
            output = cliexec.show_latlong_point(
                args.dataset,
                args.latitude,
                args.longitude,
                args.resolution,
                args.year,
                args.month,
                args.day
            )

    end_time = time.time()
    diff = end_time - start_time
    logger.info(f"show commmand time taken: {diff} seconds")

    print(json.dumps(output, indent=2))

def filter(parser: argparse.ArgumentParser):
    args = parser.parse_args()
    logger.info(f"Using shapefile:{args.shapefile}")
    logger.info(f"Using resolution:{args.resolution}")
    logger.info(f"Using tolerance:{args.tolerance}")

    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })

    if not args.shapefile:
        usage(parser, "Missing 'shapefile' parameter")
        sys.exit(1)

    output = cliexec.filter(args.shapefile, int(args.resolution), float(args.tolerance))
    print(json.dumps(output, indent=2))

def filter_assets(args: argparse.Namespace):
    logger.info(f"Using asset file:{args.asset_file}")
    logger.info(f"Using dataset file:{args.dataset_file}")

    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.filter_assets(
        args.asset_file,
        args.dataset_file
    )

    logger.info(f"{len(output['data'])} rows returned")

    cols = output["columns"]
    outstr = "columns: " + json.dumps(cols, ensure_ascii=False)
    data = output["data"]
    outstr += "\n\n"

    num_rows = args.return_rows
    if num_rows > 0:
        num_rows_str = f"{num_rows}"
        data_to_show = data[0:args.return_rows + 1]
    else:
        num_rows_str = "all"
        data_to_show = data

    outstr += f"data (showing {num_rows_str} rows):" +\
              json.dumps(data_to_show, ensure_ascii=False)
    return outstr

def visualize(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecGeospatial({
        "host": args.host,
        "port": int(args.port)
    })

    if not args.cells_path:
        usage(parser, "Missing 'cells_path' parameter")
        sys.exit(1)

    output = cliexec.visualize(args.cells_path, args.map_path)
    print(json.dumps(output, indent=2))

def usage(parser:any, msg: str):
    print(f"Error: {msg}\n")
    parser.print_help()

def add_meta_parser(
    subparsers
):
    # Parser for 'filter' command
    meta_parser = subparsers.add_parser(
        "addmeta", help="Add a metadata entry,"
                        " allowing a dataset to be accessed")
    meta_parser.add_argument(
        "--database_dir",
        help="the directory where databases are stored/created",
        required=True
    )
    meta_parser.add_argument(
        "--dataset_name",
        help="The name by which the dataset will be referred and accessed",
        required=True
    )
    meta_parser.add_argument(
        "--description",
        help="A description of what the dataset contains",
        required=True
    )
    meta_parser.add_argument(
        "--value_columns",
        help="A JSON object mapping value column name to data type. "
             "Data type must be a valid Duckdb General Purpose data type.",
        required=True
    )
    meta_parser.add_argument(
        "--key_columns",
        help="A JSON object mapping key column name to data type. "
             "Data type must be a valid Duckdb General Purpose data type.",
        required=True
    )
    meta_parser.add_argument(
        "--dataset_type",
        help="The type of dataset. Currently supported types are [h3, point]",
        required=True
    )

def show_meta_parser(
    subparsers
):
    # Parser for 'filter' command
    meta_parser = subparsers.add_parser(
        "showmeta", help="show available meta entries")

def add_initialize_parser(
    subparsers
):
    # Parser for initialize
    initialize_parser = subparsers.add_parser(
        "initialize", help="create source db from giss temperature data"
    )
    initialize_parser.add_argument(
        "--stations_path", help="The path to the giss station list"
    )
    initialize_parser.add_argument(
        "--temperatures_path", help="The path to the giss temperature data"
    )
    initialize_parser.add_argument(
        "--database_dir", help="The path where the database will be created"
    )

def add_show_parser(
    subparsers
):
    # Parser for get_temp_cell
    show_parser = subparsers.add_parser(
        "show",
        help="Show geospatial data"
    )
    show_parser.add_argument(
        "--dataset",
        help="the name of the data type to return (for example, 'temperature')",
        required=True
    )
    show_parser.add_argument(
        "--cell",
        help="The h3 cell that will be the center of the returned data",
        required=False
    )
    show_parser.add_argument(
        "--shapefile",
        help="The shapefile that defines the area data is to be returned for",
        required=False,
        type=str
    )
    show_parser.add_argument(
        "--latitude",
        help="The latitude around which data should be retrieved",
        required=False,
        type=float
    )
    show_parser.add_argument(
        "--longitude",
        help="The longitude around which data should be retrieved",
        required=False,
        type=float
    )
    show_parser.add_argument(
        "--region",
        help="Region within a shapefile for which data should be returned",
        type=str,
        default=None
    )
    show_parser.add_argument(
        "--radius",
        help="The radius around which data should be retrieved",
        required=False,
        type=float
    )
    show_parser.add_argument(
        "--resolution",
        help="The h3 resolution level to retrieve for",
        required=False,
        type=int
    )
    show_parser.add_argument(
        "--year",
        help="The year data is to be selected for.",
        required=False,
        type=int
    )
    show_parser.add_argument(
        "--month",
        help="The month the data is selected from.",
        required=False,
        type=int
    )
    show_parser.add_argument(
        "--day",
        help="The month the data is selected from.",
        required=False,
        type=int
    )
    show_parser.add_argument(
        "--type",
        help="What type of dataset is being queried",
        required=False,
        type=str,
        default="h3"
    )


def add_filter_parser(
        subparsers
):
    # Parser for 'filter' command
    filter_parser = subparsers.add_parser(
        "filter", help="Filter shape file for just land H3 cells")
    filter_parser.add_argument(
        "--shapefile", required=True, help="Path to shape file (.shp)")
    filter_parser.add_argument(
        "--resolution", help="H3 resolution (0-12, default: 0)")
    filter_parser.add_argument(
        "--tolerance",
        help="Shapefile simplification (0.01-0.1, default: 0.1")

def add_filter_assets_parser(
    subparsers
):
    f_asset_parser = subparsers.add_parser(
        "filter-assets",
        help="Filter assets based on conditions in specified datasets")
    f_asset_parser.add_argument(
        "--asset-file", required=True, help="Path to asset file")
    f_asset_parser.add_argument(
        "--dataset-file", required=True, help="Path to dataset file")
    f_asset_parser.add_argument(
        "--return-rows", required=False, type=int,
        default=2, help="How many rows of the result to return."
                        " Defaults to 2."
                        " To return all rows set to -1")

def add_visualize_parser(
    subparsers
):
    # Parser for 'visualize' command
    visualize_parser = subparsers.add_parser(
        "visualize", help="Visualized maps and overlays")
    visualize_parser.add_argument(
        "--cells_path", required=True, help="Path to H3 cells")
    visualize_parser.add_argument(
        "--map_path", required=True, help="Path for map output")

def add_viualize_dataset_parser(
    subparsers
):
    visualize_parser = subparsers.add_parser(
        "visualize-dataset", help="Visualized maps and overlays")
    visualize_parser.add_argument(
        "--database-dir", required=True,
        type=str,
        help="The directory in which databases are stored"
    )
    visualize_parser.add_argument(
        "--dataset", required=True,
        help="The name of the dataset to visualize"
             "must be a dataset registered in the metadata")
    visualize_parser.add_argument(
        "--resolution", required=True,
        type=int,
        help="The h3 resolution level to display data for"
    )
    visualize_parser.add_argument(
        "--value-column", required=True,
        type=str,
        help="The column of data which is to be visualized"
    )
    visualize_parser.add_argument(
        "--max-color", required=True,
        nargs=3,
        type=int,
        help="The h3 resolution level to display data for"
    )
    visualize_parser.add_argument(
        "--output-file", required=True,
        type=str,
        help="The file where the visualized map will be stored"
    )
    visualize_parser.add_argument(
        "--min-lat", required=False,
        type=float,
        help="The minimum latitude to display"
    )
    visualize_parser.add_argument(
        "--max-lat", required=False,
        type=float,
        help="The maximum latitude to display"
    )
    visualize_parser.add_argument(
        "--min-long", required=False,
        type=float,
        help="The minimum longitude to display"
    )
    visualize_parser.add_argument(
        "--max-long", required=False,
        type=float,
        help="The maximum longitude to display"
    )
    visualize_parser.add_argument(
        "--year", required=False,
        type=float,
        help="The year to display data for"
    )
    visualize_parser.add_argument(
        "--month", required=False,
        type=float,
        help="The month to display data for"
    )
    visualize_parser.add_argument(
        "--day", required=False,
        type=float,
        help="The day to display data for"
    )
    visualize_parser.add_argument(
        "--threshold", required=False,
        type=float,
        help="A ratio of data points relative to max and min values. Only"
             "cells where the data point is greater than threshold"
             "will be displayed. Scaled relative to min and max value"
    )
    visualize_parser.add_argument(
        "--ds-type", required=False,
        type=str,
        default="h3",
        help="the type of ds to process. acceptable values: [h3, point]"
    )
    visualize_parser.add_argument(
        "--visualizer-type", required=False,
        type=str,
        default="HexGridVisualizer",
        help="the type of ds to process. acceptable values: ["
             "HexGridVisualizer, PointLocationVisualizer]"
    )


def execute(xargs=None):
    """
    Main function that sets up the argparse CLI interface.
    """

    # Initialize argparse and set general CLI description
    parser = argparse.ArgumentParser(description="Data Mesh Agent Command Line Interface (CLI)")

    # Parser for top-level commands
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument("--host", required=True, help="Server host")
    parser.add_argument("--port", required=True, help="Server port")

    # Create subparsers to handle multiple commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    add_meta_parser(subparsers)
    show_meta_parser(subparsers)
    add_filter_parser(subparsers)
    add_filter_assets_parser(subparsers)
    add_visualize_parser(subparsers)
    add_viualize_dataset_parser(subparsers)

    add_initialize_parser(subparsers)
    add_show_parser(subparsers)

    args = parser.parse_args(xargs if xargs is not None else sys.argv[1:])
    logger.info(args)
    # print(args)

    # Set up logging
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=logging_format, level=logging.INFO if args.verbose else logging.WARNING)

    out_str = None
    # Execute corresponding function based on provided command
    if args.command == "filter":
        filter(parser)
    elif args.command == "filter-assets":
        out_str = filter_assets(args)
    elif args.command == "visualize":
        visualize(parser)
    elif args.command == "addmeta":
        addmeta(parser)
    elif args.command == "showmeta":
        out_str = showmeta(args)
    elif args.command == "initialize":
        initialize(parser)
    elif args.command == "show":
        show(parser)
    else:
        usage(parser, "Command missing - please provide command")

    if out_str is not None:
        print(out_str)
    return out_str

if __name__ == "__main__":

    # Execute mainline
    execute(sys.argv[1:])
