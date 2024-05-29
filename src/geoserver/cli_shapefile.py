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
from functools import partial

from cliexec_shapefile import CliExecShapefile

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


def transform(parser: argparse.ArgumentParser):
    args = parser.parse_args()
    logger.info(f"Using shapefile:{args.shapefile}")

    cliexec = CliExecShapefile({
        "host": args.host,
        "port": int(args.port)
    })

    if not args.shapefile:
        usage(parser, "Missing 'shapefile' parameter")
        sys.exit(1)

    output = cliexec.transform(args.shapefile)
    print(json.dumps(output, indent=2))


def statistics(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecShapefile({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.statistics(args.shapefile)
    print(json.dumps(output, indent=2))

def simplify(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecShapefile({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.simplify(args.shapefile, args.tolerance, args.path)
    print(json.dumps(output, indent=2))

def buffer(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecShapefile({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.buffer(args.shapefile, args.distance, args.units, args.path)
    print(json.dumps(output, indent=2))

def view(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecShapefile({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.view(args.shapefile, args.path)
    print(json.dumps(output, indent=2))

def usage(parser:any, msg: str):
    print(f"Error: {msg}\n")
    parser.print_help()


def add_transform_parser(
    subparsers
):
    # Parser for 'transform' command
    transform_parser = subparsers.add_parser(
        "transform", help="Transform shapefiles to EPSG 4326 (lat/lon)")
    transform_parser.add_argument(
        "--shapefile", required=True, help="Path to shape file (.shp)")

def add_statistics_parser(
    subparsers
):
    # Parser for 'statistics' command
    statistics_parser = subparsers.add_parser(
        "statistics", help="Show statistics for a shapefile")
    statistics_parser.add_argument(
        "--shapefile", required=True, help="Path to shape file (.shp)")

def add_simplify_parser(
    subparsers
):
    # Parser for 'simplify' command
    simplify_parser = subparsers.add_parser(
        "simplify", help="Simplify a shapefile (this will reduce number of polygons)")
    simplify_parser.add_argument(
        "--shapefile", required=True, help="Path to shape file (.shp)")
    simplify_parser.add_argument(
        "--tolerance", help="Shapefile simplification (0.01-0.1, default: 0.1)",
        type=float)
    simplify_parser.add_argument(
        "--path", help="Path to saved shapefile to (.shp)")

def add_buffer_parser(
    subparsers
):
    # Parser for 'buffer' command
    buffer_parser = subparsers.add_parser(
        "buffer", help="Simplify a shapefile (this will reduce number of polygons)")
    buffer_parser.add_argument(
        "--shapefile", required=True, help="Path to shape file (.shp)")
    buffer_parser.add_argument(
        "--distance", help="Buffer distance (0.01-1.0, default: 1.0)",
        type=float)
    buffer_parser.add_argument(
        "--units", help="Buffer distance units (degrees|meters, default:degrees)")
    buffer_parser.add_argument(
        "--path", help="Path to saved shapefile to (.shp)")

def add_view_parser(
    subparsers
):
    # Parser for 'view' command
    view_parser = subparsers.add_parser(
        "view", help="View a raw shapefile")
    view_parser.add_argument(
        "--shapefile", required=True, help="Path to shape file (.shp)")
    view_parser.add_argument(
        "--path", help="Path to saved output (.html)")

def execute():
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

    add_transform_parser(subparsers)
    add_statistics_parser(subparsers)
    add_simplify_parser(subparsers)
    add_buffer_parser(subparsers)
    add_view_parser(subparsers)

    args = parser.parse_args()
    logger.info(args)
    # print(args)

    # Set up logging
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=logging_format, level=logging.INFO if args.verbose else logging.WARNING)

    # Execute corresponding function based on provided command
    if args.command == "transform":
        transform(parser)
    elif args.command == "statistics":
        statistics(parser)
    elif args.command == "simplify":
        simplify(parser)
    elif args.command == "buffer":
        buffer(parser)
    elif args.command == "view":
        view(parser)
    else:
        usage(parser, "Command missing - please provide command")


if __name__ == "__main__":

    # Execute mainline
    execute()
