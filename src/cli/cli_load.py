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

from cliexec_load import CliExecLoad

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

    cliexec = CliExecLoad({
        "host": args.host,
        "port": int(args.port)
    })

    cliexec.initialize(args.database_dir)

def load(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecLoad({
        "host": args.host,
        "port": int(args.port)
    })

    if not args.config_path:
        usage(usage, "Missing config_path parameter")
        sys.exit(1)

    return cliexec.load(args.config_path)

def load_pipeline(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecLoad({
        "host": args.host,
        "port": int(args.port)
    })

    if not args.config_path:
        usage(usage, "Missing config_path parameter")
        sys.exit(1)
    return cliexec.load_pipeline(args.config_path)


def usage(parser:any, msg: str):
    print(f"Error: {msg}\n")
    parser.print_help()

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

def add_load_parser(
        subparsers
):
    # Parser for interpolate
    load = subparsers.add_parser(
        "load",
        help="load a dataset into the geospatial dataset"
    )
    load.add_argument(
        "--config_path",
        help="the location of the config file that controls the process",
        required=True
    )

def add_load_pipeline_parser(
        subparsers
):
    load = subparsers.add_parser(
        "load-pipeline",
        help="run a loading pipeline for customizable data loading"
    )
    load.add_argument(
        "--config_path",
        help="the location of the config file that controls the pipeline",
        required=True
    )

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

    add_load_parser(subparsers)
    add_initialize_parser(subparsers)
    add_load_pipeline_parser(subparsers)

    args = parser.parse_args()
    logger.info(args)
    # print(args)

    # Set up logging
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=logging_format, level=logging.INFO if args.verbose else logging.WARNING)

    # Execute corresponding function based on provided command
    if args.command == "initialize":
        initialize(parser)
    elif args.command == "load":
        load(parser)
    elif args.command == "load-pipeline":
        load_pipeline(parser)
    else:
        usage(parser, "Command missing - please provide command")


if __name__ == "__main__":

    # Execute mainline
    execute()