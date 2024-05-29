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
import json
from cliexec_repository import CliExecRepository

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOGGING_LEVEL = logging.INFO
logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

#####
# FUNCTIONS
#####

def register(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecRepository({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.register(args.repository, args.name, args.contents)
    print(json.dumps(output, indent=2))

def unregister(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecRepository({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.unregister(args.repository, args.name)
    print(json.dumps(output, indent=2))

def inventory(parser: argparse.ArgumentParser):
    args = parser.parse_args()

    cliexec = CliExecRepository({
        "host": args.host,
        "port": int(args.port)
    })

    output = cliexec.inventory(args.repository)
    print(json.dumps(output, indent=2))

def usage(parser:any, msg: str):
    print(f"Error: {msg}\n")
    parser.print_help()

def add_register_parser(
    subparsers
):
    # Parser for 'register' command
    register_parser = subparsers.add_parser(
        "register", help="Register a named shapefile")
    register_parser.add_argument(
        "--repository", required=True, help="Connection string for the repository")
    register_parser.add_argument(
        "--name", required=True, help="Registered name for the shapefile in the repository")
    register_parser.add_argument(
        "--contents", help="ZIP file of the shapefile directory")

def add_unregister_parser(
    subparsers
):
    # Parser for 'unregister' command
    unregister_parser = subparsers.add_parser(
        "unregister", help="Unregister a named shapefile")
    unregister_parser.add_argument(
        "--repository", required=True, help="Connection string for the repository")
    unregister_parser.add_argument(
        "--name", required=True, help="Registered name for the shapefile in the repository")

def add_inventory_parser(
    subparsers
):
    # Parser for 'inventory' command
    inventory_parser = subparsers.add_parser(
        "inventory", help="Show inventory of shapefiles in the repository")
    inventory_parser.add_argument(
        "--repository", required=True, help="Connection string for the repository")

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

    add_register_parser(subparsers)
    add_unregister_parser(subparsers)
    add_inventory_parser(subparsers)

    args = parser.parse_args()
    logger.info(args)
    # print(args)

    # Set up logging
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=logging_format, level=logging.INFO if args.verbose else logging.WARNING)

    # Execute corresponding function based on provided command
    if args.command == "register":
        register(parser)
    elif args.command == "unregister":
        unregister(parser)
    elif args.command == "inventory":
        inventory(parser)
    else:
        usage(parser, "Command missing - please provide command")

if __name__ == "__main__":

    # Execute mainline
    execute()