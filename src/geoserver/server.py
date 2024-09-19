# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import argparse
import logging
import os

import uvicorn as uvicorn
import yaml
from fastapi import FastAPI

import state
from geoserver.geomesh_router import router as geomesh_router
from geoserver.point_router import router as point_router

from multiprocessing import Manager

# Set up logging
LOGGING_FORMAT = \
    "%(asctime)s - %(module)s:%(funcName)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

# Set up server
app = FastAPI()
app.include_router(geomesh_router)
app.include_router(point_router)

DEFAULT_CONFIG = "./config/config.yml"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000

# TODO: move this into a configuration file
# database_dir = "../databases"

if __name__ == "__main__":
    logger.info("Server starting...")

    parser = argparse.ArgumentParser(description="Run the FastAPI server.")
    parser.add_argument("--configuration", default=DEFAULT_CONFIG,
                        help=f"Configuration file (default: {DEFAULT_CONFIG})")


    args = parser.parse_args()
    logger.info(f"using config file {args.configuration}")

    logger.info(f"Running with args: {args}")

    with open(args.configuration, 'r') as file:
        configuration = yaml.safe_load(file)

    logger.info(f"Using configuration:{configuration}")

    host = configuration['host']
    port = configuration['port']

    database_dir = configuration['database-dir']

    # state.add_global("database_dir", database_dir)
    os.environ["GEOSERVER_DATABASE_DIR"] = database_dir

    uvicorn.run(app, host=host, port=port)

    # TODO: load config from config file
