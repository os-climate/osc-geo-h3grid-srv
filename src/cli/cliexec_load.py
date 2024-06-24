# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
import os.path

from loader.loader_factory import LoaderFactory

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class CliExecLoad:

    def __init__(self, config: dict):
        """Create CLI"""
        logger.info(f"Using config:{config}")
        self.host = config["host"]
        self.port = config["port"]

    def initialize(self, database_dir: str):
        if not os.path.exists(database_dir):
            os.makedirs(database_dir)
            logger.info(f"created database dir {database_dir}")
        else:
            logger.info(f"database dir {database_dir} already exists")

    def load(
            self,
            config_path: str
    ):
        loader = LoaderFactory.create_loader(config_path)
        loader.load()

