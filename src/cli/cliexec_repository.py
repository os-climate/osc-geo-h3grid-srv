# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging

from shape.repository import Repository

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

class CliExecRepository:

    def __init__(self, config: dict):
        """Create CLI"""
        logger.info(f"Using config:{config}")
        self.host = config["host"]
        self.port = config["port"]

    def register(self, repository: str, name: str, contents: str):
        logger.info(f"Registering repository:{repository} name:{name} contents:{contents}")
        r = Repository(repository)
        result = r.register(name, contents)
        return result

    def unregister(self, repository: str, name: str):
        logger.info(f"Unregistering repository:{repository} name:{name}")
        r = Repository(repository)
        result = r.unregister(name)
        return result

    def inventory(self, repository: str, ):
        logger.info(f"Getting inventory repository:{repository}")
        r = Repository(repository)
        result = r.inventory()
        return result


