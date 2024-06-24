# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
import os.path
from typing import Dict

from shape import Shape

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


# Abstract class
class CliExecShapefile:

    def __init__(self, config: dict):
        """Create CLI"""
        logger.info(f"Using config:{config}")
        self.host = config["host"]
        self.port = config["port"]

    #####
    # FILTERING
    #####

    def transform(self, shapefile: str) -> str | None:
        s = Shape(shapefile)
        s.transform_to_epsg_4326()
        base_name = os.path.splitext(shapefile)[0]

        # EPSG:4326 is based off of the WGS84 ellipsoid, hence the naming here
        wgs84_path = base_name + "-wgs84.shp"
        s.save(path=wgs84_path)

        return wgs84_path



    def view(self, shapefile: str, path: str) -> str | None:
        s = Shape(shapefile)
        result = s.view(path)
        return result

    def statistics(self, shapefile: str) -> Dict[str, int | float]:
        s = Shape(shapefile)
        result = s.statistics()
        return result

    def simplify(
            self,
            shapefile: str,
            tolerance: float=0.1,
            path: str=None
    ) -> Dict[str, int | float]:
        s = Shape(shapefile)
        s.simplify(tolerance)
        if path:
            s.save(path)
        return s.statistics()

    def buffer(
            self,
            shapefile: str,
            distance: float,
            units: str="degrees",
            path: str=None
    ) -> Dict[str, int | float]:
        s = Shape(shapefile)
        s.buffer(distance, units)
        if path:
            s.save(path)
        return s.statistics()


