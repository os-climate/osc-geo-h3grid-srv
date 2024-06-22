# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
import os
import shutil

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

class Repository():
    """
    This repository is temporarily implemented as a local directory.
    This capability is expected to be eventually used in a
    microservice (the microservice will be accessed via REST endpoints
    which call these functions).
    """

    def __init__(
            self,
            repository: str
    ):
        """
        Initialize class

        :param shapefile:
            The path to the shapefile
        """

        self.repository = repository
        self.connection = None

    def register(self, name: str, contents: str):
        """
        Store the shapefile (and its constituent parts) in
        a repository, which currently a file directory; The
        name is the directory where the contents of the
        shapefile and its parts are stored. The contents is
        ZIP file of the shapefile directory.
        """

        # Target directory is a concatentation of the
        # repository (a directory) and the name (the directory
        # will be created if it does not exist
        target_directory = os.path.join(self.repository, name)

        # Check if the directory already exists. If it does
        # exist then raise an exception
        if os.path.isdir(target_directory):
            msg = f"Shapefile name:{name} already registered in the repository:{self.repository}"
            raise ValueError(msg)

        # Open the zip file in read mode and extract all
        # contents into the target directory
        import zipfile
        with zipfile.ZipFile(contents, 'r') as zip_ref:
            zip_ref.extractall(target_directory)

        output = {
            "status": "successful"
        }
        return output

    def unregister(self, name: str):
        """
        Remove the named shapefile and its contents
        from the repository.
        """

        # Target directory is a concatentation of the
        # repository (a directory) and the name (the directory
        # will be created if it does not exist
        target_directory = os.path.join(self.repository, name)

        # Check if the directory exists and if it exists
        # then remove the directory and all its contents
        if os.path.exists(target_directory):
            shutil.rmtree(target_directory)
        else:
            msg = f"Shapefile name:{name} is not registered in the repository:{self.repository}"
            raise ValueError(msg)

        output = {
            "status": "successful"
        }
        return output


    def inventory(self):
        """
        Get a list of all named shapefiles in the repository.
        """
        entries = os.listdir(self.repository)
        directories = [entry for entry in entries if os.path.isdir(os.path.join(self.repository, entry))]

        return directories

