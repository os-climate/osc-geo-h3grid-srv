# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
from setuptools import setup, find_packages

setup(
    name='geoserver',
    version='0.1',
    description='A server for handling geospatial data',
    author='Davis Broda',
    author_email='davis.broda@gmail.com',
    packages=find_packages(exclude=["tests", "tests.*"])
)
