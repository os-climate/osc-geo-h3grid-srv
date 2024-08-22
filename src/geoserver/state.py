# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
import multiprocessing
import os
from typing import Any, Dict

global_state: Dict = {}


def add_global(key: str, val: Any) -> None:
    global global_state
    global_state[key] = val


def get_global(key: str) -> Any:
    if key == "database_dir":
        return os.environ["GEOSERVER_DATABASE_DIR"]
    global global_state
    return global_state[key]


def remove_global(key: str):
    global global_state
    del global_state[key]
