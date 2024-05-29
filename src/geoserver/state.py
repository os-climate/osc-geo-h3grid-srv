# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
from typing import Any

global_state = {}


def add_global(key: str, val: Any):
    global_state[key] = val

def get_global(key: str) -> Any:
    return global_state[key]

def remove_global(key: str):
    del global_state[key]
