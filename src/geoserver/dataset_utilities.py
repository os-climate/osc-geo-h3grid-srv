# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-05-22 by davis.broda@brodagroupsoftware.com
import re

from geoserver.bgsexception import InvalidArgumentException


def get_point_res_col(resolution: int) -> str:
    if resolution > 15 or resolution < 0:
        raise InvalidArgumentException(
            f"resolution must be between 0 and 15. resolution was: {resolution}"
        )

    return f"res{resolution}"


def col_name_is_point_res_col(col: str) -> bool:

    match_1_digit = re.match("res[0-9]", col) is not None
    match_2_digit = re.match("res1[0-5]", col) is not None

    return match_1_digit or match_2_digit


