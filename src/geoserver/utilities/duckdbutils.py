# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
from typing import Dict, Tuple, Optional

import duckdb

from geoserver.bgsexception import InvalidColumnTypeException

# The official names, aliases not needed
GENERAL_PURPOSE_DATA_TYPES = [
    "BIGINT",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "DATE",
    "DECIMAL",  # TODO: figure out if percision and scale can be handled
    "DOUBLE",
    "HUGEINT",
    "INTEGER",
    "INTERVAL",
    "REAL",
    "SMALLINT",
    "TIME",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "UUID",
    "VARCHAR"
]

COMPOSITE_TYPES = [
    "ARRAY",
    "LIST",
    "MAP",
    "STRUCT",
    "UNION"
]

# mapping of ALIAS -> UNDERLYING TYPE
TYPE_ALIASES = {
    "INT8": "BIGINT",
    "LONG": "BIGINT",
    "BITSTRING": "BIT",
    "BYTEA": "BLOB",
    "BINARY": "BLOB",
    "VARBINARY": "BLOB",
    "BOOL": "BOOLEAN",
    "LOGICAL": "BOOLEAN",
    # TODO: figure out if precision and scale can be handled
    "NUMERIC": "DECIMAL",
    "FLOAT8": "DOUBLE",
    "INT4": "INTEGER",
    "INT": "INTEGER",
    "SIGNED": "INTEGER",
    "FLOAT4": "REAL",
    "FLOAT": "REAL",
    "INT2": "SMALLINT",
    "SHORT": "SMALLINT",
    "TIMESTAMPZ": "TIMESTAMP WITH TIME ZONE",
    "DATETIME": "TIMESTAMP",
    "INT1": "TINYINT",
    "CHAR": "VARCHAR",
    "BPCHAR": "VARCHAR",
    "TEXT": "VARCHAR",
    "STRING": "VARCHAR"
}


def duckdb_check_table_exists(
        connection: duckdb.DuckDBPyConnection,
        tablename: str
) -> bool:
    """
    Checks whether a table exists in the provided database

    :param connection: connection to the database to check for the table
    :type connection: duckdb.DuckDBPyConnection
    :param tablename: the name of the table
    :type tablename: str
    :return: True if table exists, Falst otherwise
    :rtype: bool
    """
    cur = connection.cursor()
    sql = """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = ?
    """
    res = cur.execute(sql, [tablename]).fetchone()
    if res[0] > 0:
        cur.close()
        return True
    else:
        cur.close()
        return False


def is_general_col_type(col_type:str) -> Tuple[bool, Optional[str]]:
    """
    Determines whether a given column type is a valid general purpose
    type available to duckdb.

    :param col_type: Column type to validate
    :type col_type: Dict[str,str]
    :return:
        A tuple of two values:
            1st: true if type is valid, false if invalid
            2nd: empty if type is valid, an error message if invalid
    :rtype: Tuple[bool, str]
    """
    is_valid = False
    errmsg = None

    type_upper = col_type.upper()

    if type_upper in GENERAL_PURPOSE_DATA_TYPES:
        is_valid = True
    elif type_upper in COMPOSITE_TYPES:
        is_valid = False
        errmsg = f"column type is composite, not general purpose: {col_type}"
    elif type_upper in TYPE_ALIASES.keys():
        is_valid = True
    else:
        is_valid = False
        errmsg = f"unrecognized type: {col_type}"

    return is_valid, errmsg


def convert_to_cannonical_type(col_type: str) -> str:
    type_upper = col_type.upper()

    if type_upper in GENERAL_PURPOSE_DATA_TYPES:
        out = type_upper
    elif type_upper in COMPOSITE_TYPES:
        out = type_upper
    elif type_upper in TYPE_ALIASES.keys():
        out = TYPE_ALIASES[type_upper]
    else:
        raise InvalidColumnTypeException(
            f"column type: {col_type} is not a known duckdb type")

    return out