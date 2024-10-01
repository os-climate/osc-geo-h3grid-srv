# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-09-11 by 15205060+DavisBroda@users.noreply.github.com
import logging
import os.path
from typing import Dict, Any, List, Tuple

import duckdb
import h3
import pandas
from duckdb import DuckDBPyConnection
from pandas import DataFrame

from common import const, duckdbutils
from .geomesh_router_arguments import LocatedAsset, DatasetArg, \
    AssetFilter

CELL_COL_PREFIX = "cell_"
INDEX_PREFIX = "index_"
ASSET_TABLE_NAME = "assets"

logging.basicConfig(level=logging.INFO, format=const.LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class Correlator:

    def __init__(
            self,
            database_dir: str,
            resolution: int = 7
    ):
        self.database_dir = database_dir
        if not os.path.exists(database_dir):
            logger.error("provided database directory does not exist."
                         f" database_dir: {database_dir}")
        if not os.path.isdir(database_dir):
            logger.error("provided database directory is not a directory."
                         f" database_dir: {database_dir}")

        self.resolution = resolution

    def get_correlated_data(
            self,
            assets: pandas.DataFrame,
            datasets: List[DatasetArg]
    ):
        logger.info(f"Creating temporary assets database")
        asset_db = self._create_asset_db(assets)
        logger.info(f"Correlating assets")
        results = self._correlate_data(asset_db, datasets)
        logger.info(f"returning {len(results['data'])} rows")
        return results

    def _create_asset_db(
            self,
            assets: pandas.DataFrame,
    ) -> DuckDBPyConnection:
        self._add_cells(assets, [7])
        conn = duckdb.connect(database=':memory:')

        logger.debug(f"Creating table {ASSET_TABLE_NAME}")
        conn.execute(
            f"CREATE TABLE {ASSET_TABLE_NAME} AS SELECT * FROM assets")

        res_col = f"{CELL_COL_PREFIX}{self.resolution}"
        index_res_column = f"{INDEX_PREFIX}{CELL_COL_PREFIX}_{self.resolution}"
        # Create a non-unique index on the res9 column

        logger.debug(f"Creating index for {ASSET_TABLE_NAME}")
        statement = f"""
            CREATE INDEX {index_res_column} ON {ASSET_TABLE_NAME} ({res_col})
        """
        conn.execute(statement)

        return conn

    def _add_cells(self, df: pandas.DataFrame,
                   resolutions: List[int]) -> pandas.DataFrame:
        logger.info("attaching cells to data")
        # Extract the latitude and longitude columns once for better performance
        #  As numpy is much faster than pandas
        latitudes = df['lat'].to_numpy()
        longitudes = df['long'].to_numpy()
        for res in resolutions:
            df[f"{CELL_COL_PREFIX}{res}"] = [
                h3.geo_to_h3(lat, lon, res) for lat, lon in
                zip(latitudes, longitudes)
            ]
        logger.info("done attaching cells to data")
        return df

    def _correlate_data(
            self,
            asset_db: DuckDBPyConnection,
            datasets: List[DatasetArg]
    ) -> Dict:  # TODO: better return type

        logger.debug(f"Correlating data")
        filter_strings = []
        filter_vals = []
        db_tables = []
        target_vals = []
        for dataset in datasets:
            logger.debug(f"Using dataset:{dataset}")
            db_path = self._get_database_path(dataset.name)
            logger.debug(f"Using db_path:{db_path}")

            conn = duckdb.connect(db_path)
            table_name = self._get_table_from_dataset(dataset.name)
            if not duckdbutils.duckdb_check_table_exists(conn, table_name):
                raise ValueError(
                    f"table {table_name} for dataset {dataset} "
                    f"does not exist")
            db_tables.append(f"{dataset.name}.{dataset.name}")
            for this_filter in dataset.filters:
                filter_s, filter_v = self._get_filter_string(
                        conn, table_name, this_filter
                )
                filter_strings.append(filter_s)
                filter_vals.append(filter_v)
                target_vals.append(this_filter.target_value)
            conn.close()

        full_filter = self._assemble_full_where(filter_strings)

        for dataset in datasets:
            db_path = self._get_database_path(dataset.name)
            self.attach_database(asset_db, dataset.name, db_path)

        logger.info(f"Assembling correlation"
                    f" resolution: {self.resolution}"
                    f" db_tables:{db_tables}")
        full_sql = self._assemble_correlation_query(
            self.resolution,
            db_tables,
            full_filter
        )

        logger.info(f"Executing correlation SQL")
        result_obj = asset_db.execute(full_sql, target_vals)
        result_description = result_obj.description
        result_columns = list(map(
            lambda tup: tup[0],
            result_description
        ))
        logger.info(f"fetching all...")
        results_raw = result_obj.fetchall()
        logger.info(f"fetching all... DONE")

        return self._output_to_json(results_raw, result_columns)

    def attach_database(
            self,
            database: DuckDBPyConnection,
            dataset_name: str,
            db_path: str) -> None:

        logger.info(f"attaching dataset {dataset_name} to asset database")
        attach_sql = f"ATTACH DATABASE '{db_path}' as {dataset_name}"
        database.execute(attach_sql).fetchall()

    def _get_filter_string(
            self,
            conn: DuckDBPyConnection,
            table_name: str,
            table_filter: AssetFilter
    ) -> Tuple[str, float]:
        """

        :param conn:
        :type conn:
        :param table_name:
        :type table_name:
        :param table_filter:
        :type table_filter:
        :return:
            A tuple of parameterized filter string,
            and the value that goes in the parameter
        :rtype:
        """
        col_exists = duckdbutils.duckdb_check_column_exists(
            conn, table_name, table_filter.column)
        if not col_exists:
            raise ValueError(f"Column {table_filter.column} does not exist"
                             f" in table {table_name}" )
        sql = f"({table_name}.{table_filter.column} "
        match table_filter.filter_type:
            case "greater_than":
                sql += "> "
            case "greater_than_or_equal":
                sql += ">= "
            case "lesser_than":
                sql += "<"
            case "lesser_than_or_equal":
                sql += "<= "
            case "equal_to":
                sql += "= "
            case _:
                raise ValueError(f"unreecognized filter type:"
                                 f" {table_filter.filter_type}")

        sql += f" ? "

        sql += f"OR {table_name}.{table_filter.column} IS NULL)"

        return sql, table_filter.target_value

    def _assemble_full_where(self, filter_strings: List[str]) -> str:
        if len(filter_strings) == 0:
            return ";"
        filters = " AND ".join(filter_strings)
        return f"WHERE {filters} ;"

    def _get_database_path(self, dataset_name: str):
        return self.database_dir + "/" + dataset_name + ".duckdb"

    def _get_table_from_dataset(self, dataset_name: str) -> str:
        return dataset_name

    def _assemble_correlation_query(
            self,
            resolution: int,
            db_tables: List[str],
            full_where_clause: str
    ) -> str:
        asset_cell_col = f"{CELL_COL_PREFIX}{resolution}"
        sql = f"SELECT * FROM {ASSET_TABLE_NAME} "

        for db_table in db_tables:
            table_only = db_table.split(".")[1]
            sql += f"JOIN {db_table} AS {table_only} " \
                   f"ON {ASSET_TABLE_NAME}.{asset_cell_col} = " \
                   f"{table_only}.{const.CELL_COL} "

        sql += full_where_clause
        return sql

    def _output_to_json(self, input: List[Tuple], columns: List[str]) -> Dict:
        if len(input) == 0:
            return {
                "columns": [],
                "data": []
            }
        # sanity check on row length
        if len(input[0]) != len(columns):
            raise ValueError(f"number of columns in output row ({len(input[0])}"
                             f" did not match"
                             f" expected number of columns ({len(columns)})")

        input_as_list = [list(tup) for tup in input]

        return {
            "columns": columns,
            "data": input_as_list
        }
