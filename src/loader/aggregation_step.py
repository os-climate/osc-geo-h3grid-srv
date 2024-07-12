import functools
import logging
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Dict, Callable, Any, List, Tuple

import h3
import pandas
from pandas import DataFrame

from common.const import LOGGING_FORMAT, CELL_COL

# Set up logging

logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class AggregationStep(ABC):
    @abstractmethod
    def __init__(self, conf_dict: Dict[str, Any]):
        pass

    @abstractmethod
    def get_agg_func(self) -> Callable[[pandas.Series], Any] | str:
        """
        A method that returns either a string (representing a built-in
        aggregation method), or an aggregation function.
        """
        pass

    @abstractmethod
    def get_name_suffix(self) -> str:
        pass


class CellAggregationStep:

    def __init__(
            self,
            agg_steps: List[AggregationStep],
            res: int,
            data_cols: List[str],
            key_cols: List[str]
    ):
        self.agg_steps = agg_steps
        self.res = res
        self.data_cols = data_cols
        self.agg_map = self._get_agg_mapping()
        self.key_cols = key_cols

    def run(self, in_df: DataFrame) -> DataFrame:
        """
        Run aggregations by h3 cell on the input dataframe.

        :param in_df:
            The input dataframe. Is required to have the
            'latitude' and 'longitude' columns, as well as each
            of the data columns and key columns mentioned in the
            object constructor.
        :type in_df: DataFrame
        :return: The dataframe with all aggregations performed.
        :rtype: DataFrame
        """
        if len(self.agg_steps) == 0:
            logger.info("Skipping CellAggregationStep as no aggregation steps"
                        "were specified.")
            return in_df

        logger.info("Running CellAggregationStep")

        with_cell = self._add_cell_column(in_df)

        group_cols = self.key_cols.copy()
        group_cols.append(CELL_COL)

        groups = with_cell.groupby(group_cols)[self.data_cols]

        with_agg = groups.agg(**self.agg_map).reset_index()

        return with_agg

    def _add_cell_column(self, in_df: DataFrame) -> DataFrame:
        def to_cell(row):
            lat = row['latitude']
            long = row['longitude']
            return h3.geo_to_h3(lat, long, self.res)

        in_df[CELL_COL] = in_df.apply(to_cell, axis='columns')

        return in_df

    def _get_agg_mapping(self) -> Dict[str,Tuple[str,Any]]:
        out = {}
        for data_col in self.data_cols:
            for agg_step in self.agg_steps:
                key = f"{data_col}_{agg_step.get_name_suffix()}"
                if key in out:
                    raise ValueError(
                        'aggregation output column name'
                        f' {key} is already in use by another aggregation')
                out[key] = (data_col, agg_step.get_agg_func())
        return out



class MinAggregation(AggregationStep):

    def __init__(self, conf_dict: Dict[str, Any]):
        # no parameters are needed here, as it is a builtin
        pass

    def get_agg_func(self) -> Callable[[pandas.Series], Any] | str:
        return 'min'

    def get_name_suffix(self) -> str:
        return 'min'


class MaxAggregation(AggregationStep):
    def __init__(self, conf_dict: Dict[str, Any]):
        # no parameters are needed here, as it is a builtin
        pass

    def get_agg_func(self) -> Callable[[pandas.Series], Any] | str:
        logger.info("preparing MaxAggregation aggregation step")
        return 'max'

    def get_name_suffix(self) -> str:
        return 'max'


class MeanAggregation(AggregationStep):
    def __init__(self, conf_dict: Dict[str, Any]):
        # no parameters are needed here, as it is a builtin
        pass

    def get_agg_func(self) -> Callable[[pandas.Series], Any] | str:
        return 'mean'

    def get_name_suffix(self) -> str:
        return 'mean'


class MedianAggregation(AggregationStep):
    def __init__(self, conf_dict: Dict[str, Any]):
        # no parameters are needed here, as it is a builtin
        pass

    def get_agg_func(self) -> Callable[[pandas.Series], Any] | str:
        return 'median'

    def get_name_suffix(self) -> str:
        return 'median'


class CountWithinBounds(AggregationStep):
    # created mostly as an example of how to make a non-builtin agg function
    #  and to test that it works, rather than as a common expected use case.

    def __init__(self, conf_dict: Dict[str, Any]):
        if 'min' in conf_dict:
            self.min = conf_dict['min']
        else:
            self.min = None
        if 'max' in conf_dict:
            self.max = conf_dict['max']
        else:
            self.max = None
        if self.min is None and self.max is None:
            raise ValueError(
                "CountWithinBounds requires that either or both of 'min' "
                " and 'max' values be set, butboth were None.")

    def get_agg_func(self) -> Callable[[pandas.Series], Any] | str:
        def within_bounds(series: pandas.Series) -> int:
            def within_bounds_single(x):
                if self.max is not None and x > self.max:
                    return 0
                if self.min is not None and x < self.min:
                    return 0
                return 1

            return functools.reduce(
                lambda x, y: x + within_bounds_single(y), series, 0)

        return within_bounds

    def get_name_suffix(self) -> str:
        out = f'within_bounds'
        if self.min is not None:
            out = out + f"_{self.min}"
        else:
            out = out + "_none"
        if self.max is not None:
            out = out + f"_{self.max}"
        else:
            out = out + "_none"
        return out
