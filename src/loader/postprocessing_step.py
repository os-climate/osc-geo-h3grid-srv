import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any

from pandas import DataFrame

from common import duckdbutils
from common.const import LOGGING_FORMAT, LATITUDE_COL, LONGITUDE_COL, CELL_COL

# Set up logging

logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class PostprocessingStep(ABC):
    @abstractmethod
    def __init__(self, conf_dict: Dict[str, Any]):
        pass

    @abstractmethod
    def run(self, input_df: DataFrame) -> DataFrame:
        pass


@dataclass
class MultiplyValueConf:
    def __init__(self, **entries):
        if 'multiply_by' not in entries:
            raise ValueError(
                "parameter 'multiply_by' was not provided."
                " This parameter is mandatory for MultiplyValue")
        self.__dict__.update(entries)

    multiply_by: float


class MultiplyValue(PostprocessingStep):
    def __init__(self, conf_dict: Dict[str, Any]):
        logger.debug(f"creating ShapefileFilter with conf {conf_dict}")
        self.conf = MultiplyValueConf(**conf_dict)

    def run(self, input_df: DataFrame) -> DataFrame:
        logger.info("running MultiplyValue")
        out = input_df
        all_cols = input_df.columns
        for col in all_cols:
            if col == LATITUDE_COL or col == LONGITUDE_COL or col == CELL_COL:
                continue
            out[col] = out[col] * self.conf.multiply_by

        return out


@dataclass
class AddConstantColumnConf:
    mandatory_entries = ("column_name", "column_value")

    def __init__(self, **entries):
        for man_entry in self.mandatory_entries:
            if man_entry not in entries:
                raise ValueError(
                    f"parameter '{man_entry}' was not provided."
                    " This parameter is mandatory for AddConstantColumn")

        self.__dict__.update(entries)

    column_name: str
    column_value: Any


class AddConstantColumn(PostprocessingStep):
    def __init__(self, conf_dict: Dict[str, Any]):
        logger.debug(f"creating AddConstantColumnConf with conf {conf_dict}")
        self.conf = AddConstantColumnConf(**conf_dict)

    def run(self, input_df: DataFrame) -> DataFrame:
        input_df[self.conf.column_name] = self.conf.column_value
        return input_df
