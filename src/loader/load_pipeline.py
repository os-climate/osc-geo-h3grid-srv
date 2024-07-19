import importlib
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

import yaml

from loader.aggregation_step import AggregationStep, CellAggregationStep
from loader.output_step import OutputStep
from loader.postprocessing_step import PostprocessingStep
from loader.preprocessing_step import PreprocessingStep
from loader.reading_step import ReadingStep

CLASS_NAME_PARAM = "class_name"


class LoadingPipeline:

    def __init__(
            self,
            reading_step: ReadingStep,
            preprocess_steps: List[PreprocessingStep],
            aggregation_steps: List[AggregationStep],
            postprocess_steps: List[PostprocessingStep],
            output_step: OutputStep,
            res: Optional[int] = None

    ):
        self.res = res
        self.reading_step = reading_step
        self.preprocess_steps = preprocess_steps
        self.aggregation_steps = aggregation_steps
        self.postprocess_steps = postprocess_steps
        self.outputStep = output_step

        if self.res is None and \
                len(self.aggregation_steps) > 0:
            raise ValueError(
                "Aggregation steps require the resolution parameter"
                " be set in order to function."
                " Resolution was unset, and there were"
                f" {len(self.aggregation_steps)} aggregation steps.")

    def run(self):
        data_cols = self.reading_step.get_data_cols()
        key_cols = self.reading_step.get_key_cols()
        df = self.reading_step.read()
        for pre_step in self.preprocess_steps:
            df = pre_step.run(df)

        cell_agg = CellAggregationStep(
            self.aggregation_steps,
            self.res,
            data_cols,
            key_cols
        )

        df = cell_agg.run(df)

        for post_step in self.postprocess_steps:
            df = post_step.run(df)

        self.outputStep.write(df)


@dataclass
class LoadingPipelineConf:

    def __init__(self, **entries):
        self.__dict__.update(entries)

    reading_step: Dict[str, Any]
    """Name of the reading step class"""

    output_step: Dict[str, Any]

    preprocessing_steps: List[Dict[str, Any]] = ()
    """List of preprocessing step configurations"""

    aggregation_steps: List[Dict[str, Any]] = ()
    """List of aggregation step configurations"""

    postprocessing_steps: List[Dict[str, Any]] = ()
    """List of postprocessing step configurations"""

    aggregation_resolution: Optional[int] = None
    """The h3 resolution to group and aggregate by."""

    aggregation_key_cols: Optional[List[str]] = ()
    """
    Any columns that should be used as group keys when aggregating
    in addition to the cell id column that is included by default. 
    """


class LoadingPipelineFactory:

    @staticmethod
    def create_from_conf_file(conf_path: str) -> LoadingPipeline:
        conf_dict = LoadingPipelineFactory._load_dict_from_yml(conf_path)
        conf = LoadingPipelineConf(**conf_dict)

        read_step = LoadingPipelineFactory._get_read_step(conf)
        pre_steps = LoadingPipelineFactory._get_pre_steps(conf)
        agg_steps = LoadingPipelineFactory._get_agg_steps(conf)
        post_steps = LoadingPipelineFactory._get_post_steps(conf)
        out_step = LoadingPipelineFactory._get_output_step(conf)

        return LoadingPipeline(
            read_step,
            pre_steps,
            agg_steps,
            post_steps,
            out_step,
            conf.aggregation_resolution
        )

    @staticmethod
    def _load_dict_from_yml(file_path: str) -> Dict[str, Any]:
        with open(file_path, "r") as c_file:
            d: Dict[str, Any] = yaml.safe_load(c_file)
        return d

    @staticmethod
    def _get_read_step(conf: LoadingPipelineConf) -> ReadingStep:
        splt = conf.reading_step[CLASS_NAME_PARAM].rsplit('.', 1)
        read_module = importlib.import_module(splt[0])
        read_class_name = splt[1]

        param_dict = dict(conf.reading_step).copy()
        del param_dict[CLASS_NAME_PARAM]

        read_cls = getattr(read_module, read_class_name)
        read_instance = read_cls(param_dict)
        return read_instance

    @staticmethod
    def _get_pre_steps(conf: LoadingPipelineConf) -> List[PreprocessingStep]:
        out = []
        for pre_step in conf.preprocessing_steps:
            splt = pre_step[CLASS_NAME_PARAM].rsplit('.', 1)
            module = importlib.import_module(splt[0])
            class_name = splt[1]

            param_dict = dict(pre_step).copy()
            del param_dict[CLASS_NAME_PARAM]

            pre_class = getattr(module, class_name)
            instance = pre_class(param_dict)
            out.append(instance)

        return out

    @staticmethod
    def _get_agg_steps(conf: LoadingPipelineConf) -> List[AggregationStep]:
        out = []
        for agg_ste in conf.aggregation_steps:
            splt = agg_ste[CLASS_NAME_PARAM].rsplit('.', 1)
            module = importlib.import_module(splt[0])
            class_name = splt[1]

            param_dict = dict(agg_ste).copy()
            del param_dict[CLASS_NAME_PARAM]

            agg_class = getattr(module, class_name)
            instance = agg_class(param_dict)
            out.append(instance)

        return out

    @staticmethod
    def _get_post_steps(conf: LoadingPipelineConf) -> List[PostprocessingStep]:
        out = []
        for post_step in conf.postprocessing_steps:
            splt = post_step[CLASS_NAME_PARAM].rsplit('.', 1)
            module = importlib.import_module(splt[0])
            class_name = splt[1]

            param_dict = dict(post_step).copy()
            param_dict.pop(CLASS_NAME_PARAM)

            post_class = getattr(module, class_name)
            instance = post_class(param_dict)
            out.append(instance)

        return out

    @staticmethod
    def _get_output_step(conf: LoadingPipelineConf) -> OutputStep:
        splt = conf.output_step[CLASS_NAME_PARAM].rsplit('.', 1)
        module = importlib.import_module(splt[0])
        class_name = splt[1]

        param_dict = dict(conf.output_step).copy()
        del param_dict[CLASS_NAME_PARAM]

        out_class = getattr(module, class_name)
        instance = out_class(param_dict)
        return instance
