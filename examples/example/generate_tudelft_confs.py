import logging
import os.path
import re
from typing import Dict

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(module)s:%(funcName)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

data_dir = "./data/geo_data/flood/europe_flood_data/data"
conf_output_dir = "./tmp/tudelft_index_conf"

files_raw = [
    "River_flood_depth_1971_2000_hist_0010y.tif",
    "River_flood_depth_1971_2000_hist_0030y.tif",
    "River_flood_depth_1971_2000_hist_0100y.tif",
    "River_flood_depth_1971_2000_hist_0300y.tif",
    "River_flood_depth_1971_2000_hist_1000y.tif",
    "River_flood_depth_2021_2050_RCP45_0010y.tif",
    "River_flood_depth_2021_2050_RCP45_0030y.tif",
    "River_flood_depth_2021_2050_RCP45_0100y.tif",
    "River_flood_depth_2021_2050_RCP45_0300y.tif",
    "River_flood_depth_2021_2050_RCP45_1000y.tif",
    "River_flood_depth_2021_2050_RCP85_0010y.tif",
    "River_flood_depth_2021_2050_RCP85_0030y.tif",
    "River_flood_depth_2021_2050_RCP85_0100y.tif",
    "River_flood_depth_2021_2050_RCP85_0300y.tif",
    "River_flood_depth_2021_2050_RCP85_1000y.tif",
    "River_flood_depth_2071_2100_RCP45_0010y.tif",
    "River_flood_depth_2071_2100_RCP45_0030y.tif",
    "River_flood_depth_2071_2100_RCP45_0100y.tif",
    "River_flood_depth_2071_2100_RCP45_0300y.tif",
    "River_flood_depth_2071_2100_RCP45_1000y.tif",
    "River_flood_depth_2071_2100_RCP85_0010y.tif",
    "River_flood_depth_2071_2100_RCP85_0030y.tif",
    "River_flood_depth_2071_2100_RCP85_0100y.tif",
    "River_flood_depth_2071_2100_RCP85_0300y.tif",
    "River_flood_depth_2071_2100_RCP85_1000y.tif",
    "River_flood_extent_1971_2000_hist_no_protection.tif",
    "River_flood_extent_1971_2000_hist_with_protection.tif",
    "River_flood_extent_2021_2050_RCP45_no_protection.tif",
    "River_flood_extent_2021_2050_RCP45_with_protection.tif",
    "River_flood_extent_2021_2050_RCP85_no_protection.tif",
    "River_flood_extent_2021_2050_RCP85_with_protection.tif",
    "River_flood_extent_2071_2100_RCP45_no_protection.tif",
    "River_flood_extent_2071_2100_RCP45_with_protection.tif",
    "River_flood_extent_2071_2100_RCP85_no_protection.tif",
    "River_flood_extent_2071_2100_RCP85_with_protection.tif"
]

def file_to_params(f_name: str) -> Dict[str,str]:

    flood_depth_pattern = pattern = \
        r"River_flood_depth_(\d{4})_(\d{4})_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)\.tif"
    flood_extent_pattern = \
        r"River_flood_extent_(\d{4})_(\d{4})_([a-zA-Z0-9]+)_([a-zA-Z0-9_]+)\.tif"


    depth_match = re.match(flood_depth_pattern, f_name)
    extent_match = re.match(flood_extent_pattern, f_name)
    if depth_match:
        start_year = depth_match.group(1)
        end_year = depth_match.group(2)
        scenario = depth_match.group(3)
        risk_horizon = depth_match.group(4)

        out = {
            "file_path": data_dir + "/" + f_name,
            "dataset_name": f"tu_delft_{f_name.replace('.tif', '')}",
            "scenario": scenario,
            "risk_window": risk_horizon,
            "date_range": f"{start_year}-{end_year}"
        }
        return out
    elif extent_match:
        start_year = extent_match.group(1)
        end_year = extent_match.group(2)
        scenario = extent_match.group(3)
        with_prot = extent_match.group(4)

        out = {
            "file_path": data_dir + "/" + f_name,
            "dataset_name": f"tu_delft_{f_name.replace('.tif', '')}",
            "scenario": scenario,
            "with_prot": with_prot,
            "date_range": f"{start_year}-{end_year}"
        }
        return out

data_col_name = "flood_risk"


def get_reading_step(file_path: str) -> str:
    return f"""
reading_step: "loader.geotiff_reader.GeotiffReader"
reading_step_params:
  file_path: "{file_path}"
  data_field: "{data_col_name}"
"""


def get_preprocessing_steps() -> str:
    return f""""""


def get_postprocessing_steps(
        params: Dict[str, str]
        # scenario: str, risk_window: str, date_range: str
):
    def make_param_str(key: str, val: str) -> str:
        return f"""\n  - class_name: "loader.postprocessing_step.AddConstantColumn"
    column_name: "{key}"
    column_value: "{val}"
    """
    out = "\npostprocessing_steps:"

    for k, v in params.items():
        out = out + make_param_str(k, v)

    return out


def get_output_step(dataset_name: str) -> str:
    return f"""
output_step: "loader.output_step.LocalDuckdbOutputStep"
output_step_params:
  database_dir: "./tmp"
  dataset_name: "{dataset_name}"
  mode: "create"
  description: "a flood dataset from TU Delft"
  dataset_type: "h3_index"
"""


agg_steps = """
aggregation_steps:
  - class_name: "loader.aggregation_step.MinAggregation"
  - class_name: "loader.aggregation_step.MaxAggregation"
  - class_name: "loader.aggregation_step.MedianAggregation"
  - class_name: "loader.aggregation_step.MeanAggregation"
  
aggregation_resolution: 7
"""


def get_full_conf_str(element: Dict[str, str]) -> str:
    conf_str = get_reading_step(element["file_path"])
    conf_str = conf_str + get_preprocessing_steps()
    conf_str = conf_str + agg_steps

    post_params = element.copy()
    post_params.pop("dataset_name")
    post_params.pop("file_path")

    conf_str = conf_str + get_postprocessing_steps(
       post_params
    )
    conf_str = conf_str + get_output_step(element["dataset_name"])
    return conf_str


if __name__ == "__main__":
    logger.info("Starting...")

    if not os.path.exists(conf_output_dir):
        os.mkdir(conf_output_dir)

    for element in files_raw:
        params = file_to_params(element)
        conf_str = get_full_conf_str(params)
        conf_path = conf_output_dir + f"/{params['dataset_name']}.yml"
        with open(conf_path, 'x') as f:
            f.write(conf_str)
