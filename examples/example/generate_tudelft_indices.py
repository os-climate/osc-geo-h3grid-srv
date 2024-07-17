import glob
import logging
import os
from typing import List, Dict, Any

import yaml

from cli.cliexec_load import CliExecLoad

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(module)s:%(funcName)s %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

conf_dir = "./tmp/tudelft_index_conf/"

def get_files_in_dir(dir: str) -> List[str]:
    search_pattern = os.path.join(dir, '*')
    files = glob.glob(search_pattern)

    return files

def load_dict_from_yml(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r") as c_file:
        d: Dict[str, Any] = yaml.safe_load(c_file)
    return d

if __name__ == "__main__":
    logger.info("Starting...")

    dummy_host = "localhost"
    dummy_port = 8000


    exec = CliExecLoad({
        "host": dummy_host,
        "port": dummy_port,
    })

    for file in get_files_in_dir(conf_dir):
        conf = load_dict_from_yml(file)
        output_info = conf["output_step_params"]
        db_dir = output_info["database_dir"]
        dataset_name = output_info["dataset_name"]

        out_file = db_dir + "/" + dataset_name + ".duckdb"

        if os.path.exists(out_file):
            logger.info(f"skipping generation of {out_file}"
                        f" as it already exists")
        else:
            exec .load_pipeline(file)


