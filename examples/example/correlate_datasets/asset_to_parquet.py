import argparse
import json
import os.path
import uuid
from pathlib import Path
from typing import List

import pandas
from pandas import DataFrame


def load_assets(json_path: str) -> DataFrame:
    with open(json_path) as json_file:
        data = json.load(json_file)
    df = pandas.json_normalize(data['items'])
    return df

def add_uuid(data: DataFrame) -> DataFrame:
    col_num = len(data.columns)
    num_rows = len(data)

    uuids: List[str] = [str(uuid.uuid4()) for _ in range(num_rows)]

    data.insert(col_num, "uuid", uuids, False)
    return data

def filter_cols(data: DataFrame) -> DataFrame:
    return data[['latitude', 'longitude', 'uuid']]


def save_assets(df: DataFrame, out_path: str) -> None:
    df.to_parquet(path=out_path)

def get_arg_parser():
    parser = argparse.ArgumentParser(description="asset to parquet converter")

    parser.add_argument(
        "--raw",
        required=True,
        help="path to the raw assets data"

    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="path to the output directory."
    )

    return parser


if __name__ == "__main__":
    args = get_arg_parser().parse_args()
    out_dir = args.output_dir

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    raw_file = args.raw
    filename_prefix = Path(raw_file).stem

    raw_data = load_assets(raw_file)

    with_uuid = add_uuid(raw_data)
    uuid_file_name = filename_prefix + "_uuid.parquet"
    save_assets(with_uuid, os.path.join(out_dir, uuid_file_name))

    print(f"Wrote {len(with_uuid)} rows to"
          f" {os.path.join(out_dir, uuid_file_name)}")

    anonymized = filter_cols(with_uuid)
    anon_file_name = filename_prefix + "_anonymized.parquet"
    save_assets(anonymized, os.path.join(out_dir, anon_file_name))

    print(f"Wrote {len(anonymized)} rows to"
          f" {os.path.join(out_dir, anon_file_name)}")

    print("done")