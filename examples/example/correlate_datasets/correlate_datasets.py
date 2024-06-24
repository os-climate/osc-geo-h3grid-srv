import argparse
from typing import List

import pandas
from pandas import DataFrame

# Add the source to sys.path (this is a short-term fix)
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '../../..', 'src'))
print(parent_dir)
sys.path.append(parent_dir)

from geoserver.geomesh import Geomesh, PointDataRow


def load_dataset(
        ds_name: str,
        db_dir: str,
        shapefile: str,
        region: str
) -> DataFrame:
    # the particular dataset that is being used here will be a point dataset
    geoesh = Geomesh(db_dir)
    dataset = geoesh.shapefile_get_point(
        ds_name, shapefile, region,
        None, None, None
    )
    ds = point_row_to_df(dataset)
    print(f"retrieved {len(ds)} rows")
    return ds


def point_row_to_df(rows: List[PointDataRow]) -> DataFrame:

    lst = []
    for row in rows:
        d = {
            'latitude': row.latitude,
            'longitude': row.longitude
        }
        for val_col_name in row.values.keys():
            d[val_col_name] = row.values[val_col_name]
        for cell_col_name in row.cells.keys():
            d[cell_col_name] = row.cells[cell_col_name]
        lst.append(d)

    df = pandas.DataFrame.from_records(lst)
    return df

def average_by_cell(flood_data: DataFrame,res: int) -> DataFrame:
    cell_col = f"res{res}"
    val_col = "value"
    only_relevant_cols = flood_data[[cell_col, val_col]]
    return only_relevant_cols\
        .groupby([f'res{res}'])\
        .mean()\
        .reset_index()

def correlate_anonymized(
        flood: DataFrame, asset: DataFrame, res: int
) -> DataFrame:
    cell_col = f'res{res}'
    all_result = asset\
        .set_index(cell_col)\
        .join(flood.set_index(cell_col))

    return all_result[['value', 'uuid']]

def load_non_anonymzed(parquet_location: str) -> DataFrame:
    return pandas.read_parquet(parquet_location)

def deanon_flood(uuid_flood: DataFrame, non_anon: DataFrame) -> DataFrame:
    return uuid_flood.set_index('uuid').join(non_anon.set_index('uuid'))

def get_arg_parser():
    parser = argparse.ArgumentParser(description="asset to flood correlation")

    parser.add_argument(
        "--flood-dataset",
        required=True,
        help="the dataset containing flood data"

    )
    parser.add_argument(
        "--asset-dataset",
        required=True,
        help="the dataset containing asset data"
    )
    parser.add_argument(
        "--non-anon-file",
        required=True,
        help="the parquet file holding the non-anonymized data"
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="where output will be placed"
    )
    parser.add_argument(
        "--db-dir",
        required=True,
        help="Where the geomesh server stores databases"
    )
    parser.add_argument(
        "--shapefile",
        required=True,
        help="Shapefile that defines region data is to be retrieved for"
    )
    parser.add_argument(
        "--region",
        required=True,
        help="region within shapefile that defines where data is to be "
             "retrieved for"
    )
    parser.add_argument(
        "--resolution",
        required=True,
        type=int,
        help="The resolution of h3 cells to use when correlating data"
    )

    return parser

if __name__ == "__main__":
    args = get_arg_parser().parse_args()

    shapefile = args.shapefile
    region = args.region
    db_dir = args.db_dir
    res = args.resolution
    non_anon_file = args.non_anon_file
    out_path = args.output_path


    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.width', None)

    print("Loading flood dataset")
    flood_data = load_dataset(args.flood_dataset, db_dir, shapefile, region)
    print("Loading asset dataset")
    asset_data = load_dataset(args.asset_dataset, db_dir, shapefile, region)
    print("Datasets loaded")

    print(f"Averaging flood data by cell as resolution {res}")
    flood_data_cell_avg = average_by_cell(flood_data, res)
    print("flood data averaged")

    print("Correlating anonymized (uuid-only) asset data with flood data")
    correlated_anon = correlate_anonymized(flood_data_cell_avg, asset_data, res)
    print("flood data correlated with anonymized asset data")

    with_flood_values = correlated_anon[correlated_anon['value'].notna()]\
        .reset_index()
    with_flood_values = with_flood_values.rename(
        columns={"value": "flood_depth", "res9": "cell_id"})
    num_with_values = len(with_flood_values)
    num_total = len(correlated_anon)

    print(f"when correlated {num_with_values}/{num_total}"
          f" assets had non-zero flood depth.")

    print("\nshowing 5 example rows from correlated anonymized data:")

    print(with_flood_values.head(5))




    print("\nThe following steps require access to the non-anonymized data,"
          " and in production would be performed at the client site, rather"
          " than on the geo server itself. ")
    print("Loading non-anonymized data.")
    non_anon_assets = load_non_anonymzed(non_anon_file)
    print("Adding additional fields from de-anonymized data to output")
    deanon = deanon_flood(correlated_anon, non_anon_assets)
    deanon = deanon.rename(columns={"value": "flood_depth"}).reset_index()

    print("\nshowing 5 example rows from de-anonymized data")



    print(deanon[deanon['flood_depth'].notna()].head(5))

    print("correlated data combined with non-anoymized data")

    deanon.to_parquet(out_path)

    print(f"wrote output to {out_path}")


    print("done")