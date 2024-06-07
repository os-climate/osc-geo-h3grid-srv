import argparse
from typing import List

import pandas
from pandas import DataFrame

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
    return point_row_to_df(dataset)


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
    parser = argparse.ArgumentParser(description="flood data converter")

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

    flood_data = load_dataset(args.flood_dataset, db_dir, shapefile, region)
    asset_data = load_dataset(args.asset_dataset, db_dir, shapefile, region)
    print("datasets loaded")

    flood_data_cell_avg = average_by_cell(flood_data, res)

    print("flood data averaged")

    correlated_anon = correlate_anonymized(flood_data_cell_avg, asset_data, res)

    print("flood data correlated with anonymized uuids")

    non_anon_assets = load_non_anonymzed(non_anon_file)
    deanon = deanon_flood(correlated_anon, non_anon_assets)

    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.width', None)
    print("showing example rows of output")
    print(deanon.head(5))

    print("correlated data combined with non-anoymized data")

    deanon.to_parquet(out_path)

    print(f"wrote output to {out_path}")


    print("done")