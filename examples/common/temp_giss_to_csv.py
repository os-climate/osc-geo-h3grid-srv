# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-04-09 by davis.broda@brodagroupsoftware.com
import argparse
import logging
import re

import pandas as pd

from pandas import DataFrame

MONTH = 12
YEAR = 2022

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


def load_stations(file_path: str) -> DataFrame:
    logger.info(f"Loading station file_path:{file_path}")

    # List to store each row of data
    station_data = []

    # Read and parse the fixed-width file
    with open(file_path, 'r') as file:
        line_num = 0
        for line in file:
            line_num += 1
            if line_num == 1:
                continue
            fields = [
                line[0:11].strip(),  # ID
                line[11:44].strip(),  # STATIONNAME
                line[44:54].strip(),  # LAT
                line[54:63].strip(),  # LON
                line[63:67].strip() or '0'  # BRIGHTNESSINDICATOR
            ]

            # logger.info(f"fields:{fields}")
            # Add parsed fields to the station data list
            station_data.append(fields)

    df = pd.DataFrame(station_data, columns=[
        'ID', 'STATIONNAME', 'LAT', 'LON', 'BRIGHTNESSINDICATOR'
    ])
    return df


def load_temperature(file_path: str) -> DataFrame:
    logger.info(f"Loading temperature file_path: {file_path}")

    # Lists to store parsed data
    station_data = []
    temperature_data = []

    # Parse and store data
    for record in _parse_temperature_file(file_path):
        header, *years = record

        # Splitting the line and limiting to the first three splits
        parts = header.split(maxsplit=3)
        station_id, lat, lon = parts[0], parts[1], parts[2]
        # logger.info(f"station_id: {station_id} lat:{lat} lon:{lon}")

        # Store station data
        station_data.append((station_id, float(lat), float(lon)))

        # Store yearly temperature data
        for year_line in years:
            year_data = year_line.split()
            year = int(year_data[0])
            monthly_temps = [float(temp) / 100 for temp in year_data[1:]]
            for month_num in range(0, len(monthly_temps)):
                temperature_data.append([
                    station_id, year, month_num + 1, monthly_temps[month_num]
                ])

    temperature_df_columns = ['STATIONID', 'YEAR', 'MONTH', 'TEMPERATURE']
    temperature_df = pd.DataFrame(temperature_data,
                                  columns=temperature_df_columns)
    return temperature_df


def _parse_temperature_file(file_path: str):
    with open(file_path, 'r') as file:
        record = []
        for line in file:
            # is a header row indicating a station which has data below
            if re.match(r'^[A-Za-z]', line):
                if record:
                    yield record
                record = [line]
            else:
                record.append(line)
        if record:
            yield record


def dat_to_csv_format(
        stations_file_path: str,
        temp_file_path: str
) -> DataFrame:
    stations_df = load_stations(stations_file_path)
    temp_df = load_temperature(temp_file_path)

    joined = temp_df.set_index('STATIONID').join(
        stations_df.set_index('ID'))

    return joined[['LAT', 'LON', 'YEAR', 'MONTH', 'TEMPERATURE']]


def filter_by_time(df: DataFrame) -> DataFrame:
    return df[(df.YEAR == YEAR) & (df.MONTH == MONTH)]

def write_to_csv(df: DataFrame, out_path: str):
    df.to_csv(out_path, sep=',', encoding='utf-8', index=False)
    pass


def get_arg_parser():
    parser = argparse.ArgumentParser(description="giss temperature converter")

    parser.add_argument(
        "--stations",
        required=True,
        help="path to the stations file"
    )
    parser.add_argument(
        "--temperature",
        required=True,
        help="path to the giss temperature file"

    )
    parser.add_argument(
        "--output",
        required=True,
        help="path to the output file. (should not exist prior to running)"
    )

    return parser


if __name__ == "__main__":
    args = get_arg_parser().parse_args()
    data = dat_to_csv_format(
        args.stations,
        args.temperature
    )
    filtered = filter_by_time(data)

    write_to_csv(filtered, args.output)

    print("finished")
