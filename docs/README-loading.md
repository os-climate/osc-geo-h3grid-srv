# Data Loading Capabilities

This product can load and optionally interpolate input data which contains 
a latitude and longitude into a dataset mapped to H3 cells of a desired resolution.
H3 (from Uber) cells are used to create a uniform mesh of hexagons to evenly
divide the globe at a variety of resolutions.

Once cell attributes are loaded, they will be stored
in a DuckDB database.

Non-interpolated datasets may be loaded as point datasets, 
skipping the interpolation step, and storing the exact location
of input data points into the database. 

## Command Line Interpreter (CLI)

A CLI is available that makes it easy to interact
with the service:
~~~~
python ./src/geoserver/cli_load.py $VERBOSE --host $HOST --port $PORT --help

usage: cli_load.py [-h] [--verbose] --host HOST --port PORT {load,initialize} ...

Data Mesh Agent Command Line Interface (CLI)

positional arguments:
  {load,initialize}  Available commands
    load             load a dataset into the geospatial dataset
    initialize       create source db from giss temperature data

options:
  -h, --help         show this help message and exit
  --verbose          Enable verbose output
  --host HOST        Server host
  --port PORT        Server port

~~~~

## Loading a dataset

To load a dataset through the command line supply the relevant configuration
file and run the below command to load it. An example of loading a dataset with
a configuration file is shown below. Output of a successful run of a loader will
always be a duckdb database stored in the `<database_dir>/<dataset_name>.duckdb`
location, where database_dir, and dataset_name are parameters specified in the 
configuration file.

~~~
CONFIG_PATH="./examples/loading/h3_no_header_conf.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8001 load \
--config_path $CONFIG_PATH
~~~

Example datasets and config files are available in the `./examples/loading` directory

### Assembling a configuration file

In order to load data a configuration file is needed to specify
what is to be done. Parameters common to all loaders are shown below:

| Parameter       | Type      | Description                                                                                                                                                                                                                                                                            |
|-----------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| loader_type     | str       | The type of loader to use. <br> Available loaders: CSVLoader, ParquetLoader                                                                                                                                                                                                            |
| dataset_name    | str       | The name of the dataset to be created.                                                                                                                                                                                                                                                 |
| dataset_type    | str       | The type of dataset that is to be created. <br>Available options: [ h3, point ]                                                                                                                                                                                                        |
| database_dir    | str       | The location where databases containing processed data is stored. Directory must exist.                                                                                                                                                                                                |
| interval        | str       | Determines for what time periods data is differentiated for.<br> Available options: [ one-time, yearly, monthly, daily ]                                                                                                                                                               |
| max_resolution  | str       | Determines the maximum h3 resolution for which data will be calculated                                                                                                                                                                                                                 |
| data_columns    | list[str] | A list of columns in the dataset that are to be interpreted as data. Any columns except latitude, longitude, date columns, and time columns will be ignored                                                                                                                            |
| year_column     | str       | An optional parameter that contains the name of the column that contains information about what year this data element is from.<br/> Mandatory if interval is yearly, monthly, or daily                                                                                                |
| month_column    | str       | An optional parameter that contains the name of the column that contains information about what month this data element is from.<br/> Mandatory if interval is monthly, or daily                                                                                                       |
| day_column      | str       | An optional parameter that contains the name of the column that contains information about what day this data element is from.<br/> Mandatory if interval is daily                                                                                                                     |
| shapefile       | str       | An optional parameter that indicates a shapefile to use to limit interpolation for h3 datasets. Only cells within the shapefile's boundaries will be interpolated. Does nothing in point datasets.                                                                                     |
| region          | str       | An optional parameter that indicates the name of a region within a specified shapefile to use to limit interpolation for h3 datasets. Only cells within the region's boundaries will be interpolated.<br/> Requires that the shapefile parameter exist. Does nothing in point datasets |
| mode            | str       | Determines what loading mode the loader will use. <br/>Available options: [ "create", "insert"]                                                                                                                                                                                        |
| max_parallelism | int       | Determines the maximum number of simultaneous threads to use when interpolating data                                                                                                                                                                                                   |

#### CSVLoader

The below parameters are specific to csv loader:

| Parameter      | Type      | Description                                                                                                                                                                                | 
|----------------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| file_path      | str       | The path to the csv file to be loaded                                                                                                                                                      | 
| has_header_row | str       | Indicates whether this csv file has a header row.                                                                                                                                          |
| columns        | Dict[str] | a dictionary of columns mapped to their contained data type. Supported types: [str, float, int]                                                                                            |

#### ParquetLoader

The below parameters are specific to the parquet loader:

| Parameter      | Type      | Description                           | 
|----------------|-----------|---------------------------------------|
| file_path      | str       | The path to the csv file to be loaded |

## Running Examples

There are examples of data loading within the `examples/loading` directory.

### Basic loaders

The `examples/loading/basic` directory contains two examples, both of which use
the data in the no_date_no_header file. This file contains a very small amount of data
that will demonstrate the most basic abilities of the loader.

#### h3 example

This example demonstrates the ability to create a very minimal h3 dataset.
This will create an output file at `./tmp/h3_no_header.duckdb` containing the 
loaded dataset.

```
CONFIG_PATH="./examples/loading/basic/h3_no_header_conf.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```

#### Point example

This example demonstrates the ability to create a very minimal point dataset.
This will create an output file at `./tmp/point_no_header.duckdb` containing the 
loaded dataset.

```
CONFIG_PATH="./examples/loading/basic/point_no_header_conf.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```

### Jamaica Buildings

This example provides a slightly more detailed example of a point dataset.
This will create an output file at `./tmp/jamaica_buildings.duckdb` containing the 
loaded dataset.

```
CONFIG_PATH="./examples/loading/jamaica_buildings/jamaica_building_conf.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```

### GISS Temperature

This example shows more features of the loader using the GISS temperature 
dataset, and a shapefile of countries. This example both contains more
data than prior examples, but also uses a shapefile to limit the area for 
which interpolation is performed. In this example data is interpolated
only within the borders of Jamaica.

#### prerequisites

##### Shapefiles

Shapefiles are files that define a geographic region. They are used in this
example to ensure that processing only happens within a target region. 
In order to run the below examples, shapefiles will need to be downloaded from
the following link:

Shapefiles source:
- [world-administrative-boundaries.zip](https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/world-administrative-boundaries/exports/shp?lang=en&timezone=America%2FNew_York): 

Retrieved from parent site: https://public.opendatasoft.com/explore/dataset/world-administrative-boundaries/export/
- retrieved as a dataset from the "Geographic file formats" section, 
"Shapefile" element, by clicking the "Whole dataset" link

Create the `data/shapefiles/WORLD` directory as below (if it does not already exist)
~~~
mkdir -p ./data/shapefiles/WORLD
~~~

Unzip the `world-administrative-boundaries.zip` file into the
`data/shapefiles/WORLD` directory. This should result in a 
directory structure that looks like below:

~~~
data
|-- shapefiles
    |-- WORLD
        |-- world-adminstrative-boundaries.prj
        |-- world-adminstrative-boundaries.cpg
        |-- world-adminstrative-boundaries.dbf
        |-- world-adminstrative-boundaries.shp
        |-- world-adminstrative-boundaries.shx
~~~

##### GISS Data Retrieval

The GISS temperature dataset contains data on global temperatures, 
and is used as the raw data for the examples in this README. It is used as sample
data for some of the below examples. It can be retrieved from the below links:

GISS Temperature:
- [v4.mean_GISS_homogenized.txt](https://data.giss.nasa.gov/gistemp/station_data_v4_globe/v4.mean_GISS_homogenized.txt.gz)
- [stations.txt](https://data.giss.nasa.gov/gistemp/station_data_v4_globe/station_list.txt)

These were retrieved from this parent site: https://data.giss.nasa.gov/gistemp/station_data_v4_globe/

Create the `data/geo_data/temperatures` directory using the
below command (if it does not already exist):

~~~
mkdir -p data/geo_data/temperatures
~~~

Copy both the `v4.mean_GISS_homogenized.txt` and `stations.txt` to the 
`data/geo_data/temperatures` directory.

Once the raw temperature data is retrieved, it must be turned into the sort of
CSV that the loader can process. To do this run the below command,
which will produce a csv for the loader representing data in the month 
of December, in the year 2022:

```
STATIONS="./data/geo_data/temperatures/station_list.txt" ;
TEMPERATURE="./data/geo_data/temperatures/v4.mean_GISS_homogenized.txt" ;
OUTPUT="./data/geo_data/temperatures/giss_2022_12.csv"

python ./examples/loading/giss_temperature/temp_giss_to_csv.py \
--stations $STATIONS \
--temperature $TEMPERATURE \
--output $OUTPUT
```

#### Running the example

Running the below script will generate the `tmp/giss_temperature_2022_12_example.duckdb`
file, which contains the output of this example.

```
CONFIG_PATH="./examples/loading/giss_temperature/giss_2022_12.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```


### European Flood Data

This examples shows another dataset that is available. This 
dataset contains flood information across Europe. The scripts
included with this example will generate datasets for Germany,
and a smaller higher-resolution dataset for a section of Northwestern
Germany.

This example will load the data into a parquet format. 

#### prerequisites
##### Shapefiles
Shapefiles are files that define a geographic region. They are used in this
example to ensure that processing only happens within a target region. 
In order to run the below examples, shapefiles will need to be downloaded from
the following link:

Shapefiles source:
- [world-administrative-boundaries.zip](https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/world-administrative-boundaries/exports/shp?lang=en&timezone=America%2FNew_York): 

Retrieved from parent site: https://public.opendatasoft.com/explore/dataset/world-administrative-boundaries/export/
- retrieved as a dataset from the "Geographic file formats" section, 
"Shapefile" element, by clicking the "Whole dataset" link

Create the `data/shapefiles/WORLD` directory as below (if it does not already exist)
~~~
mkdir -p ./data/shapefiles/WORLD
~~~

Unzip the `world-administrative-boundaries.zip` file into the
`data/shapefiles/WORLD` directory. This should result in a 
directory structure that looks like below:

~~~
data
|-- shapefiles
    |-- WORLD
        |-- world-adminstrative-boundaries.prj
        |-- world-adminstrative-boundaries.cpg
        |-- world-adminstrative-boundaries.dbf
        |-- world-adminstrative-boundaries.shp
        |-- world-adminstrative-boundaries.shx
~~~


In addition a simple shapefile that selects a small rectangle of northeast Germany is available
in the `examples/loading/flood_data/nw_germany_shapefile` location. 

Create the `./data/shapefiles/custom/nw_germany_shapefile` directory to hold these files:
~~~
mkdir -p ./data/shapefiles/custom/nw_germany_shapefile
~~~

Then copy the files from `examples/loading/flood_data/nw_germany_shapefile` directory
to the `./data/shapefiles/custom/nw_germany_shapefile` directory.

~~~
cp ./examples/loading/flood_data/nw_germany_shapefile/* ./data/shapefiles/custom/nw_germany_shapefile
~~~

##### Flood Data

Additionally, the flood data that will be used as the 
raw data for this example will need to be retrieved. Note that this
data is 5GB in size.

It can be retrieved from the below link
- [Pan-European data sets of river flood probability of occurrence under present and future climate_1_all.zip](https://data.4tu.nl/file/df7b63b0-1114-4515-a562-117ca165dc5b/5e6e4334-15b5-4721-a88d-0c8ca34aee17)

Which was retrieved from this [parent site](https://data.4tu.nl/articles/dataset/Pan-European_data_sets_of_river_flood_probability_of_occurrence_under_present_and_future_climate/12708122) 

Create the `data/geo_data/flood/europe_flood_data` directory as below:

~~~
mkdir -p ./data/geo_data/flood/europe_flood_data
~~~

Unzip the `Pan-European data sets of river flood probability of occurrence under present and future climate_1_all.zip`
file into the `data/geo_data/flood/europe_flood_data` directory.
This should result in a directory structure that looks like the below:

~~~
data
|-- geo_data
    |-- flood
        |-- europe_flood_data
            |-- data.zip
            |-- readme_river_floods_v1.1.pdf
~~~

Create the `data/geo_data/flood/europe_flood_data/data` directory as below

~~~
mkdir -p ./data/geo_data/flood/europe_flood_data/data
~~~

Unzip the `data.zip` file into the `./data/geo_data/flood/europe_flood_data/data`
directory. This should result in a file structure like below:

~~~
data
|-- geo_data
    |-- flood
        |-- europe_flood_data
            |-- data.zip
            |-- readme_river_floods_v1.1.pdf
            |-- data
                |-- River_discharge_1971_2000_hist.dbf
                |-- River_discharge_1971_2000_hist.prj
                ...
~~~

Once the information is retrieved, it must be turned into a parquet file that 
the loader can process. To do that run the below file:

for Germany
```
RAW="./data/geo_data/flood/europe_flood_data/data/River_flood_depth_1971_2000_hist_0010y.tif" ; 
OUT="./tmp/flood_germany.parquet" ;
FILTER="Germany" ;

python ./examples/loading/flood_data/flood_to_parquet.py \
--raw $RAW \
--output $OUT \
--filter $FILTER
```

for nw germany

```
RAW="./data/geo_data/flood/europe_flood_data/data/River_flood_depth_1971_2000_hist_0010y.tif" ; 
OUT="./tmp/flood_nw_germany.parquet" ;
FILTER="NW_Germany" ;

python ./examples/loading/flood_data/flood_to_parquet.py \
--raw $RAW \
--output $OUT \
--filter $FILTER
```

### Running the example
#### Data for all of Germany as an h3 dataset
Running this example will generate the `tmp/flood_data.duckdb` file
as the output of this example. It will contain flood information about 
Germany, up to resolution 7.

```
CONFIG_PATH="./examples/loading/flood_data/flood_data.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```

#### Data for Northeast Germany
Running this example will generate the `tmp/flood_nw_germany.duckdb` file
as the output of this example. It will contain flood information about 
northwestern Germany, up to resolution 9.
```
CONFIG_PATH="./examples/loading/flood_data/flood_data_nw_germany.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```

#### Data for Germany as a point dataset

While this dataset uses the same raw data as the h3 Germany example,
it loads the data as a collection of points, without interpolating.

```
CONFIG_PATH="./examples/loading/flood_data/flood_data_point.yml" ;

python ./src/geoserver/cli_load.py --host localhost --port 8000 load \
--config_path $CONFIG_PATH
```