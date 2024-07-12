# Geospatial Capabilities

This product offers a geospatial temporal data mesh that uses
the H3 grid (created by Uber) to create a uniform mesh of the globe at
a variety of resolutions.

The geospatial components include:
- A server that serves location data
- a CLI that issues requests for location data

## Prerequisites

### Setting up your Environment

Some environment variables are used by various code and scripts.
Set up your environment as follows (note that "source" is used)
~~~~
source ./bin/environment.sh
~~~~

It is recommended that a Python virtual environment be created.
Several convenience scripts are available to create and activate
a virtual environment.

To create a new virtual environment run the below command
(it will create a directory called "venv" in your current working directory):
~~~~
$PROJECT_DIR/bin/venv.sh
~~~~

Once your virtual environment has been created, it can be activated
as follows (note: you *must* activate the virtual environment
for it to be used, and the command requires `source` to ensure
environment variables to support venv are established correctly):
~~~~
source $PROJECT_DIR/bin/vactivate.sh
~~~~

Install the required libraries as follows:
~~~~
pip install -r requirements.txt
~~~~


## Shapefiles

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

### Geospatial Data

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

See [Data Loading](/docs/README-loading.md) for instructions on turning this data into
a geomesh dataset.

### Starting the server

The server can be started with the below command. Note that
the environment and vactivate scripts must be run prior.

~~~
./bin/start.sh --configuration ./config/config-example.yml
~~~

## Dataset types

There are two types of dataset supported by the geospatial server, and each
has slightly different interactions with endpoints.

The first type of dataset is a continuous or h3 dataset. This is a dataset where data
is interpolated between known values to generate a distribution with
values on hexes that do not contain data points themselves, but that
have data points nearby them. This can be useful for information like
temperature data, where the underlying distribution varies continuously,
and nearby data can be used to predict intermediate values without large errors.

The second type of dataset is a point dataset. This is a dataset where
data is stored as a raw collection of points, without interpolation. This
can be useful for datasets like asset locations, where data does not vary
continuously between points, and knowing nearby points cannot be used to
predict values as unknown points.

## Command Line Interpreter (CLI)

A CLI is available that makes it easy to interact
with the service:
~~~~
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT --help

usage: cli_geospatial.py [-h] [--verbose] --host HOST --port PORT
                         {addmeta,showmeta,filter,visualize,visualize-dataset,initialize,show} ...

Data Mesh Agent Command Line Interface (CLI)

positional arguments:
  {addmeta,showmeta,filter,visualize,visualize-dataset,initialize,show}
                        Available commands
    addmeta             Add a metadata entry, allowing a dataset to be accessed
    showmeta            show available meta entries
    filter              Filter shape file for just land H3 cells
    visualize           Visualized maps and overlays
    visualize-dataset   Visualized maps and overlays
    initialize          create source db from giss temperature data
    show                Show geospatial data

options:
  -h, --help            show this help message and exit
  --verbose             Enable verbose output
  --host HOST           Server host
  --port PORT           Server port
~~~~


### Get Data by Latitude & Longitude / Radius

One service that the geospatial server offers is the ability to
retrieve data for all h3 cells within a specified radius of a
central point. If using this endpoint this point is specified
by its latitude and longitude.

#### Continuous dataset
~~~
DATASET="giss_temperature_dec_2022"
LATITUDE=43.856 ;
LONGITUDE=-79.059 ;
RESOLUTION=3 ;
RADIUS=200 ;
YEAR=2022 ;
MONTH=12 ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --latitude $LATITUDE \
    --longitude $LONGITUDE \
    --radius $RADIUS \
    --resolution $RESOLUTION \
    --year $YEAR \
    --month $MONTH
~~~

#### Point dataset
~~~
DATASET="jamaica_buildings" ;
LATITUDE=17.9736 ;
LONGITUDE=-76.7907 ;
RADIUS=200 ;
TYPE=point ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --latitude $LATITUDE \
    --longitude $LONGITUDE \
    --radius $RADIUS \
	--type $TYPE
~~~

### Get Data by Latitude & Longitude Point

Another offered service is to retrieves data for the
h3 cell that includes the specified latitude and longitude.
This endpoint exists solely for continuous datasets,
with no equivalent endpoint for point datasets.

~~~
DATASET="giss_temperature_dec_2022" ;
LATITUDE=43.856 ;
LONGITUDE=-79.059 ;
RESOLUTION=3 ;
YEAR=2022 ;
MONTH=12 ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --latitude $LATITUDE \
    --longitude $LONGITUDE \
    --resolution $RESOLUTION \
    --year $YEAR \
    --month $MONTH
~~~

### Get Data by Cell / Radius

Retrieves data for all h3 cells within the specified
radius of the center of a specified h3 cell. The cell is specified
by the cell's ID in the h3 grid.

#### Continuous Datasets
~~~
DATASET="giss_temperature_dec_2022" ;
CELL="832b9bfffffffff" ;
RADIUS=200 ;
YEAR=2022 ;
MONTH=12 ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --cell "$CELL" \
    --radius $RADIUS \
    --year $YEAR \
    --month $MONTH
~~~

#### Point datasets

To set up the data for the below example run the "Jamaica Buildings" example
in [README-loading.md](/docs/README-loading.md)

~~~
DATASET="jamaica_buildings" ;
CELL="8867328a6dfffff" ;
RADIUS=200 ;
TYPE=point ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --cell $CELL \
    --radius $RADIUS \
	--type $TYPE
~~~

### Get Data for a Specific Cell

Retrieves data for a specified h3 cell, identified by ID.
For continuous datasets, this will return only the value
in the specified cell. For point datasets this will return
all values within the bounds of the specified cell.

#### Continuous Datasets
~~~
DATASET="giss_temperature_dec_2022" ;
CELL="832b9bfffffffff" ;
YEAR=2022 ;
MONTH=12 ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --cell "$CELL" \
    --year $YEAR \
    --month $MONTH
~~~

#### Point Datasets
~~~
DATASET="jamaica_buildings" ;
CELL="8867328a6dfffff" ;
TYPE=point ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --cell $CELL \
    --radius $RADIUS \
	--type $TYPE
~~~


### Get Data in shapefile region

Retrieves data within a region defined by a shapefile. If a
shapefile has multiple regions defined within it, one may be
selected with the optional `--region` argument. If this argument
is not provided all regions within the shapefile will be considered
a part of the region to retrieve data for.

#### Continuous Dataset
~~~
DATASET="giss_temperature_dec_2022"
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
RESOLUTION=3 ;
REGION="Canada" ;
YEAR=2022 ;
MONTH=12 ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --shapefile $SHAPEFILE \
    --region $REGION \
    --resolution $RESOLUTION \
    --year $YEAR \
    --month $MONTH
~~~

#### Point Dataset
~~~
DATASET="jamaica_buildings"
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
REGION="Jamaica" ;
TYPE=point ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --shapefile $SHAPEFILE \
    --region $REGION \
	--type $TYPE
~~~

### Filter Data based on shapefile

Shapefiles define geospatial polygons, typically used to
represent countries, regions, or areas of the globe.

The `filter` command finds all H3 cells of a given
resolution that are contained within the bounds of any region
within a provided shapefile.

This is intended to allow the filtered results to be put into a file that
can be later used for the visualize command.

The below command uses the world shapefile to identify all
h3 cells that fall within - or intersect - a country's borders.

This example will generate an output containing these cells' IDs within
the `./samples/cells-3.json` file.

~~~~
RESOLUTION=3 ;
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
TOLERANCE=0.1 ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT filter \
    --shapefile "$SHAPEFILE" \
    --resolution $RESOLUTION \
    --tolerance $TOLERANCE \
    > ./samples/cells-WORLD-$RESOLUTION.json
~~~~

### Visualize Cells

The `visualize` command takes H3 cells output by the above
filter operation (in ./samples/cells-3.json) and uses them
to create a visualization of those cells:
~~~~
RESOLUTION=3 ;
CELLS_PATH="./samples/cells-WORLD-$RESOLUTION.json" ;
MAP_PATH="./samples/cells-WORLD-$RESOLUTION.html" ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT visualize \
    --cells_path $CELLS_PATH \
    --map_path $MAP_PATH
~~~~

### Visualize Dataset

The `visualize-dataset` command allows a visual display of a numerical dataset
to be displayed on a map. It will generate an output html file that can be viewed
to see the visualization. Colour scale will vary linearly between the max and min value
in the dataset, with the maximum color value set in the max-color argument.

A settable threshold exists, and the visualization will not draw parts
of the grid that fall below a threshold value. Threshold is
calculated as min + ((max - min) * threshold), and any values below this will not be displayed.
Setting a threshold allows for faster generation of visualization, especially
when there are large chunks of data with minimum or near-minimum values that
do not meaningfully affect the information the visualization is trying to convey.

~~~
DATABASE_DIR="./tmp" ;
DATASET="flood_data" ;
RESOLUTION=6 ;
VALUE_COLUMN="value" ;
RED=0 ;
GREEN=0 ;
BLUE=255 ;
OUTPUT_FILE="./tmp/visualize_flood_germany_6_10%_threshold_map_bounds.html" ;
MIN_LAT=46.5 ;
MAX_LAT=55.5 ;
MIN_LONG=4.5 ;
MAX_LONG=16.5 ;
THRESHOLD=0.1 ;

python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT visualize-dataset \
--database-dir $DATABASE_DIR \
--dataset $DATASET \
--resolution $RESOLUTION \
--value-column $VALUE_COLUMN \
--max-color $RED $GREEN $BLUE \
--output-file $OUTPUT_FILE \
--min-lat $MIN_LAT \
--max-lat $MAX_LAT \
--min-long $MIN_LONG \
--max-long $MAX_LONG \
--threshold $THRESHOLD
~~~


#### Visualizing point datasets

In order to visualize point datasets, the `ds-type` parameter will need
to be supplied, and set to "point". This will calculate a visualization by
taking the mean value of all points that fall within an h3 cell, at the
appropriate resolution.

~~~
DATABASE_DIR="./tmp" ;
DATASET="flood_data" ;
RESOLUTION=6 ;
VALUE_COLUMN="value" ;
RED=0 ;
GREEN=0 ;
BLUE=255 ;
OUTPUT_FILE="./tmp/visualize_flood_germany_6_10%_threshold_map_bounds.html" ;
MIN_LAT=46.5 ;
MAX_LAT=55.5 ;
MIN_LONG=4.5 ;
MAX_LONG=16.5 ;
THRESHOLD=0.1 ;
DS_TYPE="point" ;

python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT visualize-dataset \
--database-dir $DATABASE_DIR \
--dataset $DATASET \
--resolution $RESOLUTION \
--value-column $VALUE_COLUMN \
--max-color $RED $GREEN $BLUE \
--output-file $OUTPUT_FILE \
--min-lat $MIN_LAT \
--max-lat $MAX_LAT \
--min-long $MIN_LONG \
--max-long $MAX_LONG \
--threshold $THRESHOLD \
--ds-type $DS_TYPE
~~~


### Add Dataset to Metadata

In order to retrieve information from a dataset, that dataset's metadata
must be created. Use the `addmeta` command to add this metadata.

~~~
DATABASE_DIR="./tmp" ;
DATASET_NAME="giss_temperature" ;
DESCRIPTION="Temperature data for the entire globe" ;
VALUE_COLUMNS="{\"temperature\":\"REAL\"}" ;
KEY_COLUMNS="{\"h3_cell\":\"VARCHAR\"}" ;
DATASET_TYPE="h3" ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT addmeta \
    --database_dir $DATABASE_DIR \
    --dataset_name $DATASET_NAME \
    --description "$DESCRIPTION" \
    --value_columns $VALUE_COLUMNS \
    --key_columns $KEY_COLUMNS \
    --dataset_type $DATASET_TYPE
~~~

### Show Dataset Metadata

The `showmeta` command retrieves all metadata currently available about
all datasets.

~~~
DATABASE_DIR="./tmp" ;
python ./src/cli_geospatial.py $VERBOSE --host $HOST --port $PORT showmeta \
    --database_dir $DATABASE_DIR
~~~


## REST Interface

This describes the REST APIs that can be used to access datasets registered
with the server. In order to retrieve data for a dataset, it must first be
loaded (see [loading README](./README-loading.md)), and registered with
the `addmeta` command line command.

### Get temperature by latitude and longitude

#### By radius

To access all temperature data within a specified radius of a point,
access the `/api/geomesh/latlong/radius/{dataset_name}` POST endpoint for
continuous datasets, or the `/api/point/latlong/radius/{dataset_name}`
POST endpoint for point datasets.

Arguments required

| Arg name   | Type  | Description                                                                              |
|------------|-------|------------------------------------------------------------------------------------------|
| latitude   | float | Latitude of central point.                                                               |
| longitude  | float | Longitude of central point.                                                              |
| radius     | float | All cells within this radius will be retrieved.                                          |
| resolution | int   | The h3 resolution level to retrieve data for. Only used for continuous datasets          |
| year       | int   | The year to retrieve data for. Optional Parameter.                                       |
| month      | int   | The month to retrieve data for. must be an integer between 1 and 12. Optional Parameter. |
| day        | int   | The day to retrieve data for. must be an integer between 1 and 31. Optional parameter.   |


#### In a single cell

To access the temperature data for whatever cell contains the
specified point, access the `/api/geodata/giss/latlong/point` POST endpoint
for continuous datasets. No such endpoint exists for point datasets.

Arguments required

| Arg name   | Type   | Description                                                                              |
|------------|--------|------------------------------------------------------------------------------------------|
| latitude   | float  | Latitude of central point                                                                |
| longitude  | float  | Longitude of central point                                                               |
| resolution | int    | The h3 resolution level to retrieve data for                                             |
| year       | int    | The year to retrieve data for. Optional Parameter.                                       |
| month      | int    | The month to retrieve data for. must be an integer between 1 and 12. Optional Parameter. |
| day        | int    | The day to retrieve data for. must be an integer between 1 and 31. Optional parameter.   |


### Get temperature by cell

#### By radius

To access all temperature data within a specified radius of a specified
h3 cell access the `/api/geomesh/cell/radius/{dataset_name}` POST endpoint for
continuous datasets, and the `/api/point/cell/radius/{dataset_name}`
POST endpoint for point datasets.

Arguments required

| Arg name   | Type  | Description                                                                              |
|------------|-------|------------------------------------------------------------------------------------------|
| cell       | str   | The id of the h3 cell that serves as the center of the returned data                     |
| radius     | float | All cells within this radius will be retrieved                                           |
| resolution | int   | The h3 resolution level to retrieve data for                                             |
| year       | int   | The year to retrieve data for. Optional Parameter.                                       |
| month      | int   | The month to retrieve data for. must be an integer between 1 and 12. Optional Parameter. |
| day        | int   | The day to retrieve data for. must be an integer between 1 and 31. Optional parameter.   |


#### In a single cell

To access the data for a single cell in a continuous dataset,
access the `/api/geodata/cell/point/{dataset_name}` POST endpoint.
To retrieve all data that falls within the bounds of a cell in
a point dataset, access the `/api/point/cell/point/{dataset_name}` POST endpoint.

Arguments required

| Arg name   | Type  | Description                                                                              |
|------------|-------|------------------------------------------------------------------------------------------|
| cell       | str   | The id of the h3 cell that serves as the center of the returned data                     |
| resolution | int   | The h3 resolution level to retrieve data for                                             |
| year       | int   | The year to retrieve data for. Optional Parameter.                                       |
| month      | int   | The month to retrieve data for. must be an integer between 1 and 12. Optional Parameter. |
| day        | int   | The day to retrieve data for. must be an integer between 1 and 31. Optional parameter.   |


