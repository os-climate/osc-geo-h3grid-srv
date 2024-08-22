# Getting Started

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


### Starting server in local mode

The server can be started in local mode with the below command:

~~~
./bin/start.sh --configuration ./config/config-example.yml
~~~

### Running as a Docker Image

The geo server can be run either directly on your local machine, or as a 
docker image. If running on directly on a local machine, this section 
can be skipped.

In order to create a docker image the `DOCKER_USERNAME` environment 
variable must be set to a valid dockerhub username.

A Dockerfile is provided for this service. A docker image for this service can be
creating using the following script, which will create but not publish the image:

```
$PROJECT_DIR/bin/dockerize.sh
```

In order to publish this image the `DOCKER_TOKEN` environment variable
must be set to a dockerhub token that is associated with the username set in the
`DOCKER_USERNAME` environment variable. Additionally, the
`DOCKER_REGISTRY` environment variable must be set if publishing
to a custom registry. 

Then the below command can be executed to create and publish an image,
with the `--publish` argument controlling whether the image is published,
and where it is published to. The `--latest` argument controls whether a
specific version is published, or whether this version will also be published
as "latest". The `--version` argument controls what specific version number
the image will have when published.

```console
$PROJECT_DIR/bin/dockerize.sh --publish [false|custom|dockerhub] [--latest] [--version <version>]
```

To run this image use the following command

```
./bin/startd.sh 0 config.yml
```

Note that if using docker mode, some CLI calls must be made from
within the docker image.

### Configuring the CLI

A CLI is available that makes it easy to interact
with the Registry:

The CLI communicates with the geo server
and hence requires a host and port. For your convenience,
this tutorial uses HOST and PORT environment variables to hold these.

If started in local mode, the host and port will be `localhost` and `8000`,
if using the default configuration.

Also, verbose logging can be enabled using the "--verbose" tag.
For your convenience, each of the examples in this tutorial
use an environment variable, VERBOSE, which if set to
"--verbose" will permit extended logging in the CLI.

Local host is set up to use port 8000:
~~~~
HOST=localhost ;
PORT=8000 ;
VERBOSE="--verbose"
~~~~

Docker host is set up to use port 25001:
~~~~
HOST=localhost ;
PORT=24000 ;
VERBOSE="--verbose"
~~~~

To disable verbose logging, unset VERBOSE:
~~~~
VERBOSE=""
~~~~


### Retrieve shapefiles

Shapefiles are files that define a geographic region. They are used in this
example to ensure that processing only happens within a target region.
In order to run the below examples, shapefiles will need to be downloaded from
the following link:

Shapefiles source:
- [world-administrative-boundaries.zip][1]

Retrieved from parent site: 
https://public.opendatasoft.com/explore/dataset/world-administrative-boundaries/export/
- retrieved as a dataset from the "Geographic file formats" section,
"Shapefile" element, by clicking the "Whole dataset" link

Create the `data/shapefiles/WORLD` directory as below 
(if it does not already exist)
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

### Retrieve Data

The GISS temperature dataset contains data on global temperatures,
and is used as the raw data for the examples in this README. It can be
retrieved from the below links:

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

python ./examples/common/temp_giss_to_csv.py \
--stations $STATIONS \
--temperature $TEMPERATURE \
--output $OUTPUT
```

### Create directories

Create the directories needed for running the examples:
~~~
mkdir ./tmp
~~~

## Loading Data

In order to load data the 
[os-geo-h3loader-cli]
library must be used to generate the databases that the server
uses. Instructions in this section will frequently refer to files
in that repository.

In order to load this data into the geo server, run the below command. This
will create the dataset in the `./tmp` directory.

The configuration used in this example specifies the information
needed to load the data into the database, as well as specifying a shapefile
and region - Germany - that controls what region of the world data is loaded
for. Data will not be calculated for any part of the h3 grid that is
not within this region.

This takes about 1 minute to run, and should be run using code from 
the [os-geo-h3loader-cli] repository.

```
CONFIG_PATH="./examples/getting-started/giss_2022_12.yml" ;

python ./src/cli/cli_load.py --host $HOST --port $PORT load \
--config_path $CONFIG_PATH
```

This command will create the `./data/geo_data/temperatures/giss_2022_12.csv` file as output.

For more information on loading datasets, see the [loading README](/docs/README-loading.md).

## Register the new dataset

In order to work with the dataset it must be registered with the server. The
below command will create the metadata necessary to interact with the
previously created database. This metadata will be stored in the
`./tmp/dataset_metadata.duckdb` database, which will be created
if it does not already exist.

```
DATABASE_DIR="./tmp" ;
DATASET_NAME="giss_temperature_2022_12_example" ;
DESCRIPTION="GISS temperature data for December 2022 in Germany" ;
VALUE_COLUMNS="{\"temperature\":\"REAL\"}" ;
KEY_COLUMNS="{\"h3_cell\":\"VARCHAR\"}" ;
DATASET_TYPE="h3" ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT addmeta \
    --database_dir $DATABASE_DIR \
    --dataset_name $DATASET_NAME \
    --description "$DESCRIPTION" \
    --value_columns $VALUE_COLUMNS \
    --key_columns $KEY_COLUMNS \
    --dataset_type $DATASET_TYPE
```

## View metadata

To view the current metadata, run the below command:

~~~
DATABASE_DIR="./tmp" ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT showmeta \
    --database_dir $DATABASE_DIR
~~~

## Querying the dataset

The dataset created can be queried through the API or through the
command line. The below examples will use the command line interface.

For information on all available APIs or available command line commands
see the [geospatial README](/docs/README-geospatial.md).

### Data by Radius

This query will retrieve temperature data within a 200km radius of
Berlin, Germany. All hexes in the h3 grid that have their center point
fall within this radius will have their data returned.

~~~
DATASET="giss_temperature_2022_12_example"
LATITUDE=52.518 ;
LONGITUDE=13.405 ;
RESOLUTION=5 ;
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

### Data by Shapefile

This example uses a shapefile to return only data for hexagons
within the bounds of Germany.

~~~
DATASET="giss_temperature_2022_12_example" ;
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
RESOLUTION=3 ;
REGION="Germany" ;
YEAR=2022 ;
MONTH=12 ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --shapefile $SHAPEFILE \
    --region $REGION \
    --resolution $RESOLUTION \
    --year $YEAR \
    --month $MONTH
~~~

### Visualize dataset

This command will generate a visualization of the temperature data, using
a red color scale, over the Germany. The visualization can be seen by
loading the output file (`./tmp/giss_temperature_dec_2022_6_Germany.html`) in
a web browser.

~~~
DATABASE_DIR="./tmp" ;
DATASET="giss_temperature_2022_12_example" ;
RESOLUTION=6 ;
VALUE_COLUMN="temperature" ;
RED=255 ;
GREEN=0 ;
BLUE=0 ;
OUTPUT_FILE="./tmp/giss_temperature_dec_2022_6_Germany.html" ;
MIN_LAT=46 ;
MAX_LAT=56 ;
MIN_LONG=4 ;
MAX_LONG=17 ;
THRESHOLD=0.02 ;
YEAR=2022 ;
MONTH=12 ;

python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT visualize-dataset \
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
--year $YEAR \
--month $MONTH
~~~

[os-geo-h3loader-cli]: https://github.com/os-climate/osc-geo-h3loader-cli
[1]: https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/world-administrative-boundaries/exports/shp?lang=en&timezone=America%2FNew_York