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

All examples laid out in this file assume that the `config-example.yml` file
was used as the configuration, however in production you will likely wish to 
use a custom configuration file tailored to your purposes. 

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

To run this image use the following command, which will run
and start the server, pulling configuration values from the 
`$PROJECT_DIR/config/config_docker.yml` file.

```
./bin/startd.sh 0
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

## Querying the dataset

For these examples the contents of the
`./examples/common/example_datasets` directory are used. This directory
contains a pair of pre-generated databases, and the metadata dabase used
to track them.

The dataset created can be queried through the API or through the
command line. The below examples will use the command line interface.

For information on all available APIs or available command line commands
see the [geospatial README](/docs/README-geospatial.md).

### Data by Radius

This query will retrieve temperature data within a 50km radius of
Berlin, Germany. All hexes in the h3 grid that have their center point
fall within this radius will have their data returned.

~~~
DATASET="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany"
LATITUDE=52.518 ;
LONGITUDE=13.405 ;
RESOLUTION=7 ;
RADIUS=20 ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --latitude $LATITUDE \
    --longitude $LONGITUDE \
    --radius $RADIUS \
    --resolution $RESOLUTION 
~~~

### Data by Shapefile

This example uses a shapefile to return only data for hexagons
within the bounds of Germany.

~~~
DATASET="tu_delft_river_flood_depth_1971_2000_hist_0010y_germany" ;
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
RESOLUTION=7 ;
REGION="Germany" ;
python ./src/cli/cli_geospatial.py $VERBOSE --host $HOST --port $PORT show \
    --dataset $DATASET \
    --shapefile $SHAPEFILE \
    --region $REGION \
    --resolution $RESOLUTION 
~~~

[os-geo-h3loader-cli]: https://github.com/os-climate/osc-geo-h3loader-cli
[1]: https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/world-administrative-boundaries/exports/shp?lang=en&timezone=America%2FNew_York