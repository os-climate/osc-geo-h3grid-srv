# osc-geo-h3grid-srv - Service for Geospatial Temporal Data Mesh

Experimental geospatial temporal data mesh that uses
H3 (from Uber) cells to create a uniform mesh of regions
or varying resolutions for the globe.

Capabilities include:
- loading geospatial data
- interpolating latitude/longitude data to map into H3 cells
of varying resolution.
- managing shapefiles

The Ecosystem Platform was originally developed by Broda Group Software
with key contributions by:
- [Eric Broda](https://www.linkedin.com/in/ericbroda/)
- [Davis Broda](https://www.linkedin.com/in/davisbroda/)
- [Graeham Broda](https://www.linkedin.com/in/graeham-broda-3a2294b3/)

## The Geospatial Grid

The h3 geospatial indexing system ia an indexing system created
by Uber to represent the entire globe. It consists of a series of 
hexagonal grids that cover the world at different resolution levels.  

For more information see the [h3 website](https://h3geo.org/), or
[uber's h3 introduction blog](https://www.uber.com/en-CA/blog/h3/)

## Setting up your Environment

Some environment variables are used by various code and scripts.
Set up your environment as follows (note that "source" is used)
~~~~
source ./bin/environment.sh
~~~~

It is recommended that a Python virtual environment be created.
Several convenience scripts are available to create and activate
a virtual environment.

To create a new virtual environment (it will create a directory
called "venv" in your current working directory):
~~~~
$PROJECT_DIR/bin/venv.sh
~~~~

Once your virtual enviornment has been created, it can be activated
as follows (note: you *must* activate the virtual environment
for it to be used, and the command requires "source" to ensure
environment variables to support venv are established correctly):
~~~~
source $PROJECT_DIR/bin/vactivate.sh
~~~~

Install the required libraries as follows:
~~~~
pip install -r requirements.txt
~~~~


## Getting started

For a brief overview of how to get started with this application, see
the [Getting Started Guide](/docs/getting-started.md). 


## About the CLIs

To see more information about particular aspects of the geo server, see
the below documentation.

This repo offers a command language interface (CLI) to demonstrate
this functionality:
- [Geospatial](/docs/README-geospatial.md): Query information in the Geospatial Data Mesh
- [Data Loading](/docs/README-loading.md): Interpolate sparse data into H3 cell grid
- [Shapefile](/docs/README-shapefile.md): Shapefile simplification, statistics, and viewing
- [Repository](/docs/README-repository.md): Shapefile registration and inventory management
- [End-to-End Examples](/docs/README-example.md): Examples of datasets taken from loading to visualization

## Running tests

```
python -m unittest discover ./test
```

## Branch Naming Guidelines

Each branch should have an associated github issue. Branches should be named as follows:
`<branch-type>/issue-<issue number>-<short description>`. Where the branch type is one of: 
[feature, bugfix, hotfix], the issue number is the number of the associated issue, and the 
short description is a dash ('-') seperated description of the branch's purpose. This to between
one and three words if possibble.

## Roadmap

For information on planned features, see the [roadmap](/docs/roadmap.md)