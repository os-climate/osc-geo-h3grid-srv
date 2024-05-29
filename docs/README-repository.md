# Repository Capabilities

Our Geospatial Data Mesh will permit searching for data in
a number of ways:
- by latitude and longitude, returning a point value (for the
nearest cell centroid), or all cells for a radius around this coordinate
- by a cell (H3) index, returning a point value (for the
nearest cell centroid), or all cells for a radius around this coordinate

However, a third option is required - the ability to search for
all cells (and hence their attributes) that are within a
particular shapefile.  The shapefile provides the
boundary which is latitude/longitude (or other coordinate system)
for a given region. To provide an outstanding user experience we will make
shapefiles a primary search element.

Note that we expect these
shapefiles to probably be optimized /simplified to make
searching/filtering efficient and effective - this capability
is supported via our "Shape" class (in shape.py).

To do this, we will need a repository of shapefiles.  This
section documents how to register and unregister a shapefile
as well as get an inventory of registered shapefiles.

There are a few concepts to be aware of with respect
to the shapefile repository:
- Shapefiles are actually a collection of files,
centered around a ".shp" file.  These other files
support broader shapefile capabilities and generally
must accompany the actual .shp file to be able to
process and consume shapefile
- Our repository will retain the full group of
files and make them known by a single name in
our repository; Currently this name has no constraints
(it is just a string) but it is expected that
in the future we may make this a bit more expressive
(perhaps including a fully qualified name that
incorporates a domain)
- The full set of shapefiles are registered as a
ZIP file, which is a compressed copy of the files; Upon successful
registration, these files are uncompressed for subsequent
use

## Command Line Interpreter (CLI)

A CLI is available that makes it easy to interact
with the service:
~~~~
python ./src/geoserver/cli_repository.py $VERBOSE --host $HOST --port $PORT --help

~~~~

## Registering a Shapefile

Let's first create a shapefile ZIP file.  This will ZIP the
contents of the directory "./tmp/WORLD" and put it in a
file called ./tmp/WORLD.zip:
~~~~
cd ./tmp/WORLD ;
zip -r ../WORLD.zip * ;
cd - ;
~~~~

Now that we have a ZIP representation of the shapefile,
let's register it with the repository:
~~~~
REPOSITORY="./repository" ;
NAME="WORLD" ;
CONTENTS="./tmp/WORLD.zip" ;
python ./src/geoserver/cli_repository.py $VERBOSE --host $HOST --port $PORT register \
    --repository "$REPOSITORY" \
    --name "$NAME" \
    --contents "$CONTENTS"

{
  "status": "successful"
}
~~~~

## Viewing Shapefile Inventory in the Repository

Now that we have registered a shapefile, we can see
what named shapefiles exist in our repository:
~~~~
REPOSITORY="./repository" ;
python ./src/geoserver/cli_repository.py $VERBOSE --host $HOST --port $PORT inventory \
    --repository "$REPOSITORY"

[
  "WORLD"
]
~~~~

## Unregistering a Shapefile

Once registered, a shapefile name (and its contents) can be unregistered,
which will remove it from the repository.  Let's remove the
previously registered shapefile:
~~~~
REPOSITORY="./repository" ;
NAME="WORLD" ;
python ./src/geoserver/cli_repository.py $VERBOSE --host $HOST --port $PORT unregister \
    --repository "$REPOSITORY" \
    --name "$NAME"

{
  "status": "successful"
}
~~~~

