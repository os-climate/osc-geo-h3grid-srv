# Shapefile Capabilities

This product will help manage shapefiles used
to view and filter geospatial data.

## About Shapefiles

Geopandas shapefiles have several accompanying files that
are automatically generated alongside the primary `.shp` file.
These files are essential
components of the shapefile format, each serving a specific
function, and they are typically saved in the same directory
as the `.shp` file. Here's a brief overview of these files:

1. **.shp**: This is the main file containing the geometry
of the features.

2. **.shx**: The shape index file. It stores the positional
index of the geometric data for efficient access and reading.

3. **.dbf**: The attribute format file. It stores tabular
attribute information associated with the shapefile. Each
row corresponds to a shape, and each column holds an attribute.

4. **.prj**: The projection file. It contains information
about the coordinate system and projection information used
for the geometries in the shapefile.

5. **.cpg**: The code page file (optional). It specifies the
character encoding used in the `.dbf` file.

6. **.sbn** and **.sbx**: Spatial index files (optional). Used
by some software for quicker spatial querying.

When you use the `to_file()` method in Geopandas, at a minimum,
the `.shp`, `.shx`, and `.dbf` files are created in the
specified directory. If your GeoDataFrame has projection
information (`crs` attribute), a `.prj` file will also be
generated.

For example, if you save your shapefile as `"my_shapefile.shp"`, you
should see the following files in your directory:

- `my_shapefile.shp`
- `my_shapefile.shx`
- `my_shapefile.dbf`
- `my_shapefile.prj` (if CRS information is available)

All these files are necessary to comprehensively describe the
shapefile's data and should be kept together to maintain data
integrity. If you transfer the shapefile, you need to include
all these associated files.


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


## Retrieving Shapefiles

Shapefiles are files that define a geographic region. They are used in this
example to ensure that processing only happens within a target region.
In order to run the below examples, shapefiles will need to be downloaded from
the following link (if not already downloaded)

Shapefiles source:
- [world-administrative-boundaries.zip]

Retrieved from parent site: 
https://public.opendatasoft.com/explore/dataset/world-administrative-boundaries/export/
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

A shapefile for Canada is available below

Canada Shapefile
- [website][1]
  - From the Boundary file options, select:
    - Language: English
    - Type: Cartographic Boundary Files (CBF)
    - Administrative boundaries: Census divisions
    - Format: Shapefile (.shp)
  - Then hit the continue button.
  - This should bring up a link labeled 
`lcd_000b21a_e.zip (ZIP version, 136,594.0 kb)`
    - click this link to download the `lcd_000b21a_e.zip` file

Create the `./data/shapefiles/CAN` directory to hold this shapefile

~~~
mkdir -p ./data/shapefiles/CAN
~~~

Unzip the `lcd_000b21a_e.zip` file into the `./data/shapefiles/CAN`

This should result in a
directory structure that looks like below:

~~~
data
|-- shapefiles
    |-- CAN
        |-- lcd_000b21a_e.dbf
        |-- lcd_000b21a_e.prj
        |-- lcd_000b21a_e.shp
        |-- lcd_000b21a_e.shx
        |-- lcd_000b21a_e.xml
~~~

The canada shapefile will need to be converted to wgs84 format. Use the below
command to do so (this may take several minutes):

~~~~
SHAPEFILE="./data/shapefiles/CAN/lcd_000b21a_e.shp" ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT transform \
    --shapefile "$SHAPEFILE"
~~~~

This will generate the file structure seen below

~~~
data
|-- shapefiles
    |-- CAN
        |-- lcd_000b21a_e.dbf
        |-- lcd_000b21a_e.prj
        |-- lcd_000b21a_e.shp
        |-- lcd_000b21a_e.shx
        |-- lcd_000b21a_e.xml
        |-- lcd_000b21a_e-wgs84.cpg
        |-- lcd_000b21a_e-wgs84.dbf
        |-- lcd_000b21a_e-wgs84.prj
        |-- lcd_000b21a_e-wgs84.shp
        |-- lcd_000b21a_e-wgs84.shx

~~~

A shapefile for the USA is available below:

USA Shapefile:
- [website][2]
  - The will be a section of the page with the following headings:
2023, 2023 TIGER/Line Shapefiles, Download
  - Select the "Web Interface" link from this section
  - select 2023 as the year, and "Counties (and equivalent)" as the Layer
  - Hit the submit button
    - This will bring up a different page
  - Click the "Download national file" button. This will download the
`tl_2023_us_county.zip` file

Create the `./data/shapefiles/USA` directory to hold this shapefile

~~~
mkdir -p ./data/shapefiles/USA
~~~

Unzip the `tl_2023_us_county.zip` file into the `./data/shapefiles/USA`

This should result in a
directory structure that looks like below:

~~~
data
|-- shapefiles
    |-- USA
        |-- tl_2023_us_country.cpg
        |-- tl_2023_us_country.dbf
        |-- tl_2023_us_country.prj
        |-- tl_2023_us_country.shp
        |-- tl_2023_us_country.shp.ea.iso.xml
        |-- tl_2023_us_country.shp.iso.xml
        |-- tl_2023_us_country.shx
~~~


## Command Line Interpreter (CLI)

A CLI is available that makes it easy to interact
with the service:
~~~~
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT --help

usage: cli_shapefile.py [-h] [--verbose] --host HOST --port PORT {transform,statistics,simplify,buffer,view} ...

Data Mesh Agent Command Line Interface (CLI)

positional arguments:
  {transform,statistics,simplify,buffer,view}
                        Available commands
    transform           Transform shapefiles to EPSG 4326 (lat/lon)
    statistics          Show statistics for a shapefile
    simplify            Simplify a shapefile (this will reduce number of polygons)
    buffer              Simplify a shapefile (this will reduce number of polygons)
    view                View a raw shapefile

options:
  -h, --help            show this help message and exit
  --verbose             Enable verbose output
  --host HOST           Server host
  --port PORT           Server port
~~~~

## Convert Shapefiles to WGS84 Format

Shapefiles come in many formats but the one we want is
called WGS84 which uses a latitude/longitude coordinate system.
This command will transform common formats to WGS84, and output
the resulting files in the same location of the original
with `-wgs84` appended to their file name:
~~~~
SHAPEFILE="./data/shapefiles/CAN/lcd_000b21a_e.shp" ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT transform \
    --shapefile "$SHAPEFILE"
~~~~


## Get Statistics for a Shapefile

Shapefile optimization is dependent upon understanding the attributes,
of the shapefile.

Example: World map
~~~~
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT statistics \
    --shapefile "$SHAPEFILE"

{
  "count_polygons": 2007,
  "count_vertices": 217009,
  "mean_num_vertices": 108.12605879422023,
  "mean_area": 7.684178131345802,
  "mean_perimeter": 6.840947526847944,
  "mean_area_perimeter_ratio": 0.12112059727680852,
  "mean_shape_index": 1.6032667463465637,
  "mean_num_holes": 0.0014947683109118087,
  "number_of_features": 256,
  "geometry_types": [
    "Polygon",
    "MultiPolygon"
  ],
  "geometry_type_counts": {
    "Polygon": 141,
    "MultiPolygon": 115
  },
  "total_bounds": [
    -179.9999899999999,
    -58.49860999999993,
    179.99999000000003,
    83.62360000000007
  ],
  :
  :
}
~~~~

Example: USA map (is relatively simple, with 3371 polygons, but
has lots of attributes):
~~~~
SHAPEFILE="./data/shapefiles/USA/tl_2023_us_county.shp" ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT statistics \
    --shapefile "$SHAPEFILE"

{
  "count_polygons": 3371,
  "count_vertices": 8188629,
  "mean_num_vertices": 2429.1394245031147,
  "mean_area": 0.34698383240258607,
  "mean_perimeter": 2.416474222540989,
  "mean_area_perimeter_ratio": 0.09398853830619144,
  "mean_shape_index": 1.4350742014602533,
  "mean_num_holes": 0.013645802432512608,
  "number_of_features": 3235,
  "geometry_types": [
    "Polygon",
    "MultiPolygon"
  ],
  "geometry_type_counts": {
    "Polygon": 3182,
    "MultiPolygon": 53
  },
  "total_bounds": [
    -179.231086,
    -14.601813,
    179.859681,
    71.439786
  ],
  :
  :
}
~~~~

Example: Canada map (is complex, with 176209 polygons,
with the bulk of the polygons actually contained in
multi-polygons):
~~~~
SHAPEFILE="./data/shapefiles/CAN/lcd_000b21a_e-wgs84.shp" ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT statistics \
    --shapefile "$SHAPEFILE"

{
  "count_polygons": 176209,
  "count_vertices": 17291386,
  "mean_num_vertices": 98.1299820099995,
  "mean_area": 0.00956406356540714,
  "mean_perimeter": 0.0473455811847466,
  "mean_area_perimeter_ratio": 0.0006730547457339072,
  "mean_shape_index": 1.4057918007864374,
  "mean_num_holes": 0.0005334574283946904,
  "number_of_features": 293,
  "geometry_types": [
    "MultiPolygon",
    "Polygon"
  ],
  "geometry_type_counts": {
    "Polygon": 148,
    "MultiPolygon": 145
  },
  "total_bounds": [
    -141.01807315762753,
    41.681320303727844,
    -52.619366289675774,
    83.13708640823207
  ],
  :
  :
}
~~~~

## Simplify Shapefiles

Maps can be quite detailed with many complex polygons (the more
vertices a polygon has, the more complex it is).  Unfortunately,
the more complex the shape is, and the more lengthy any processing
becomes.  To address this, a shapefile can be "simplified", which
will reduce the number of vertices for the shape
polygons (and multipolygons).

The optimal tolerance seems to be "0.01" which dramatically shrinks the
shapefile and number of vertices (and, at times, polygons) while keeping
a coherent shape without empty spots.  The smaller and less complex
shapefile also significantly speeds up processing.

Note that higher tolerances can work, but they have a tendency
to have empty holes in the shape which may impact processing, or cause
processing errors.

### About Simplifying and Tolerances

Shapefile libraries have a "tolerance" parameter which determines
how much simplification occurs. Here's what it means in real terms:

1. **Definition of Tolerance**: Tolerance is the maximum distance that
the simplified geometry is allowed to deviate from the original geometry.
It sets a threshold for how much the simplification process can
alter the shape of the geometries in the shapefile.

2. **Unit of Measurement**: The unit of the tolerance value is the
same as the coordinate system of your shapefile. If your shapefile
is in a geographic coordinate system (like WGS 84), the tolerance
is in degrees. If it's in a projected coordinate system, it's
usually in meters or feet.

3. **Practical Implication**:
   - A smaller tolerance value means that the simplified geometry
   will stay closer to the original geometry, preserving more detail.
   - A larger tolerance value allows for greater deviation, which
   means more simplification and less detail. This can significantly
   alter the appearance and spatial relationships of the geometries,
   especially for complex shapes.

4. **Real-World Example**:
   - If you set a tolerance of 1 meter in a shapefile with a
   projected coordinate system, the simplified geometries will
   deviate from their original shape by no more than 1 meter.
   - If your shapefile is in a geographic coordinate system
   and you set a tolerance of 0.0001 degrees, this translates
   to a deviation of about 11 meters at the equator (since a
   degree of latitude is approximately 111 kilometers).

5. **Use Cases**:
   - Simplification is often used to reduce file size and
   speed up processing. It's particularly useful for visualization
   and when high precision is not required.
   - However, it's essential to choose an appropriate tolerance
   considering the scale and purpose of your analysis. Over-simplification
   can lead to loss of critical details and misrepresentation of
   spatial relationships.

For practical purposes, the conversion of the given tolerance values
from degrees to kilometers at the equator is as follows:

- 1.0 degree = 111.0 km
- 0.5 degrees = 55.5 km
- 0.25 degrees = 27.75 km
- 0.10 degrees = 11.1 km
- 0.01 degrees = 1.11 km (1111 meters)
- 0.001 degrees = 0.111 km (111 meters)
- 0.0001 degrees = 0.0111 km  (11 meters)

This conversion is based on the approximation that one degree of
latitude (or longitude at the equator) is approximately 111 kilometers.

In summary, the tolerance in shapefile simplification is a measure
of how much you're willing to allow the simplified geometries to
differ from the original ones, with the actual distance of this
deviation determined by the tolerance value and the coordinate
system of the shapefile.

### Simplify Examples

The simplify command will take a shapefile and simplify it down to
a smaller version.
The command will simplify and display statistics by default, with an optional
`--path` parameter that will store the result into a new shapefile.

Simplify the WORLD map (notice the reduced number of vertices):
~~~~
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
TOLERANCE=0.1 ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT simplify \
    --shapefile "$SHAPEFILE" \
    --tolerance $TOLERANCE

{
  "count_polygons": 2007,
  "count_vertices": 28296,
  "mean_num_vertices": 14.09865470852018,
  "mean_area": 7.661577868296819,
  "mean_perimeter": 6.342956577525841,
  "mean_area_perimeter_ratio": 0.12049765602728271,
  "mean_shape_index": 1.7308405963230042,
  "mean_num_holes": 0.0014947683109118087,
  "number_of_features": 256,
  "geometry_types": [
    "Polygon",
    "MultiPolygon"
  ],
  "geometry_type_counts": {
    "Polygon": 141,
    "MultiPolygon": 115
  },
  "total_bounds": [
    -179.9999899999999,
    -58.49472999999995,
    179.99999000000003,
    83.60679000000005
  ],
  :
  :
}
~~~~

Simplify the USA map (notice the reduced number of vertices):
~~~~
SHAPEFILE="./data/shapefiles/USA/tl_2023_us_county.shp" ;
TOLERANCE=0.1 ;
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT simplify \
    --shapefile "$SHAPEFILE" \
    --tolerance $TOLERANCE

{
  "count_polygons": 3371,
  "count_vertices": 21213,
  "mean_num_vertices": 6.292791456541086,
  "mean_area": 0.33996386366598746,
  "mean_perimeter": 2.0750544628515457,
  "mean_area_perimeter_ratio": 0.10344223370577886,
  "mean_shape_index": 1.2455165078604686,
  "mean_num_holes": 0.013645802432512608,
  "number_of_features": 3235,
  "geometry_types": [
    "Polygon",
    "MultiPolygon"
  ],
  "geometry_type_counts": {
    "Polygon": 3182,
    "MultiPolygon": 53
  },
  "total_bounds": [
    -179.230233,
    -14.601339,
    179.859681,
    71.439786
  ],
  :
  :
}
~~~~

Simplify the CAN (Canada) map (notice the reduced number of vertices).
In this example the `--path` argument is provided, which will store the
result as a new shapefile. This will produce output in the file specified
in path, as well as several similarly named files in the same directory.

~~~~
SHAPEFILE="./data/shapefiles/CAN/lcd_000b21a_e-wgs84.shp" ;
TOLERANCE=0.01 ;
PATH="./data/shapefiles/CAN/lcd_000b21a_e-wgs84-simplified.shp"

python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT simplify \
    --shapefile "$SHAPEFILE" \
    --tolerance $TOLERANCE \
    --path "$PATH"

{
  "count_polygons": 176209,
  "count_vertices": 969749,
  "mean_num_vertices": 5.503402209875772,
  "mean_area": 0.009555841400787775,
  "mean_perimeter": 0.03934500511875684,
  "mean_area_perimeter_ratio": 0.0006816130975421319,
  "mean_shape_index": 1.528548686854621,
  "mean_num_holes": 0.0005334574283946904,
  "number_of_features": 293,
  "geometry_types": [
    "MultiPolygon",
    "Polygon"
  ],
  "geometry_type_counts": {
    "Polygon": 148,
    "MultiPolygon": 145
  },
  "total_bounds": [
    -141.01807315762753,
    41.68141509505574,
    -52.619366289675774,
    83.13654909325368
  ],
  :
  :
}
~~~~

## Buffer Shapefile

Some calculations may require the shapefile to be extended around
its perimeter.  This is called "buffering". Buffering a
shapefile creates new geometries that
represent a specified distance around existing features, which
is extremely useful for a wide range of spatial analyses and
planning activities.

### About Buffering

Buffering in the context of a shapefile is a geospatial operation
used to create a zone around the features (like points, lines, or
polygons) within the shapefile. This zone is typically a uniformly
sized area or distance around the features. The buffer operation
is quite practical and widely used in geographic information
system (GIS) analyses. Here’s how it works in practical terms:

1. **Buffering Points**: If your shapefile contains points (like
locations of trees, wells, or landmarks), buffering these points
creates circles (or polygons approximating circles) around each
point. The radius of each circle is the buffer distance you specify.
For instance, creating a 10-meter buffer around trees in a park
would result in circular areas with a 10-meter radius around
each tree.

2. **Buffering Lines**: For line features (like roads, rivers,
or utility lines), buffering adds a corridor of a specified
width around each line. This buffer extends equally on both
sides of the line. For example, a 5-meter buffer around a road
would create a corridor 10 meters wide (5 meters on each side)
along the entire road.

3. **Buffering Polygons**: When buffering polygons (like
buildings, lakes, or land parcels), the operation creates a
new polygon larger than the original, with the added width
being the buffer distance. If the buffer distance is negative,
the new polygon is smaller, effectively creating an inner
buffer. For instance, buffering a lake with a 20-meter buffer
might be used to designate a protective zone around the lake.

Practical applications include:

- **Proximity Analysis**: Buffering is commonly used to identify
areas within a certain distance of a feature. For instance, you might
buffer schools and then analyze what facilities (like parks or
libraries) fall within that buffer zone.

- **Environmental Planning**: To protect sensitive areas, a buffer
zone might be created around them. For example, creating buffer
zones around wetlands to regulate construction and preserve ecosystems.

- **Safety and Regulations**: Buffer zones around utilities, such
as gas pipelines, can be used to enforce safety regulations.

- **Impact Studies**: In assessing the impact of a new development,
buffers can be used to visualize and analyze the areas that will
be affected.

Buffer zones are often visualized on maps to clearly show the
areas around features that are within a certain distance. This
can be crucial for planning, decision-making, and regulatory
compliance.

Note that buffering may nominally increase the complexity
of the shapefile.

### Buffering Examples

Buffer a shapefile using either degrees or meters units (both
distances in degrees and meters are shown - pick
the distance and units from below):
~~~~
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
DISTANCE=11100 ;
DISTANCE_UNITS="meters"
DISTANCE=0.1 ;
DISTANCE_UNITS="degrees"
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT buffer \
    --shapefile "$SHAPEFILE" \
    --distance $DISTANCE \
    --units "$DISTANCE_UNITS"

{
  "count_polygons": 664,
  "count_vertices": 365359,
  "mean_num_vertices": 550.2394578313254,
  "mean_area": 39.43415349901627,
  "mean_perimeter": 19.68996588786907,
  "mean_area_perimeter_ratio": 0.3750658795146738,
  "mean_shape_index": 1.4778930660433793,
  "mean_num_holes": 0.43373493975903615,
  "number_of_features": 256,
  "geometry_types": [
    "Polygon",
    "MultiPolygon"
  ],
  "geometry_type_counts": {
    "Polygon": 177,
    "MultiPolygon": 79
  },
  "total_bounds": [
    -179.9965232657516,
    -58.592155706532985,
    179.99998441848797,
    83.7227010613685
  ],
  :
  :
}
~~~~

## View a Raw Shapefile

Create an HTML file to view a raw shapefile.  Note that several shapefiles
are available, so you can pick the one you want to use.  When complete,
point your browser to the output path to view the output:
~~~~
SHAPEFILE="./data/shapefiles/CAN/lcd_000b21a_e-wgs84.shp" ;
SHAPEFILE="./data/shapefiles/USA/tl_2023_us_county.shp" ;
SHAPEFILE="./data/shapefiles/WORLD/world-administrative-boundaries.shp" ;
SHAPEFILE="./tmp/sample.shp" ;
OUTPUT_PATH="./tmp/view.html"
python ./src/cli/cli_shapefile.py $VERBOSE --host $HOST --port $PORT view \
    --shapefile "$SHAPEFILE" \
    --path "$OUTPUT_PATH"
~~~~

[world-administrative-boundaries.zip]: https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/world-administrative-boundaries/exports/shp?lang=en&timezone=America%2FNew_York
[1]: https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm?year=21
[2]: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html