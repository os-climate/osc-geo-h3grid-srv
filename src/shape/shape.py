# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
from typing import Optional, Set, Tuple, List

import geopandas
import h3
import numpy as np
from geopandas import GeoDataFrame
from pandas import Series
from shapely import Polygon, MultiPolygon, Point
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

# Set up logging
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


class Shape:

    def __init__(
            self,
            shapefile: str
    ):
        """
        Initialize class

        :param shapefile:
            The path to the shapefile
        """

        self.shapefile = shapefile
        self.gdf: GeoDataFrame = geopandas.read_file(self.shapefile)
        self.repository = None
        self.connection = None

    def gdf(self):
        return self.gdf

    def shapefile(self):
        return self.shapefile

    def statistics(self):
        """
        Return statistics for a shapefile
        """
        gdf: GeoDataFrame = self.gdf

        # Count polygons in each MultiPolygon and total polygons
        polygons_in_multipolygon = {}
        count_polygons = 0

        for idx, row in gdf.iterrows():
            geom = row.geometry
            if geom.geom_type == 'MultiPolygon':
                num_polygons = sum(1 for _ in geom.geoms)
                polygons_in_multipolygon[idx] = num_polygons
                count_polygons += num_polygons
            elif geom.geom_type == 'Polygon':
                count_polygons += 1
        # common_stats['polygons_in_multipolygon'] = polygons_in_multipolygon

        # Get attribute list
        attributes_list = gdf.columns.tolist()

        # Calcuate attribute-specific statistics
        attribute_stats = {}
        for attribute in attributes_list:
            if gdf[attribute].dtype == 'object':
                # Categorical data
                attribute_stats[attribute] = {
                    'unique_values': gdf[attribute].nunique(),
                    'value_counts': gdf[attribute].value_counts().to_dict()
                }
            elif gdf[attribute].dtype in ['int64', 'float64']:
                # Numerical data
                attribute_stats[attribute] = {
                    'mean': gdf[attribute].mean(),
                    'median': gdf[attribute].median(),
                    'max': gdf[attribute].max(),
                    'min': gdf[attribute].min()
                }
        attribute_stats = self._convert_values(attribute_stats)

        # Calculate commplexity statistics

        # Initialize lists to store metric values for all geometries
        num_vertices_list = []
        area_list = []
        perimeter_list = []
        area_perimeter_ratio_list = []
        shape_index_list = []
        num_holes_list = []

        count_vertices = 0
        for geom in gdf['geometry']:
            if geom.geom_type == 'Polygon':
                polygons = [geom]
            elif geom.geom_type == 'MultiPolygon':
                polygons = geom.geoms
            else:
                continue  # Skip non-polygon geometries

            for poly in polygons:
                num_vertices = len(poly.exterior.coords)
                count_vertices += num_vertices
                area = poly.area
                perimeter = poly.length
                area_perimeter_ratio = area / perimeter if perimeter != 0 else 0
                shape_index = perimeter / (
                            2 * np.sqrt(np.pi * area)) if area != 0 else 0
                num_holes = len(poly.interiors)

                # Append each metric to its respective list
                num_vertices_list.append(num_vertices)
                area_list.append(area)
                perimeter_list.append(perimeter)
                area_perimeter_ratio_list.append(area_perimeter_ratio)
                shape_index_list.append(shape_index)
                num_holes_list.append(num_holes)

        # Calculating the mean for each list
        mean_num_vertices = sum(num_vertices_list) / len(num_vertices_list)
        mean_area = sum(area_list) / len(area_list)
        mean_perimeter = sum(perimeter_list) / len(perimeter_list)
        mean_area_perimeter_ratio = sum(area_perimeter_ratio_list) / len(
            area_perimeter_ratio_list)
        mean_shape_index = sum(shape_index_list) / len(shape_index_list)
        mean_num_holes = sum(num_holes_list) / len(num_holes_list)

        stats = {
            "coordinate_system": self.gdf.crs,
            "count_polygons": count_polygons,
            "count_vertices": count_vertices,
            "mean_num_vertices": mean_num_vertices,
            "mean_area": mean_area,
            "mean_perimeter": mean_perimeter,
            "mean_area_perimeter_ratio": mean_area_perimeter_ratio,
            "mean_shape_index": mean_shape_index,
            "mean_num_holes": mean_num_holes,
            "number_of_features": len(gdf),
            "geometry_types": gdf.geometry.type.unique().tolist(),
            "geometry_type_counts": gdf.geometry.type.value_counts().to_dict(),
            "total_bounds": gdf.total_bounds.tolist(),
            "attributes": attributes_list,
            "attribute_stats": attribute_stats
        }
        return stats

    def view(self, path: str):
        """
        Create an HTML file containing a view of the shapefile
        and its polygons
        """
        logger.info("Viewing shapefile")
        import folium

        # Convert the GeoDataFrame to a GeoJSON format
        geojson_data = self.gdf.to_json()

        # Create a map object centered around the
        #  mean of the shapefile's coordinates
        map_center = [self.gdf.geometry.centroid.y.mean(),
                      self.gdf.geometry.centroid.x.mean()]
        m = folium.Map(location=map_center, zoom_start=12)

        # Add the GeoJSON layer to the map
        folium.GeoJson(geojson_data).add_to(m)

        # Save the map to an HTML file
        m.save(path)

    def simplify(self, tolerance: float):
        """
        Simplify a shapefile, which will reduce the
        number of polygons and vertices in a shapefile
        thereby reducing it size and complexity (which
        requires less CPU to process) .
        """

        logger.info(f"Simplifying tolerance:{tolerance}")

        # Ensure tolerance is non-negative
        if tolerance < 0:
            raise ValueError("Tolerance must be non-negative.")

        self.gdf["geometry"] = self.gdf["geometry"].simplify(
            tolerance,
            preserve_topology=True
        )
        return self.statistics()

    def buffer(self, distance: float, units: str):
        """
        Create a buffer of a distance in the provided units
        around a shapefile.  The distance units are one of
        'meters' or 'degrees'.  Note that one degree is approximately
        111km at the equator.
        """
        logger.info(f"Buffering distance:{distance} units:{units}")

        # Ensure distance is non-negative
        if distance < 0:
            raise ValueError("Distance must be non-negative.")

        # Units must be "degrees" or "meters"
        valid_units = ["degrees", "meters"]
        if units not in valid_units:
            raise ValueError(f"Units must be one of:{valid_units}")

        # Convert distance to meters
        actual_distance = distance
        if units == "degrees":
            logger.info("Input distance in degrees, converting to meters")
            # one degree at equator is 111km
            actual_distance = 111 * 1000 * distance
        logger.info(f"Actual distance (meters):{actual_distance}")

        original_crs = self.gdf.crs
        logger.info(f"Original coordinate system:{original_crs}")

        # Convert geometry to a linear unit (from degrees)
        # To ensure buffer does not give weird results near poles
        # due to an angle of latitude covering a smaller distance near poles
        utm_crs = 'EPSG:32633'
        logger.info(f"Converting to coordinate system:{utm_crs}")
        self.gdf = self.gdf.to_crs(utm_crs)

        # Fix (buffer(0))
        logger.info("Fixing transformed geometry")
        self.gdf = self._fix_invalid_geometries(self.gdf)
        invalid_geometries = self.gdf[~self.gdf['geometry'].is_valid]
        if not invalid_geometries.empty:
            logger.info(
                f"Found invalid original geometries:{invalid_geometries}")

        # Execute buffer
        logger.info("Buffering geometry")
        self.gdf['geometry'] = self.gdf['geometry'].buffer(actual_distance)
        self.gdf = self._fix_invalid_geometries(self.gdf)
        invalid_geometries = self.gdf[~self.gdf['geometry'].is_valid]
        if not invalid_geometries.empty:
            logger.info(
                f"Found invalid buffered geometries:{invalid_geometries}")

        # Convert back to original geometry
        logger.info(f"Converting to coordinate system:{original_crs}")
        self.gdf = self.gdf.to_crs(original_crs)

        # Fix (buffer(0))
        logger.info("Fixing updated geometry")
        self.gdf['geometry'] = self.gdf['geometry'].apply(
            lambda geom: geom.buffer(0) if not geom.is_valid else geom)
        self.gdf = self._fix_invalid_geometries(self.gdf)
        invalid_geometries = self.gdf[~self.gdf['geometry'].is_valid]
        if not invalid_geometries.empty:
            logger.info(f"Found invalid fional geometries:{invalid_geometries}")

        return self.statistics()

    def save(self, path: str):
        """
        Save the shapefile represented by an instance
        of this class. Note that one can simplify a
        shapefile and/or buffer it and then save its
        processed form for subsequent processing.
        """
        logger.info(f"Saving shapefile to path:{path}")
        self.gdf.to_file(path)

    def transform_to_epsg_4326(self) -> None:
        """
        Transform the data of this Shape into EPSG:4326 format
        If already in EPSG:4326 format, no changes will occur.

        :return: no return value
        :rtype: None
        """
        epsg = "EPSG:4326"
        if self.gdf.crs == epsg:
            logger.info(f"Coordinate system is already: {epsg}")
        else:
            self.gdf = self.gdf.to_crs(epsg=4326)

    def get_h3_in_shape(
            self,
            buffer: float,
            resolution: int,
            reverse_coords: bool = True,
            region: Optional[str] = None,
            min_longitude: Optional[float] = None,
            max_longitude: Optional[float] = None,
            min_latitude: Optional[float] = None,
            max_latitude: Optional[float] = None,
    ) -> Set[str]:
        gdf = self._filter_lat_long(
            self.gdf,
            min_longitude,
            max_longitude,
            min_latitude,
            max_latitude
        )

        if region is not None:
            gdf = gdf[gdf.name == region]

        if reverse_coords:
            gdf = gdf['geometry'].apply(self._reverse_coordinates)

        cells = self._get_h3_cells(gdf, buffer, resolution)

        return cells

    def get_max_lat_long(
            self,
            region: Optional[str] = None
    ) -> Tuple[float, float, float, float]:
        """
        Gets the maximum and minimum latitudes/longitudes that are within
        the specified shapefile/region
        :param region:
            The region in the shapefile to get lat/long for. If absent
            every region in the shapefile will be included in the check.
        :type region: Optional[str]
        :return:
            tuple of [min longitude, min latitude, max longitude,
            max latitude]
        :rtype: Tuple[float, float, float, float]
        """
        if region is not None:
            gdf = self.gdf[self.gdf.name == region]
        else:
            gdf = self.gdf
        (min_long, min_lat, max_long, max_lat) = 99999, 99999, -99999, -99999
        for geometry in gdf.geometry:
            (this_min_long, this_min_lat, this_max_long, this_max_lat) = \
                geometry.bounds
            if this_min_long < min_long:
                min_long = this_min_long
            if this_max_long > max_long:
                max_long = this_max_long
            if this_min_lat < min_lat:
                min_lat = this_min_lat
            if this_max_lat > max_lat:
                max_lat = this_max_lat
        return min_long, min_lat, max_long, max_lat

    def point_within_shape(
            self,
            latitude: float,
            longitude: float,
            region: Optional[str] = None
    ):
        if region is not None:
            gdf = self.gdf[self.gdf.name == region]
        else:
            gdf = self.gdf

        # beware weirdness in latitude/longitude order
        #  as geopandas vs shapefile sometimes use different order
        point = Point(latitude, longitude)

        contained = False
        for geo in gdf.geometry:
            if geo.contains(point):
                contained = True
                break
        return contained

    def dataframe_points_within_shape(
            self,
            data_geodf: GeoDataFrame,
            region: Optional[str],

    ):
        if region is not None:
            reduced_shape_df = self.gdf[self.gdf.name == region]
        else:
            reduced_shape_df = self.gdf
        return geopandas.tools.sjoin(data_geodf, reduced_shape_df, how='inner')

    def contains_region(self, region: str) -> bool:
        return region in self.gdf['name'].values

    def _fix_invalid_geometries(self, gdf):
        """
        Fixes invalid geometries in a GeoDataFrame.

        Parameters:
        gdf (GeoDataFrame):
            The GeoDataFrame with potentially invalid geometries.

        Returns:
        GeoDataFrame: A GeoDataFrame with fixed geometries.
        """

        def fix_geometry(geom):
            import shapely
            if geom is None or geom.is_empty:
                # logger.info("Empty geom")
                return shapely.geometry.Polygon()
            elif not geom.is_valid:
                # logger.info("Invalid geom")
                # Suppress specific runtime warnings from Shapely (these
                # log messages are emitted and are a nuisance, since
                # the next line 'buffer(0)' fixes them)
                import warnings
                warnings.filterwarnings("ignore", category=RuntimeWarning,
                                        module="shapely")

                geom = geom.buffer(0)
                # logger.info(f"Buffered geom state:{geom.is_valid}")
                return geom
            else:
                return geom

        gdf['geometry'] = gdf['geometry'].apply(fix_geometry)
        return gdf

    def _convert_values(self, val):
        if isinstance(val, (np.int64, np.int32)):
            return int(val)
        elif isinstance(val, (np.float64, np.float32)):
            return float(val)
        elif isinstance(val, dict):
            return {k: self._convert_values(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [self._convert_values(x) for x in val]
        if isinstance(val, int):
            return val
        if isinstance(val, float):
            return val
        if isinstance(val, str):
            return val
        else:
            logger.info(f"Unknown value type: {type(val)} value:{val}")
            return val

    def _filter_lat_long(
            self,
            gdf: GeoDataFrame,
            min_longitude: Optional[float] = None,
            max_longitude: Optional[float] = None,
            min_latitude: Optional[float] = None,
            max_latitude: Optional[float] = None,
    ) -> GeoDataFrame:
        out_geo = gdf

        if min_longitude is not None:
            if min_longitude > 180 or min_longitude < -180:
                raise ValueError(
                    "minimum longitude must be in range [-180, 180]")
            out_geo = gdf.cx[min_longitude:, :]

        if max_longitude is not None:
            if max_longitude > 180 or max_longitude < -180:
                raise ValueError(
                    "maximum longitude must be in range [-180, 180]")
            out_geo = gdf.cx[:max_longitude, :]

        if min_latitude is not None:
            if min_latitude > 90 or min_latitude < -90:
                raise ValueError(
                    "minimum latitude must be in range [-90, 90]")
            out_geo = gdf.cx[:, min_latitude:]

        if max_latitude is not None:
            if max_latitude > 90 or max_latitude < -90:
                raise ValueError(
                    "maximum latitude must be in range [-90, 90]")
            out_geo = gdf.cx[:, :max_latitude]

        return out_geo

    def _reverse_coordinates(self, shape):
        # Swap the coordinates from (lon, lat) to (lat, lon)
        if isinstance(shape, Polygon):
            return Polygon([(y, x) for x, y in shape.exterior.coords])
        elif isinstance(shape, MultiPolygon):
            polygons = [Polygon([(y, x) for x, y in poly.exterior.coords])
                        for poly in shape.geoms]
            return MultiPolygon(polygons)
        else:
            # Apply similar logic for other geometry types if needed
            print("unknown shape")
            raise ValueError("unknown shape")
            # logger.info(f"Unknown shape:{shape}")

    def _get_h3_cells(
            self,
            gdf: GeoDataFrame,
            buffer: float,
            resolution: int
    ) -> Set[str]:
        cells = set()

        for original_geom in gdf.geometry:
            geom = original_geom.buffer(buffer)

            # Process Polygon geometries
            if isinstance(geom, Polygon):
                geojson = shape(geom).__geo_interface__
                overlap_cells = h3.polyfill(geojson, resolution)
                cells.update(overlap_cells)

            # Break down MultiPolygon geometries into Polygons
            elif isinstance(geom, MultiPolygon):
                for poly in geom.geoms:
                    geojson = shape(poly).__geo_interface__
                    overlap_cells = h3.polyfill(geojson, resolution)
                    cells.update(overlap_cells)

        return cells

