"""
The module has geojson helper methods.
"""

import geojson
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry


class GeojsonUtil:
    """
    Class with geojson helper methods.
    """


    @staticmethod
    def geojson_to_polygon(geo_json: str) -> BaseGeometry:
        """
        returns Polygon/Multi Polygon for given geo_json,
        else throws error.
        :param str geo_json: input geojson
        :return: returns Polygon/MultiPolygon based on geojson type.
        """
        shape_json = geojson.loads(geo_json)
        if shape_json.type == 'Polygon':
            return Polygon(shape_json.coordinates[0])

        elif shape_json.type == 'MultiPolygon':
            polygons = list(shape_json.coordinates)
            return MultiPolygon(polygons, context_type='geojson')

        else:
            raise ValueError("Expected geojson type Polygon or MultiPolygon, but provided: %s",
                             shape_json.type)


    @staticmethod
    def geojson_to_bounding_rect(geo_json: str) -> Polygon:
        """
        returns bounding rectangle polygon for given geo_json.
        :param str geo_json: input geojson string
        :return: returns Polygon object of bounding rectangle for input geo_json
        :rtype: Polygon
        """
        polygon = GeojsonUtil.geojson_to_polygon(geo_json)

        lon_min, lat_min, lon_max, lat_max = polygon.bounds
        return Polygon.from_bounds(lon_min, lat_min, lon_max, lat_max)


    @staticmethod
    def bounding_rect_to_geojson(polygon: Polygon) -> dict:
        """
        returns json string of bounding rectangle for input polygon.
        :param Polygon polygon: input aoi polygon
        :return: returns dict for bounding rectangle geojson.
        :rtype: dict
        """
        envelope_lon_min, envelope_lat_min, envelope_lon_max, envelope_lat_max = polygon.bounds

        bounding_rect_coordinates = [
            [envelope_lon_min, envelope_lat_min],
            [envelope_lon_min, envelope_lat_max],
            [envelope_lon_max, envelope_lat_max],
            [envelope_lon_max, envelope_lat_min],
            [envelope_lon_min, envelope_lat_min]
        ]

        return {"type": "Polygon", "coordinates": [bounding_rect_coordinates]}
