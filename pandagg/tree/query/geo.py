from .abstract import Leaf


class GeoBoundingBox(Leaf):
    KEY = "geo_bounding_box"


class GeoDistance(Leaf):
    KEY = "geo_distance"


class GeoPolygone(Leaf):
    KEY = "geo_polygon"


class GeoShape(Leaf):
    KEY = "geo_shape"
