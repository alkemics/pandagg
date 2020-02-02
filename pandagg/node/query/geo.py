
from .abstract import SingleFieldQueryClause, LeafQueryClause


class GeoBoundingBox(SingleFieldQueryClause):
    KEY = 'geo_bounding_box'


class GeoDistance(LeafQueryClause):
    KEY = 'geo_distance'

    def __init__(self, field, location, distance, _name=None, **body):
        self.field = field
        self.location = location
        b = {field: location, 'distance': distance}
        b.update(body)
        super(GeoDistance, self).__init__(_name=_name, **b)

    @property
    def tag(self):
        return '%s, field=%s' % (self.KEY, self.field)

    @classmethod
    def deserialize(cls, **body):
        allowed_params = {'distance', 'distance_type', '_name', 'validation_method'}
        other_keys = set(body.keys()).difference(allowed_params)
        assert len(other_keys) == 1
        field_key = other_keys.pop()
        field_value = body.pop(field_key)
        return cls(field=field_key, location=field_value, **body)


class GeoPolygone(SingleFieldQueryClause):
    KEY = 'geo_polygon'


class GeoShape(SingleFieldQueryClause):
    KEY = 'geo_shape'


GEO_QUERIES = [
    GeoBoundingBox,
    GeoDistance,
    GeoPolygone,
    GeoShape
]
