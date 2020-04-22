from .abstract import KeyFieldQueryClause, AbstractSingleFieldQueryClause


class GeoBoundingBox(KeyFieldQueryClause):
    KEY = "geo_bounding_box"


class GeoDistance(AbstractSingleFieldQueryClause):
    KEY = "geo_distance"

    def __init__(self, distance, **body):
        # pop all allowed args to find out which keyword is used as field
        _name = body.pop("_name", None)
        distance_type = body.pop("distance_type", None)
        validation_method = body.pop("validation_method", None)
        if len(body) != 1:
            raise ValueError("Wrong declaration: %s" % body)
        field, location = self.expand__to_dot(body).popitem()
        self.field = field
        b = {field: location}
        if distance_type is not None:
            b["distance_type"] = distance_type
        if validation_method is not None:
            b["validation_method"] = validation_method
        super(GeoDistance, self).__init__(
            _name=_name, field=field, distance=distance, **b
        )

    def line_repr(self, depth, **kwargs):
        return "%s, field=%s" % (self.KEY, self.field)


class GeoPolygone(KeyFieldQueryClause):
    KEY = "geo_polygon"


class GeoShape(KeyFieldQueryClause):
    KEY = "geo_shape"
