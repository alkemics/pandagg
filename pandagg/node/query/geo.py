from typing import Any, Optional, Tuple

from .abstract import KeyFieldQueryClause, AbstractSingleFieldQueryClause
from pandagg.types import DistanceType, ValidationMethod


class GeoBoundingBox(KeyFieldQueryClause):
    KEY = "geo_bounding_box"


class GeoDistance(AbstractSingleFieldQueryClause):
    KEY = "geo_distance"

    def __init__(self, distance: str, **body: Any) -> None:
        # pop all allowed args to find out which keyword is used as field
        _name: Optional[str] = body.pop("_name", None)
        distance_type: Optional[DistanceType] = body.pop("distance_type", None)
        validation_method: Optional[ValidationMethod] = body.pop(
            "validation_method", None
        )
        if len(body) != 1:
            raise ValueError("Wrong declaration: %s" % body)
        field, location = self.expand__to_dot(body).popitem()
        super(GeoDistance, self).__init__(
            _name=_name,
            field=field,
            distance=distance,
            distance_type=distance_type,
            validation_method=validation_method,
            **{field: location}
        )

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return self.KEY, "field=%s" % self.field


class GeoPolygone(KeyFieldQueryClause):
    KEY = "geo_polygon"


class GeoShape(KeyFieldQueryClause):
    KEY = "geo_shape"
