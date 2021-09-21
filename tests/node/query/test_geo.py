from pandagg.query import GeoBoundingBox, GeoDistance, GeoPolygone, GeoShape


def test_geo_bounding_box_clause():
    body = {
        "pin.location": {
            "top_left": {"lat": 40.73, "lon": -74.1},
            "bottom_right": {"lat": 40.01, "lon": -71.12},
        }
    }
    expected = {"geo_bounding_box": body}

    q1 = GeoBoundingBox(
        field="pin.location",
        top_left={"lat": 40.73, "lon": -74.1},
        bottom_right={"lat": 40.01, "lon": -71.12},
    )
    q2 = GeoBoundingBox(
        pin__location={
            "top_left": {"lat": 40.73, "lon": -74.1},
            "bottom_right": {"lat": 40.01, "lon": -71.12},
        }
    )
    for q in (q1, q2):
        assert q.body == body
        assert q.to_dict() == expected
        assert q.line_repr(depth=0) == (
            "geo_bounding_box",
            'field=pin.location, bottom_right={"lat": 40.01, "lon": -71.12}, '
            'top_left={"lat": 40.73, "lon": -74.1}',
        )


def test_geo_polygone_clause():
    body = {"person.location": {"points": [[-70, 40], [-80, 30], [-90, 20]]}}
    expected = {"geo_polygon": body}

    q1 = GeoPolygone(field="person.location", points=[[-70, 40], [-80, 30], [-90, 20]])
    q2 = GeoPolygone(person__location={"points": [[-70, 40], [-80, 30], [-90, 20]]})
    for q in (q1, q2):
        assert q.body == body
        assert q.to_dict() == expected
        assert q.line_repr(depth=None) == (
            "geo_polygon",
            "field=person.location, points=[[-70, 40], [-80, 30], [-90, 20]]",
        )


def test_geo_distance_clause():
    body = {"distance": "12km", "pin.location": "drm3btev3e86"}
    expected = {"geo_distance": body}

    q = GeoDistance(pin__location="drm3btev3e86", distance="12km")
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == ("geo_distance", "field=pin.location")


def test_geo_shape():
    body = {
        "location": {
            "shape": {"type": "envelope", "coordinates": [[13.0, 53.0], [14.0, 52.0]]},
            "relation": "within",
        }
    }
    expected = {"geo_shape": body}

    q1 = GeoShape(
        field="location",
        relation="within",
        shape={"type": "envelope", "coordinates": [[13.0, 53.0], [14.0, 52.0]]},
    )
    q2 = GeoShape(
        location={
            "shape": {"type": "envelope", "coordinates": [[13.0, 53.0], [14.0, 52.0]]},
            "relation": "within",
        }
    )
    for q in (q1, q2):
        assert q.body == body
        assert q.to_dict() == expected
        assert q.line_repr(depth=None) == (
            "geo_shape",
            'field=location, relation="within", shape={"coordinates": [[13.0, 53.0], '
            '[14.0, 52.0]], "type": "envelope"}',
        )
