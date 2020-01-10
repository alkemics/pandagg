
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.query import GeoBoundingBox, GeoDistance, GeoPolygone, GeoShape


class GeoQueriesTestCase(TestCase):

    def test_geo_bounding_box_clause(self):
        body = {
            "pin.location": {
                "top_left": {
                    "lat": 40.73,
                    "lon": -74.1
                },
                "bottom_right": {
                    "lat": 40.01,
                    "lon": -71.12
                }
            }
        }
        expected = {'geo_bounding_box': body}

        q = GeoBoundingBox(
            field='pin.location',
            top_left={
                "lat": 40.73,
                "lon": -74.1
            },
            bottom_right={
                "lat": 40.01,
                "lon": -71.12
            }
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'geo_bounding_box, field=pin.location')

        deserialized = GeoBoundingBox.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'geo_bounding_box, field=pin.location')

    def test_geo_polygone_clause(self):
        body = {
            "person.location": {
                "points": [
                    [-70, 40],
                    [-80, 30],
                    [-90, 20]
                ]
            }
        }
        expected = {'geo_polygon': body}

        q = GeoPolygone(
            field='person.location',
            points=[
                [-70, 40],
                [-80, 30],
                [-90, 20]
            ]
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'geo_polygon, field=person.location')

        deserialized = GeoPolygone.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'geo_polygon, field=person.location')

    def test_geo_distance_clause(self):
        body = {
            "distance": "12km",
            "pin.location": "drm3btev3e86"
        }
        expected = {'geo_distance': body}

        q = GeoDistance(
            field='pin.location',
            distance="12km",
            location='drm3btev3e86'
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'geo_distance, field=pin.location')

        deserialized = GeoDistance.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'geo_distance, field=pin.location')

    def test_geo_shape(self):
        body = {
            "location": {
                "shape": {
                    "type": "envelope",
                    "coordinates" : [[13.0, 53.0], [14.0, 52.0]]
                },
                "relation": "within"
            }
        }
        expected = {'geo_shape': body}

        q = GeoShape(
            field='location',
            relation="within",
            shape={
                "type": "envelope",
                "coordinates": [[13.0, 53.0], [14.0, 52.0]]
            }
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'geo_shape, field=location')

        deserialized = GeoShape.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'geo_shape, field=location')
