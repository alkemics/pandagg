from pandagg.document import DocumentSource
from pandagg.index import DeclarativeIndex
from pandagg.mappings import Text, Keyword, Integer, GeoPoint, Date


class Inspection(DocumentSource):
    name = Text()
    borough = Keyword()
    cuisine = Keyword()
    grade = Keyword()
    score = Integer()
    location = GeoPoint()
    inspection_date = Date(format="MM/dd/yyyy")


class NYCRestaurants(DeclarativeIndex):
    name = "nyc-restaurants"
    document = Inspection
    # Note: "mappings" attribute is optional if "document" is provided
    mappings = {
        "properties": {
            "name": {"type": "text"},
            "borough": {"type": "keyword"},
            "cuisine": {"type": "keyword"},
            "grade": {"type": "keyword"},
            "score": {"type": "integer"},
            "location": {"type": "geo_point"},
            "inspection_date": {"type": "date", "format": "MM/dd/yyyy"},
        }
    }
    settings = {"number_of_shards": 1}
