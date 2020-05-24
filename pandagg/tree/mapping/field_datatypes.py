"""https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html"""

from .mapping import Mapping


# CORE DATATYPES
# string
class Text(Mapping):
    KEY = "text"


class Keyword(Mapping):
    KEY = "keyword"


# numeric
class Long(Mapping):
    KEY = "long"


class Integer(Mapping):
    KEY = "integer"


class Short(Mapping):
    KEY = "short"


class Byte(Mapping):
    KEY = "byte"


class Double(Mapping):
    KEY = "double"


class Float(Mapping):
    KEY = "float"


class HalfFloat(Mapping):
    KEY = "half_float"


class ScaledFloat(Mapping):
    KEY = "scaled_float"


# date
class Date(Mapping):
    KEY = "date"


class DateNanos(Mapping):
    KEY = "date_nanos"


# boolean
class Boolean(Mapping):
    KEY = "boolean"


# binary
class Binary(Mapping):
    KEY = "binary"


# range
class IntegerRange(Mapping):
    KEY = "integer_range"


class FloatRange(Mapping):
    KEY = "float_range"


class LongRange(Mapping):
    KEY = "long_range"


class DoubleRange(Mapping):
    KEY = "double_range"


class DateRange(Mapping):
    KEY = "date_range"


# COMPLEX DATATYPES
class Object(Mapping):
    KEY = "object"
    _display_pattern = " {%s}"


class Nested(Mapping):
    KEY = "nested"
    _display_pattern = " [%s]"


# GEO DATATYPES
class GeoPoint(Mapping):
    """For lat/lon points"""

    KEY = "geo_point"


class GeoShape(Mapping):
    """For complex shapes like polygons"""

    KEY = "geo_shape"


# SPECIALIZED DATATYPES
class IP(Mapping):
    """for IPv4 and IPv6 addresses"""

    KEY = "IP"


class Completion(Mapping):
    """To provide auto-complete suggestions"""

    KEY = "completion"


class TokenCount(Mapping):
    """To count the number of tokens in a string"""

    KEY = "token_count"


class MapperMurMur3(Mapping):
    """To compute hashes of values at index-time and store them in the index"""

    KEY = "murmur3"


class MapperAnnotatedText(Mapping):
    """To index text containing special markup (typically used for identifying named entities)"""

    KEY = "annotated-text"


class Percolator(Mapping):
    """Accepts queries from the query-dsl"""

    KEY = "percolator"


class Join(Mapping):
    """Defines parent/child relation for documents within the same index"""

    KEY = "join"


class RankFeature(Mapping):
    """Record numeric feature to boost hits at query time."""

    KEY = "rank_feature"


class RankFeatures(Mapping):
    """Record numeric features to boost hits at query time."""

    KEY = "rank_features"


class DenseVector(Mapping):
    """Record dense vectors of float values."""

    KEY = "dense_vector"


class SparseVector(Mapping):
    """Record sparse vectors of float values."""

    KEY = "sparse_vector"


class SearchAsYouType(Mapping):
    """A text-like field optimized for queries to implement as-you-type completion"""

    KEY = "search_as_you_type"


class Alias(Mapping):
    """Defines an alias to an existing field."""

    KEY = "alias"


class Flattened(Mapping):
    """Allows an entire JSON object to be indexed as a single field."""

    KEY = "flattened"


class Shape(Mapping):
    """For arbitrary cartesian geometries."""

    KEY = "shape"


class Histogram(Mapping):
    """For pre-aggregated numerical values for percentiles aggregations."""

    KEY = "histogram"
