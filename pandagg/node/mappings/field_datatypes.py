"""https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html"""

from .abstract import ComplexField, RegularField


# CORE DATATYPES
# string
class Text(RegularField):
    KEY = "text"


class Keyword(RegularField):
    KEY = "keyword"


class ConstantKeyword(RegularField):
    KEY = "constant_keyword"


class WildCard(RegularField):
    KEY = "wildcard"


# numeric
class Long(RegularField):
    KEY = "long"


class Integer(RegularField):
    KEY = "integer"


class Short(RegularField):
    KEY = "short"


class Byte(RegularField):
    KEY = "byte"


class Double(RegularField):
    KEY = "double"


class Float(RegularField):
    KEY = "float"


class HalfFloat(RegularField):
    KEY = "half_float"


class ScaledFloat(RegularField):
    KEY = "scaled_float"


# date
class Date(RegularField):
    KEY = "date"


class DateNanos(RegularField):
    KEY = "date_nanos"


# boolean
class Boolean(RegularField):
    KEY = "boolean"


# binary
class Binary(RegularField):
    KEY = "binary"


# range
class IntegerRange(RegularField):
    KEY = "integer_range"


class FloatRange(RegularField):
    KEY = "float_range"


class LongRange(RegularField):
    KEY = "long_range"


class DoubleRange(RegularField):
    KEY = "double_range"


class DateRange(RegularField):
    KEY = "date_range"


class IpRange(RegularField):
    KEY = "ip_range"


# COMPLEX DATATYPES
class Object(ComplexField):
    KEY = "object"


class Nested(ComplexField):
    KEY = "nested"


# GEO DATATYPES
class GeoPoint(RegularField):
    """For lat/lon points"""

    KEY = "geo_point"


class GeoShape(RegularField):
    """For complex shapes like polygons"""

    KEY = "geo_shape"


# SPECIALIZED DATATYPES
class IP(RegularField):
    """for IPv4 and IPv6 addresses"""

    KEY = "ip"


class Completion(RegularField):
    """To provide auto-complete suggestions"""

    KEY = "completion"


class TokenCount(RegularField):
    """To count the number of tokens in a string"""

    KEY = "token_count"


class MapperMurMur3(RegularField):
    """To compute hashes of values at index-time and store them in the index"""

    KEY = "murmur3"


class MapperAnnotatedText(RegularField):
    """To index text containing special markup (typically used for identifying named entities)"""

    KEY = "annotated-text"


class Percolator(RegularField):
    """Accepts queries from the query-dsl"""

    KEY = "percolator"


class Join(RegularField):
    """Defines parent/child relation for documents within the same index"""

    KEY = "join"


class RankFeature(RegularField):
    """Record numeric feature to boost hits at query time."""

    KEY = "rank_feature"


class RankFeatures(RegularField):
    """Record numeric features to boost hits at query time."""

    KEY = "rank_features"


class DenseVector(RegularField):
    """Record dense vectors of float values."""

    KEY = "dense_vector"


class SparseVector(RegularField):
    """Record sparse vectors of float values."""

    KEY = "sparse_vector"


class SearchAsYouType(RegularField):
    """A text-like field optimized for queries to implement as-you-type completion"""

    KEY = "search_as_you_type"


class Alias(RegularField):
    """Defines an alias to an existing field."""

    KEY = "alias"


class Flattened(RegularField):
    """Allows an entire JSON object to be indexed as a single field."""

    KEY = "flattened"


class Shape(RegularField):
    """For arbitrary cartesian geometries."""

    KEY = "shape"


class Histogram(RegularField):
    """For pre-aggregated numerical values for percentiles aggregations."""

    KEY = "histogram"
