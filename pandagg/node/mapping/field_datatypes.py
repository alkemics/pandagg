"""https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html"""

from .abstract import Field, ComplexField, NumericField


# CORE DATATYPES
# string
class Text(Field):
    KEY = "text"


class Keyword(Field):
    KEY = "keyword"


# numeric
class Long(NumericField):
    KEY = "long"


class Integer(NumericField):
    KEY = "integer"


class Short(NumericField):
    KEY = "short"


class Byte(NumericField):
    KEY = "byte"


class Double(NumericField):
    KEY = "double"


class Float(NumericField):
    KEY = "float"


class HalfFloat(NumericField):
    KEY = "half_float"


class ScaledFloat(NumericField):
    KEY = "scaled_float"


# date
class Date(NumericField):
    KEY = "date"


class DateNanos(NumericField):
    KEY = "date_nanos"


# boolean
class Boolean(NumericField):
    KEY = "boolean"


# binary
class Binary(Field):
    KEY = "binary"


# range
class IntegerRange(Field):
    KEY = "integer_range"


class FloatRange(Field):
    KEY = "float_range"


class LongRange(Field):
    KEY = "long_range"


class DoubleRange(Field):
    KEY = "double_range"


class DateRange(Field):
    KEY = "date_range"


# COMPLEX DATATYPES
class Object(ComplexField):
    KEY = "object"
    _display_pattern = " {%s}"


class Nested(ComplexField):
    KEY = "nested"
    _display_pattern = " [%s]"


# GEO DATATYPES
class GeoPoint(Field):
    """For lat/lon points"""

    KEY = "geo_point"


class GeoShape(Field):
    """For complex shapes like polygons"""

    KEY = "geo_shape"


# SPECIALIZED DATATYPES
class IP(Field):
    """for IPv4 and IPv6 addresses"""

    KEY = "IP"


class Completion(Field):
    """To provide auto-complete suggestions"""

    KEY = "completion"


class TokenCount(Field):
    """To count the number of tokens in a string"""

    KEY = "token_count"


class MapperMurMur3(Field):
    """To compute hashes of values at index-time and store them in the index"""

    KEY = "murmur3"


class MapperAnnotatedText(Field):
    """To index text containing special markup (typically used for identifying named entities)"""

    KEY = "annotated-text"


class Percolator(Field):
    """Accepts queries from the query-dsl"""

    KEY = "percolator"


class Join(Field):
    """Defines parent/child relation for documents within the same index"""

    KEY = "join"


class RankFeature(Field):
    """Record numeric feature to boost hits at query time."""

    KEY = "rank_feature"


class RankFeatures(Field):
    """Record numeric features to boost hits at query time."""

    KEY = "rank_features"


class DenseVector(Field):
    """Record dense vectors of float values."""

    KEY = "dense_vector"


class SparseVector(Field):
    """Record sparse vectors of float values."""

    KEY = "sparse_vector"


class SearchAsYouType(Field):
    """A text-like field optimized for queries to implement as-you-type completion"""

    KEY = "search_as_you_type"


class Alias(Field):
    """Defines an alias to an existing field."""

    KEY = "alias"


class Flattened(Field):
    """Allows an entire JSON object to be indexed as a single field."""

    KEY = "flattened"


class Shape(Field):
    """For arbitrary cartesian geometries."""

    KEY = "shape"


class Histogram(Field):
    """For pre-aggregated numerical values for percentiles aggregations."""

    KEY = "histogram"
