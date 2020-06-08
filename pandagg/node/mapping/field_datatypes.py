"""https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html"""

from .abstract import UnnamedComplexField, UnnamedRegularField


# CORE DATATYPES
# string
class Text(UnnamedRegularField):
    KEY = "text"


class Keyword(UnnamedRegularField):
    KEY = "keyword"


# numeric
class Long(UnnamedRegularField):
    KEY = "long"


class Integer(UnnamedRegularField):
    KEY = "integer"


class Short(UnnamedRegularField):
    KEY = "short"


class Byte(UnnamedRegularField):
    KEY = "byte"


class Double(UnnamedRegularField):
    KEY = "double"


class Float(UnnamedRegularField):
    KEY = "float"


class HalfFloat(UnnamedRegularField):
    KEY = "half_float"


class ScaledFloat(UnnamedRegularField):
    KEY = "scaled_float"


# date
class Date(UnnamedRegularField):
    KEY = "date"


class DateNanos(UnnamedRegularField):
    KEY = "date_nanos"


# boolean
class Boolean(UnnamedRegularField):
    KEY = "boolean"


# binary
class Binary(UnnamedRegularField):
    KEY = "binary"


# range
class IntegerRange(UnnamedRegularField):
    KEY = "integer_range"


class FloatRange(UnnamedRegularField):
    KEY = "float_range"


class LongRange(UnnamedRegularField):
    KEY = "long_range"


class DoubleRange(UnnamedRegularField):
    KEY = "double_range"


class DateRange(UnnamedRegularField):
    KEY = "date_range"


# COMPLEX DATATYPES
class Object(UnnamedComplexField):
    KEY = "object"


class Nested(UnnamedComplexField):
    KEY = "nested"


# GEO DATATYPES
class GeoPoint(UnnamedRegularField):
    """For lat/lon points"""

    KEY = "geo_point"


class GeoShape(UnnamedRegularField):
    """For complex shapes like polygons"""

    KEY = "geo_shape"


# SPECIALIZED DATATYPES
class IP(UnnamedRegularField):
    """for IPv4 and IPv6 addresses"""

    KEY = "IP"


class Completion(UnnamedRegularField):
    """To provide auto-complete suggestions"""

    KEY = "completion"


class TokenCount(UnnamedRegularField):
    """To count the number of tokens in a string"""

    KEY = "token_count"


class MapperMurMur3(UnnamedRegularField):
    """To compute hashes of values at index-time and store them in the index"""

    KEY = "murmur3"


class MapperAnnotatedText(UnnamedRegularField):
    """To index text containing special markup (typically used for identifying named entities)"""

    KEY = "annotated-text"


class Percolator(UnnamedRegularField):
    """Accepts queries from the query-dsl"""

    KEY = "percolator"


class Join(UnnamedRegularField):
    """Defines parent/child relation for documents within the same index"""

    KEY = "join"


class RankFeature(UnnamedRegularField):
    """Record numeric feature to boost hits at query time."""

    KEY = "rank_feature"


class RankFeatures(UnnamedRegularField):
    """Record numeric features to boost hits at query time."""

    KEY = "rank_features"


class DenseVector(UnnamedRegularField):
    """Record dense vectors of float values."""

    KEY = "dense_vector"


class SparseVector(UnnamedRegularField):
    """Record sparse vectors of float values."""

    KEY = "sparse_vector"


class SearchAsYouType(UnnamedRegularField):
    """A text-like field optimized for queries to implement as-you-type completion"""

    KEY = "search_as_you_type"


class Alias(UnnamedRegularField):
    """Defines an alias to an existing field."""

    KEY = "alias"


class Flattened(UnnamedRegularField):
    """Allows an entire JSON object to be indexed as a single field."""

    KEY = "flattened"


class Shape(UnnamedRegularField):
    """For arbitrary cartesian geometries."""

    KEY = "shape"


class Histogram(UnnamedRegularField):
    """For pre-aggregated numerical values for percentiles aggregations."""

    KEY = "histogram"
