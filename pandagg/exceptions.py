from __future__ import unicode_literals


class InvalidAggregation(Exception):
    """Wrong aggregation definition"""


class MappingError(Exception):
    """Basic Mapping Error"""

    pass


class AbsentMappingFieldError(MappingError):
    """Field is not present in mapping."""

    pass


class InvalidOperationMappingFieldError(MappingError):
    """Invalid aggregation type on this mapping field."""

    pass


class VersionIncompatibilityError(Exception):
    """Pandagg is not compatible with this ElasticSearch version.
    """

    pass
