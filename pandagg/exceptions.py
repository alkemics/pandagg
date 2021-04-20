class InvalidAggregation(Exception):
    """Wrong aggregation definition"""


class MappingError(Exception):
    """Basic Mappings Error"""

    pass


class AbsentMappingFieldError(MappingError):
    """Field is not present in mappings."""

    pass


class InvalidOperationMappingFieldError(MappingError):
    """Invalid aggregation type on this mappings field."""

    pass


class VersionIncompatibilityError(Exception):
    """Pandagg is not compatible with this ElasticSearch version."""

    pass
