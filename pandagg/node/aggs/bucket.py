"""Not implemented aggregations include:
- children agg
- geo-hash grid
- ipv4
- sampler
- significant terms
"""

from typing import Any, Optional, Dict, Union, List

from pandagg.node.types import NUMERIC_TYPES
from pandagg.node.aggs.abstract import MultipleBucketAgg, UniqueBucketAgg
from pandagg.types import Meta, QueryClauseDict, RangeDict, DistanceType


class Global(UniqueBucketAgg):

    KEY = "global"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, meta: Meta = None):
        super(Global, self).__init__(agg_body={}, meta=meta)


class Filter(UniqueBucketAgg):

    KEY = "filter"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self, filter: Optional[QueryClauseDict] = None, meta: Meta = None, **body: Any
    ):
        if (filter is not None) != (not body):
            raise ValueError(
                'Filter aggregation requires exactly one of "filter" or "body"'
            )
        if filter:
            filter_ = filter.copy()
        else:
            filter_ = body.copy()
        self.filter: QueryClauseDict = filter_
        super(Filter, self).__init__(meta=meta, **filter_)


class MatchAll(Filter):
    def __init__(self, meta: Meta = None, **body: Any):
        super(MatchAll, self).__init__(filter={"match_all": {}}, meta=meta, **body)


class Nested(UniqueBucketAgg):

    KEY = "nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, path: str, meta: Meta = None, **body: Any):
        self.path: str = path
        super(Nested, self).__init__(path=path, meta=meta, **body)


class ReverseNested(UniqueBucketAgg):

    KEY = "reverse_nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, path: Optional[str] = None, meta: Meta = None, **body: Any):
        self.path: Optional[str] = path
        body_kwargs = dict(body)
        if path:
            body_kwargs["path"] = path
        super(ReverseNested, self).__init__(meta=meta, **body_kwargs)


class Missing(UniqueBucketAgg):
    KEY = "missing"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, field: str, meta: Meta = None, **body: Any):
        self.field: str = field
        super(UniqueBucketAgg, self).__init__(field=field, meta=meta, **body)


class Terms(MultipleBucketAgg):
    """Terms aggregation."""

    KEY = "terms"
    VALUE_ATTRS = ["doc_count", "doc_count_error_upper_bound", "sum_other_doc_count"]

    def __init__(
        self,
        field: str,
        missing: Optional[Union[int, str]] = None,
        size: Optional[int] = None,
        meta: Optional[Meta] = None,
        **body: Any
    ) -> None:
        self.field: str = field
        self.missing: Optional[Union[int, str]] = missing
        self.size: Optional[int] = size

        body_kwargs = dict(body)
        if missing is not None:
            body_kwargs["missing"] = missing
        if size is not None:
            body_kwargs["size"] = size

        super(Terms, self).__init__(field=field, meta=meta, **body_kwargs)

    def is_convertible_to_composite_source(self) -> bool:
        # TODO: elasticsearch documentation is unclear about which body clauses are accepted as a source, for now just
        # sure that 'include'/'exclude' are not supported as composite source:
        # https://github.com/elastic/elasticsearch/issues/50368
        if "include" in self.body or "exclude" in self.body:
            return False
        return True


class Filters(MultipleBucketAgg):

    KEY = "filters"
    VALUE_ATTRS = ["doc_count"]
    DEFAULT_OTHER_KEY = "_other_"
    IMPLICIT_KEYED = True

    def __init__(
        self,
        filters: Dict[str, QueryClauseDict],
        other_bucket: bool = False,
        other_bucket_key: Optional[str] = None,
        meta: Optional[Meta] = None,
        **body: Any
    ) -> None:
        self.filters: Dict[str, QueryClauseDict] = filters
        self.other_bucket: bool = other_bucket
        self.other_bucket_key: Optional[str] = other_bucket_key

        body_kwargs = dict(body)
        if other_bucket:
            body_kwargs["other_bucket"] = other_bucket
        if other_bucket_key:
            body_kwargs["other_bucket_key"] = other_bucket_key

        super(Filters, self).__init__(filters=filters, meta=meta, **body_kwargs)


class Histogram(MultipleBucketAgg):

    KEY = "histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(
        self, field: str, interval: int, meta: Optional[Meta] = None, **body: Any
    ) -> None:
        self.field: str = field
        self.interval: int = interval
        super(Histogram, self).__init__(
            field=field, interval=interval, meta=meta, **body
        )

    def is_convertible_to_composite_source(self) -> bool:
        return True


class DateHistogram(MultipleBucketAgg):
    KEY = "date_histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["date"]

    def __init__(
        self,
        field: str,
        interval: str = None,
        calendar_interval: str = None,
        fixed_interval: str = None,
        meta: Meta = None,
        key_as_string: bool = True,
        **body: Any
    ) -> None:
        """Date Histogram aggregation.
        :param key_as_string: if True extracted key of bucket will be the formatted date

        Note: interval is deprecated from 7.2 in favor of calendar_interval and fixed interval
        """
        self.field: str = field

        if not (interval or fixed_interval or calendar_interval):
            raise ValueError(
                'One of "interval", "calendar_interval" or "fixed_interval" must be provided.'
            )
        if interval:
            body["interval"] = interval
        if calendar_interval:
            body["calendar_interval"] = calendar_interval
        if fixed_interval:
            body["fixed_interval"] = fixed_interval

        self.interval = interval or calendar_interval or fixed_interval
        super(DateHistogram, self).__init__(
            field=field,
            meta=meta,
            key_path="key_as_string" if key_as_string else "key",
            **body
        )

    def is_convertible_to_composite_source(self) -> bool:
        return True


class Range(MultipleBucketAgg):
    KEY = "range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(
        self,
        field: str,
        ranges: List[RangeDict],
        keyed: bool = False,
        meta: Optional[Meta] = None,
        **body: Any
    ) -> None:
        self.field: str = field
        self.ranges: List[RangeDict] = ranges
        super(Range, self).__init__(
            field=field, ranges=ranges, meta=meta, keyed=keyed, **body
        )


class DateRange(Range):
    KEY = "date_range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["date"]


class GeoDistance(Range):
    KEY = "geo_distance"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["geo_point"]

    def __init__(
        self,
        field: str,
        origin: str,
        ranges: List[RangeDict],
        unit: Optional[str] = None,
        distance_type: Optional[DistanceType] = None,
        keyed: bool = False,
        meta: Optional[Meta] = None,
        **body: Any
    ) -> None:
        if unit is not None:
            body["unit"] = unit
        if distance_type is not None:
            body["distance_type"] = distance_type
        super(Range, self).__init__(
            field=field, ranges=ranges, origin=origin, meta=meta, keyed=keyed, **body
        )
