"""Not implemented aggregations include:
- children
- parent
- sampler
- diversified-sampler
- multi-terms
- significant text
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
        super(ReverseNested, self).__init__(meta=meta, path=path, **body)


class Missing(UniqueBucketAgg):
    KEY = "missing"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, field: str, meta: Meta = None, **body: Any):
        self.field: str = field
        super(Missing, self).__init__(field=field, meta=meta, **body)


class Sampler(UniqueBucketAgg):
    KEY = "sampler"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, shard_size: Optional[int] = None, **body: Any) -> None:
        super(Sampler, self).__init__(shard_size=shard_size, **body)


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
        super(Terms, self).__init__(
            field=field, missing=missing, size=size, meta=meta, **body
        )

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
        super(Filters, self).__init__(
            filters=filters,
            other_bucket=other_bucket,
            other_bucket_key=other_bucket_key,
            meta=meta,
            **body
        )


class AdjacencyMatrix(MultipleBucketAgg):

    KEY = "adjacency_matrix"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        filters: Dict[str, QueryClauseDict],
        separator: Optional[str] = None,
        meta: Optional[Meta] = None,
        **body: Any
    ) -> None:
        super(AdjacencyMatrix, self).__init__(
            filters=filters, separator=separator, meta=meta, **body
        )


class Histogram(MultipleBucketAgg):

    KEY = "histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(
        self, field: str, interval: int, meta: Optional[Meta] = None, **body: Any
    ) -> None:
        self.field: str = field
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
        super(DateHistogram, self).__init__(
            field=field,
            interval=interval,
            calendar_interval=calendar_interval,
            fixed_interval=fixed_interval,
            meta=meta,
            key_as_string=key_as_string,
            **body
        )

    def is_convertible_to_composite_source(self) -> bool:
        return True


class VariableWidthHistogram(MultipleBucketAgg):
    KEY = "variable_width_histogram"
    VALUE_ATTRS = ["doc_count", "min", "max"]

    def __init__(self, field: str, buckets: int, **body: Any) -> None:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-variablewidthhistogram-aggregation.html

        Note: This aggregation cannot currently be nested under any aggregation that collects from more than a single
        bucket.
        """
        self.field = field
        super(VariableWidthHistogram, self).__init__(
            field=field, buckets=buckets, **body
        )


class AutoDateHistogram(MultipleBucketAgg):
    KEY = "auto_date_histogram"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        field: str,
        buckets: Optional[int] = None,
        format: Optional[str] = None,
        time_zone: Optional[str] = None,
        minimum_interval: Optional[str] = None,
        missing: Optional[str] = None,
        meta: Optional[Meta] = None,
        key_as_string: bool = True,
        **body: Any
    ) -> None:
        self.field: str = field
        super(AutoDateHistogram, self).__init__(
            field=field,
            buckets=buckets,
            format=format,
            time_zone=time_zone,
            minimum_interval=minimum_interval,
            missing=missing,
            meta=meta,
            key_as_string=key_as_string,
            **body
        )


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
        super(Range, self).__init__(
            field=field, ranges=ranges, keyed=keyed, meta=meta, **body
        )


class DateRange(Range):
    KEY = "date_range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["date"]


class IPRange(Range):
    KEY = "ip_range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["ip"]


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
        super(Range, self).__init__(
            field=field,
            origin=origin,
            ranges=ranges,
            unit=unit,
            distance_type=distance_type,
            keyed=keyed,
            meta=meta,
            **body
        )


class GeoHashGrid(MultipleBucketAgg):
    KEY = "geohash_grid"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["geo_point", "geo_shape"]

    def __init__(
        self,
        field: str,
        precision: Optional[int] = None,
        bounds: Optional[Dict] = None,
        size: Optional[int] = None,
        shard_size: Optional[int] = None,
        **body: Any
    ) -> None:
        self.field = field
        super(GeoHashGrid, self).__init__(
            field=field,
            precision=precision,
            bounds=bounds,
            size=size,
            shard_size=shard_size,
            **body
        )


class GeoTileGrid(MultipleBucketAgg):
    KEY = "geotile_grid"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["geo_point", "geo_shape"]

    def __init__(
        self,
        field: str,
        precision: Optional[int] = None,
        bounds: Optional[Dict] = None,
        size: Optional[int] = None,
        shard_size: Optional[int] = None,
        **body: Any
    ) -> None:
        self.field = field
        super(GeoTileGrid, self).__init__(
            field=field,
            precision=precision,
            bounds=bounds,
            size=size,
            shard_size=shard_size,
            **body
        )


class SignificantTerms(MultipleBucketAgg):
    KEY = "significant_terms"
    VALUE_ATTRS = ["doc_count", "score", "bg_count"]

    def __init__(self, field: str, **body: Any) -> None:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html
        """
        self.field = field
        super(SignificantTerms, self).__init__(field=field, **body)


class RareTerms(MultipleBucketAgg):
    KEY = "rare_terms"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        field: str,
        max_doc_count: Optional[int] = None,
        precision: Optional[float] = None,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        missing: Optional[Any] = None,
        **body: Any
    ) -> None:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-rare-terms-aggregation.html
        """
        self.field = field
        super(RareTerms, self).__init__(
            field=field,
            max_doc_count=max_doc_count,
            precision=precision,
            include=include,
            exclude=exclude,
            missing=missing,
            **body
        )
