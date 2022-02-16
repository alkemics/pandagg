# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket.html

from typing import Any, Optional, Dict, Union, List

from pandagg.node.types import NUMERIC_TYPES
from pandagg.node.aggs.abstract import MultipleBucketAgg, UniqueBucketAgg
from pandagg.types import Meta, QueryClauseDict, RangeDict, DistanceType, ExecutionHint


class Global(UniqueBucketAgg):

    KEY = "global"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, **body: Any) -> None:
        super(Global, self).__init__(**body)


class Filter(UniqueBucketAgg):

    KEY = "filter"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        filter: Optional[QueryClauseDict] = None,
        meta: Optional[Meta] = None,
        **body: Any
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
    KEY = "match_all"

    def __init__(self, **body: Any):
        super(MatchAll, self).__init__(filter={"match_all": {}}, **body)


class Nested(UniqueBucketAgg):

    KEY = "nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, path: str, **body: Any):
        self.path: str = path
        super(Nested, self).__init__(path=path, **body)


class ReverseNested(UniqueBucketAgg):

    KEY = "reverse_nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, path: Optional[str] = None, **body: Any) -> None:
        self.path: Optional[str] = path
        super(ReverseNested, self).__init__(path=path, **body)


class Missing(UniqueBucketAgg):
    KEY = "missing"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, field: str, **body: Any) -> None:
        self.field: str = field
        super(Missing, self).__init__(field=field, **body)


class Sampler(UniqueBucketAgg):
    KEY = "sampler"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, shard_size: Optional[int] = None, **body: Any) -> None:
        super(Sampler, self).__init__(shard_size=shard_size, **body)


class DiversifiedSampler(UniqueBucketAgg):
    KEY = "diversified_sampler"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        field: str,
        shard_size: Optional[int],
        max_docs_per_value: Optional[int] = None,
        execution_hint: Optional[ExecutionHint] = None,
        **body: Any
    ) -> None:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-diversified-sampler-aggregation.html
        """
        self.field = field
        super(DiversifiedSampler, self).__init__(
            shard_size=shard_size,
            field=field,
            max_docs_per_value=max_docs_per_value,
            execution_hint=execution_hint,
            **body
        )


class Children(UniqueBucketAgg):
    KEY = "children"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, type: str, **body: Any) -> None:
        super(Children, self).__init__(type=type, **body)


class Parent(UniqueBucketAgg):
    KEY = "parent"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, type: str, **body: Any) -> None:
        super(Parent, self).__init__(type=type, **body)


class Terms(MultipleBucketAgg):
    """Terms aggregation."""

    KEY = "terms"
    VALUE_ATTRS = ["doc_count", "doc_count_error_upper_bound", "sum_other_doc_count"]

    def __init__(
        self,
        field: str,
        missing: Optional[Union[int, str]] = None,
        size: Optional[int] = None,
        **body: Any
    ) -> None:
        self.field: str = field
        super(Terms, self).__init__(field=field, missing=missing, size=size, **body)

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
        **body: Any
    ) -> None:
        super(Filters, self).__init__(
            filters=filters,
            other_bucket=other_bucket,
            other_bucket_key=other_bucket_key,
            **body
        )


class AdjacencyMatrix(MultipleBucketAgg):

    KEY = "adjacency_matrix"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        filters: Dict[str, QueryClauseDict],
        separator: Optional[str] = None,
        **body: Any
    ) -> None:
        super(AdjacencyMatrix, self).__init__(
            filters=filters, separator=separator, **body
        )


class Histogram(MultipleBucketAgg):

    KEY = "histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(self, field: str, interval: int, **body: Any) -> None:
        self.field: str = field
        super(Histogram, self).__init__(field=field, interval=interval, **body)

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
            key_as_string=key_as_string,
            **body
        )


class Range(MultipleBucketAgg):
    KEY = "range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(
        self, field: str, ranges: List[RangeDict], keyed: bool = False, **body: Any
    ) -> None:
        self.field: str = field
        super(Range, self).__init__(field=field, ranges=ranges, keyed=keyed, **body)


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
        **body: Any
    ) -> None:
        super(Range, self).__init__(
            field=field,
            origin=origin,
            ranges=ranges,
            unit=unit,
            distance_type=distance_type,
            keyed=keyed,
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


class SignificantText(MultipleBucketAgg):
    KEY = "significant_text"
    VALUE_ATTRS = ["doc_count", "score", "bg_count"]
    WHITELISTED_MAPPING_TYPES = ["text"]

    def __init__(self, field: str, **body: Any) -> None:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significanttext-aggregation.html
        """
        self.field = field
        super(SignificantText, self).__init__(field=field, **body)


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


class MultiTerms(MultipleBucketAgg):
    KEY = "multi_terms"
    VALUE_ATTRS = ["doc_count", "doc_count_error_upper_bound", "sum_other_doc_count"]

    def __init__(self, terms: List[Dict], **body: Any) -> None:
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-multi-terms-aggregation.html
        """
        super(MultiTerms, self).__init__(terms=terms, key_as_string=True, **body)
