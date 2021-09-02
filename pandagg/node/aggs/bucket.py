"""Not implemented aggregations include:
- children agg
- geo-distance
- geo-hash grid
- ipv4
- sampler
- significant terms
"""


from pandagg.node.types import NUMERIC_TYPES
from pandagg.node.aggs.abstract import MultipleBucketAgg, UniqueBucketAgg
from pandagg.types import (
    Meta,
    BucketKey,
    QueryClauseDict,
    RangeDict,
    BucketDict,
    BucketKeyAtom,
)
from typing import Any, Optional, Dict, Union, List


class Global(UniqueBucketAgg):

    KEY = "global"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, meta: Meta = None):
        super(Global, self).__init__(agg_body={}, meta=meta)

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        return None


class Filter(UniqueBucketAgg):

    KEY = "filter"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self, filter: Optional[Dict[str, Any]] = None, meta: Meta = None, **body: Any
    ):
        if (filter is not None) != (not body):
            raise ValueError(
                'Filter aggregation requires exactly one of "filter" or "body"'
            )
        if filter:
            filter_ = filter.copy()
        else:
            filter_ = body.copy()
        self.filter = filter_
        super(Filter, self).__init__(meta=meta, **filter_)

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        return self.filter


class MatchAll(Filter):
    def __init__(self, meta: Meta = None, **body: Any):
        super(MatchAll, self).__init__(filter={"match_all": {}}, meta=meta, **body)


class Nested(UniqueBucketAgg):

    KEY = "nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, path: str, meta: Meta = None, **body: Any):
        self.path = path
        super(Nested, self).__init__(path=path, meta=meta, **body)

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        return None


class ReverseNested(UniqueBucketAgg):

    KEY = "reverse_nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, path: Optional[str] = None, meta: Meta = None, **body: Any):
        self.path = path
        body_kwargs = dict(body)
        if path:
            body_kwargs["path"] = path
        super(ReverseNested, self).__init__(meta=meta, **body_kwargs)

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        return None


class Missing(UniqueBucketAgg):
    KEY = "missing"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, field: str, meta: Meta = None, **body: Any):
        self.field: str = field
        super(UniqueBucketAgg, self).__init__(field=field, meta=meta, **body)

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        return {"bool": {"must_not": {"exists": {"field": self.field}}}}


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

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        """Provide filter to get documents belonging to document of given key."""
        if key == "missing":
            return {"bool": {"must_not": {"exists": {"field": self.field}}}}
        return {"term": {self.field: {"value": key}}}

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

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        """Provide filter to get documents belonging to document of given key."""
        key_: str = key  # type: ignore
        if key_ in self.filters.keys():
            return self.filters[key_]
        if self.other_bucket:
            if key_ == self.DEFAULT_OTHER_KEY or key_ == self.other_bucket_key:
                return {
                    "bool": {
                        "must_not": {"bool": {"should": list(self.filters.values())}}
                    }
                }
        raise ValueError("Unkown <%s> key" % key_)


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

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        key_: BucketKeyAtom = key  # type: ignore
        try:
            key_ = float(key_)  # type: ignore
        except (TypeError, ValueError):
            raise ValueError(
                "Filter key of an histogram aggregation must be numeric, git <%s> of type <%s>"
                % (key_, type(key_))
            )
        return {"range": {self.field: {"gte": key_, "lt": key_ + self.interval}}}

    def is_convertible_to_composite_source(self) -> bool:
        return True


class DateHistogram(MultipleBucketAgg):
    KEY = "date_histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["date"]
    # interval is deprecated from 7.2 in favor of calendar_interval and fixed interval

    def __init__(
        self,
        field: str,
        interval: str = None,
        calendar_interval: str = None,
        fixed_interval: str = None,
        meta: Meta = None,
        keyed: bool = False,
        key_as_string: bool = True,
        **body: Any
    ) -> None:
        """Date Histogram aggregation.
        Note: interval is deprecated from 7.2 in favor of calendar_interval and fixed interval
        :param keyed: defines returned buckets format: if True, as dict.
        :param key_as_string: if True extracted key of bucket will be the formatted date (applicable if keyed=False)
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
            keyed=keyed,
            key_path="key_as_string" if key_as_string else "key",
            **body
        )

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#date-math
        return {
            "range": {self.field: {"gte": key, "lt": "%s||+%s" % (key, self.interval)}}
        }

    def is_convertible_to_composite_source(self) -> bool:
        return True


class Range(MultipleBucketAgg):
    KEY = "range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    KEY_SEP: str = "-"

    bucket_key_suffix: Optional[str]

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
        self.bucket_key_suffix: Optional[str]

        body_kwargs = dict(body)
        if keyed:
            self.bucket_key_suffix = "_as_string"
        else:
            self.bucket_key_suffix = None
        super(Range, self).__init__(
            field=field, ranges=ranges, meta=meta, keyed=keyed, **body_kwargs
        )

    @property
    def from_key(self) -> str:
        if self.bucket_key_suffix:
            return "from%s" % self.bucket_key_suffix
        return "from"

    @property
    def to_key(self) -> str:
        if self.bucket_key_suffix:
            return "to%s" % self.bucket_key_suffix
        return "to"

    def _extract_bucket_key(self, bucket: BucketDict) -> BucketKey:
        if self.from_key in bucket:
            key = "%s%s" % (bucket[self.from_key], self.KEY_SEP)
        else:
            key = "*-"
        if self.to_key in bucket:
            key += str(bucket[self.to_key])
        else:
            key += "*"
        return key

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        key_: str = key  # type: ignore
        from_, to_ = key_.split(self.KEY_SEP)
        inner = {}
        if from_ != "*":
            inner["gte"] = from_
        if to_ != "*":
            inner["lt"] = to_
        return {"range": {self.field: inner}}


class DateRange(Range):
    KEY = "date_range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["date"]
    # cannot use range '-' separator since some keys contain it
    KEY_SEP: str = "::"

    def __init__(
        self,
        field: str,
        key_as_string: bool = True,
        meta: Optional[Meta] = None,
        **body: Any
    ) -> None:
        self.key_as_string: bool = key_as_string

        super(DateRange, self).__init__(field=field, keyed=True, meta=meta, **body)
