#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Not implemented aggregations include:
- children agg
- geo-distance
- geo-hash grid
- ipv4
- sampler
- significant terms
"""

from builtins import str as text

from pandagg.node.types import NUMERIC_TYPES
from pandagg.node.aggs.abstract import MultipleBucketAgg, UniqueBucketAgg


class Global(UniqueBucketAgg):

    KEY = "global"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, name, meta=None):
        super(Global, self).__init__(name=name, agg_body={}, meta=meta)

    def get_filter(self, key):
        return None


class Filter(UniqueBucketAgg):

    KEY = "filter"
    VALUE_ATTRS = ["doc_count"]

    def __init__(self, name, filter=None, meta=None, **kwargs):
        if (filter is not None) != (not kwargs):
            raise ValueError(
                'Filter aggregation requires exactly one of "filter" or "kwargs"'
            )
        if filter:
            filter_ = filter.copy()
        else:
            filter_ = kwargs.copy()
        self.filter = filter_
        super(Filter, self).__init__(name=name, meta=meta, **filter_)

    def get_filter(self, key):
        return self.filter


class MatchAll(Filter):
    def __init__(self, name, meta=None):
        super(MatchAll, self).__init__(name=name, filter={"match_all": {}}, meta=meta)


class Nested(UniqueBucketAgg):

    KEY = "nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, name, path, meta=None):
        self.path = path
        super(Nested, self).__init__(name=name, path=path, meta=meta)

    def get_filter(self, key):
        return None


class ReverseNested(UniqueBucketAgg):

    KEY = "reverse_nested"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["nested"]

    def __init__(self, name, path=None, meta=None, **body):
        self.path = path
        body_kwargs = dict(body)
        if path:
            body_kwargs["path"] = path
        super(ReverseNested, self).__init__(name=name, meta=meta, **body_kwargs)

    def get_filter(self, key):
        return None


class Missing(UniqueBucketAgg):
    KEY = "missing"
    VALUE_ATTRS = ["doc_count"]
    BLACKLISTED_MAPPING_TYPES = []

    def __init__(self, name, field, meta=None, **body):
        super(UniqueBucketAgg, self).__init__(name=name, field=field, meta=meta, **body)

    def get_filter(self, key):
        return {"bool": {"must_not": {"exists": {"field": self.field}}}}


class Terms(MultipleBucketAgg):
    """Terms aggregation.
    """

    KEY = "terms"
    VALUE_ATTRS = ["doc_count", "doc_count_error_upper_bound", "sum_other_doc_count"]
    BLACKLISTED_MAPPING_TYPES = []

    def __init__(self, name, field, missing=None, size=None, meta=None, **body):
        self.field = field
        self.missing = missing
        self.size = size

        body_kwargs = dict(body)
        if missing is not None:
            body_kwargs["missing"] = missing
        if size is not None:
            body_kwargs["size"] = size

        super(Terms, self).__init__(name=name, field=field, meta=meta, **body_kwargs)

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        if key == "missing":
            return {"bool": {"must_not": {"exists": {"field": self.field}}}}
        return {"term": {self.field: {"value": key}}}


class Filters(MultipleBucketAgg):

    KEY = "filters"
    VALUE_ATTRS = ["doc_count"]
    DEFAULT_OTHER_KEY = "_other_"
    IMPLICIT_KEYED = True

    def __init__(
        self,
        name,
        filters,
        other_bucket=False,
        other_bucket_key=None,
        meta=None,
        **body
    ):
        self.filters = filters
        self.other_bucket = other_bucket
        self.other_bucket_key = other_bucket_key
        body_kwargs = dict(body)
        if other_bucket:
            body_kwargs["other_bucket"] = other_bucket
        if other_bucket_key:
            body_kwargs["other_bucket_key"] = other_bucket_key

        super(Filters, self).__init__(
            name=name, filters=filters, meta=meta, **body_kwargs
        )

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        if key in self.filters.keys():
            return self.filters[key]
        if self.other_bucket:
            if key == self.DEFAULT_OTHER_KEY or key == self.other_bucket_key:
                return {
                    "bool": {
                        "must_not": {"bool": {"should": list(self.filters.values())}}
                    }
                }
        raise ValueError("Unkown <%s> key in <Agg %s>" % (key, self.name))


class Histogram(MultipleBucketAgg):

    KEY = "histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(self, name, field, interval, meta=None, **body):
        self.field = field
        self.interval = interval
        super(Histogram, self).__init__(
            name=name, field=field, interval=interval, meta=meta, **body
        )

    def get_filter(self, key):
        try:
            key = float(key)
        except (TypeError, ValueError):
            raise ValueError(
                "Filter key of an histogram aggregation must be numeric, git <%s> of type <%s>"
                % (key, type(key))
            )
        return {"range": {self.field: {"gte": key, "lt": key + self.interval}}}


class DateHistogram(MultipleBucketAgg):
    KEY = "date_histogram"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = ["date"]
    # interval is deprecated from 7.2 in favor of calendar_interval and fixed interval

    def __init__(
        self,
        name,
        field,
        interval=None,
        calendar_interval=None,
        fixed_interval=None,
        meta=None,
        keyed=False,
        key_as_string=True,
        **body
    ):
        """Date Histogram aggregation.
        Note: interval is deprecated from 7.2 in favor of calendar_interval and fixed interval
        :param keyed: defines returned buckets format: if True, as dict.
        :param key_as_string: if True extracted key of bucket will be the formatted date (applicable if keyed=False)
        """
        self.field = field
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
            name=name,
            field=field,
            meta=meta,
            keyed=keyed,
            key_path="key_as_string" if key_as_string else "key",
            **body
        )

    def get_filter(self, key):
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#date-math
        return {
            "range": {self.field: {"gte": key, "lt": "%s||+%s" % (key, self.interval)}}
        }


class Range(MultipleBucketAgg):
    KEY = "range"
    VALUE_ATTRS = ["doc_count"]
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    KEY_SEP = "-"

    def __init__(self, name, field, ranges, keyed=False, meta=None, **body):
        self.field = field
        self.ranges = ranges
        body_kwargs = dict(body)
        if keyed:
            self.bucket_key_suffix = "_as_string"
        else:
            self.bucket_key_suffix = None
        super(Range, self).__init__(
            name=name, field=field, ranges=ranges, meta=meta, keyed=keyed, **body_kwargs
        )

    @property
    def from_key(self):
        if self.bucket_key_suffix:
            return "from%s" % self.bucket_key_suffix
        return "from"

    @property
    def to_key(self):
        if self.bucket_key_suffix:
            return "to%s" % self.bucket_key_suffix
        return "to"

    def _extract_bucket_key(self, bucket):
        if self.from_key in bucket:
            key = "%s%s" % (bucket[self.from_key], self.KEY_SEP)
        else:
            key = "*-"
        if self.to_key in bucket:
            key += text(bucket[self.to_key])
        else:
            key += "*"
        return key

    def get_filter(self, key):
        from_, to_ = key.split(self.KEY_SEP)
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
    KEY_SEP = "::"

    def __init__(self, name, field, key_as_string=True, meta=None, **body):
        self.key_as_string = key_as_string
        super(DateRange, self).__init__(
            name=name, field=field, keyed=True, meta=meta, **body
        )


class Composite(MultipleBucketAgg):
    KEY = "composite"

    def get_filter(self, key):
        raise NotImplementedError()
