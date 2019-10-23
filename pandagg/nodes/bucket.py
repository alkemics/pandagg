#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import re

from pandagg.mapping.types import NUMERIC_TYPES
from pandagg.nodes.abstract import ListBucketAgg, UniqueBucketAgg, BucketAggNode


class Terms(ListBucketAgg):
    """Terms aggregation.
    """
    AGG_TYPE = 'terms'
    VALUE_ATTRS = ['doc_count', 'doc_count_error_upper_bound', 'sum_other_doc_count']
    # TODO - check list of allowed/blacklisted fields
    BLACKLISTED_MAPPING_TYPES = []
    DEFAULT_SIZE = 20

    def __init__(self, agg_name, field, meta=None, missing=None, size=None, aggs=None):
        self.field = field
        self.missing = missing

        agg_body = {
            "field": field,
            "size": self.DEFAULT_SIZE if size is None else size
        }
        if missing is not None:
            agg_body["missing"] = missing

        super(Terms, self).__init__(
            agg_name=agg_name,
            agg_body=agg_body,
            meta=meta,
            aggs=aggs
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'field' in agg_body
        kwargs = {
            'field': agg_body['field']
        }
        if 'missing' in agg_body:
            kwargs['missing'] = agg_body['missing']
        if 'missing' in agg_body:
            kwargs['missing'] = agg_body['missing']
        if 'size' in agg_body:
            kwargs['size'] = agg_body['size']
        return kwargs

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        if key == 'missing':
            return {'bool': {'must_not': {'exists': {'field': self.field}}}}
        return {'term': {self.field: key}}


class Filters(BucketAggNode):

    AGG_TYPE = 'filters'

    def __init__(self, agg_name, filters, other_bucket=False, other_bucket_key=None, meta=None, aggs=None, **kwargs):
        self.filters = filters
        self.other_bucket = other_bucket
        self.other_bucket_key = other_bucket_key
        body = {
            "filters": filters,
            "other_bucket": other_bucket
        }
        if other_bucket_key is not None:
            body['other_bucket_key'] = other_bucket_key

        if kwargs:
            body.update(kwargs)

        super(Filters, self).__init__(
            agg_name=agg_name,
            agg_body=body,
            meta=meta,
            aggs=aggs
        )

    def extract_buckets(self, response_value):
        buckets = response_value['buckets']
        for key in sorted(buckets.keys()):
            yield (key, buckets[key])

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        filter_ = self.filters[key]
        return filter_

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'filters' in agg_body
        return {'filters': agg_body['filters']}


class Histogram(ListBucketAgg):

    AGG_TYPE = 'histogram'
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES

    def __init__(self, agg_name, field, interval, hist_format=None, meta=None, aggs=None):
        self.field = field
        self.interval = interval
        self.hist_format = hist_format
        body = {"field": field, "interval": interval}
        if hist_format:
            body['format'] = hist_format
        super(Histogram, self).__init__(
            agg_name=agg_name,
            agg_body=body,
            meta=meta,
            aggs=aggs
        )

    def get_filter(self, key):
        # TODO
        return None

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'field' in agg_body
        assert 'interval' in agg_body
        kwargs = {"field": agg_body['field'], "interval": agg_body['interval']}
        if 'format' in agg_body:
            kwargs['hist_format'] = agg_body['format']
        return kwargs


class DateHistogram(Histogram):
    WHITELISTED_MAPPING_TYPES = ['date']
    AGG_TYPE = 'date_histogram'
    ALLOWED_INTERVAL_UNITS = ('y', 'q', 'M', 'w', 'd')  # not under a day to avoid breaking ES ('h', 'm', 's')

    def __init__(self,
                 agg_name, field, interval, meta=None, date_format="yyyy-MM-dd", use_key_as_string=True, aggs=None):
        self._validate_interval(interval)
        super(DateHistogram, self).__init__(
            agg_name=agg_name,
            field=field,
            interval=interval,
            hist_format=date_format,
            meta=meta,
            aggs=aggs
        )
        if use_key_as_string:
            self.KEY_PATH = 'key_as_string'

    @classmethod
    def _validate_interval(cls, interval):
        units_pattern = '(%s)' % '|'.join(cls.ALLOWED_INTERVAL_UNITS)
        full_pattern = r'\d*' + units_pattern
        pattern = re.compile(full_pattern)
        if not pattern.match(interval):
            raise ValueError('Wrong interval pattern %s for %s.' % (interval, cls.__name__))

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        kwargs = Histogram.agg_body_to_init_kwargs(agg_body)
        if 'hist_format' in kwargs:
            kwargs['date_format'] = kwargs.pop('hist_format')
        return kwargs


class DateRange(ListBucketAgg):
    WHITELISTED_MAPPING_TYPES = ['date']
    AGG_TYPE = 'date_range'


class Global(UniqueBucketAgg):

    AGG_TYPE = 'global'

    def __init__(self, agg_name, meta=None, aggs=None):
        super(Global, self).__init__(
            agg_name=agg_name,
            agg_body={},
            meta=meta,
            aggs=aggs
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return {}


class Filter(UniqueBucketAgg):

    AGG_TYPE = 'filter'

    def __init__(self, agg_name, filter_, meta=None, aggs=None):
        self.filter_ = filter_
        super(Filter, self).__init__(
            agg_name=agg_name,
            agg_body=filter_,
            meta=meta,
            aggs=aggs
        )

    def get_filter(self, key):
        return self.filter_

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return {'filter_': agg_body}


class MatchAll(Filter):

    def __init__(self, agg_name, meta=None, aggs=None):
        super(MatchAll, self).__init__(
            agg_name=agg_name,
            filter_={'match_all': {}},
            meta=meta,
            aggs=aggs
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return agg_body


class Nested(UniqueBucketAgg):

    AGG_TYPE = 'nested'
    WHITELISTED_MAPPING_TYPES = ['nested']

    def __init__(self, agg_name, path, meta=None, aggs=None):
        self.path = path
        super(Nested, self).__init__(
            agg_name=agg_name,
            agg_body={"path": path},
            meta=meta,
            aggs=aggs
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'path' in agg_body
        return {'path': agg_body['path']}


class ReverseNested(UniqueBucketAgg):

    AGG_TYPE = 'reverse_nested'
    WHITELISTED_MAPPING_TYPES = ['nested']

    def __init__(self, agg_name, path=None, meta=None, aggs=None):
        self.path = path
        super(ReverseNested, self).__init__(
            agg_name=agg_name,
            agg_body={"path": path} if path else {},
            meta=meta,
            aggs=aggs
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        if 'path' in agg_body:
            return {'path': agg_body['path']}
        return {}


BUCKET_AGGS = {
    agg.AGG_TYPE: agg
    for agg in [
        Terms,
        Filters,
        Histogram,
        DateHistogram,
        Global,
        Filter,
        Nested,
        ReverseNested,
    ]
}
