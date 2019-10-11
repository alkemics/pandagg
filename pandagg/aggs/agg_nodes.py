#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from pandagg.tree import Node


class AggregationNode(Node):
    """Wrapper around elasticsearch aggregation concept.
    https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations.html

    Each aggregation can be seen both a Node that can be encapsulated in a parent agg.

    Define a method to build aggregation request.
    """

    AGG_TYPE = None
    VALUE_ATTRS = NotImplementedError()
    APPLICABLE_MAPPING_TYPES = None

    def __init__(self, agg_name, agg_body, meta=None):
        self.agg_name = agg_name
        super(AggregationNode, self).__init__(identifier=self.agg_name)
        self.agg_body = agg_body
        self.meta = meta

    def build_aggregation(self, tree=None, depth=None):
        """ElasticSearch aggregation queries follow this formatting:
        {
            "<aggregation_name>" : {
                "<aggregation_type>" : {
                    <aggregation_body>
                }
                [,"meta" : {  [<meta_data_body>] } ]?
            }
        }
        """
        aggs = {self.AGG_TYPE: self.agg_body}
        if self.meta:
            aggs["meta"] = self.meta
        return {self.agg_name: aggs}

    @classmethod
    def extract_bucket_value(cls, response, value_as_dict=False):
        attrs = cls.VALUE_ATTRS
        if value_as_dict:
            return {attr_: response.get(attr_) for attr_ in attrs}
        return response.get(attrs[0])

    def __repr__(self):
        return u"<{class_}, name={name}, type={type}, body={body}>".format(
            class_=self.__class__.__name__, type=self.AGG_TYPE, name=self.agg_name, body=self.agg_body
        ).encode('utf-8')


class MetricAggregation(AggregationNode):
    """Metric aggregation are aggregations providing a single bucket, with value attributes to be extracted."""
    VALUE_ATTRS = NotImplementedError()

    @staticmethod
    def extract_buckets(response_value):
        yield (None, response_value)

    @staticmethod
    def get_filter(*args, **kwargs):
        return None

    @staticmethod
    def list_filter_keys():
        return []

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class BucketAggregationNode(AggregationNode):
    """Bucket aggregation have special abilities: they can encapsulate other aggregations as children.
    Each time, the extracted value is a 'doc_count'.

    Provide methods:
    - to build aggregation request (with children aggregations)
    - to to extract buckets from raw response
    - to build query to filter documents belonging to that bucket
    """
    VALUE_ATTRS = ['doc_count']

    def __init__(self, agg_name, agg_body, meta=None, children=None):
        super(BucketAggregationNode, self).__init__(
            agg_name=agg_name,
            agg_body=agg_body,
            meta=meta,
        )
        children = children or []
        for child in children:
            assert isinstance(child, AggregationNode)
        self.children = children

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    def build_aggregation(self, tree=None, depth=None):
        # compute also sub-aggregations
        aggs = super(BucketAggregationNode, self).build_aggregation()
        if tree is None or depth == 0:
            return aggs
        if depth is not None:
            depth -= 1
        sub_aggs = {}
        for child in tree.children(self.agg_name):
            sub_aggs.update(child.agg_dict(tree, depth))

        if sub_aggs:
            aggs[self.agg_name]['aggs'] = sub_aggs
        return aggs

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        raise NotImplementedError()

    def list_filter_keys(self):
        raise NotImplementedError()

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class ListBucketAggregation(BucketAggregationNode):

    # Aggregation that return a list of buckets as a list (terms, histogram, date-histogram).
    KEY_PATH = 'key'

    def extract_buckets(self, response_value):
        for bucket in response_value['buckets']:
            yield (bucket[self.KEY_PATH], bucket)

    def get_filter(self, key):
        raise NotImplementedError()

    def list_filter_keys(self):
        raise NotImplementedError()

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class Terms(ListBucketAggregation):
    AGG_TYPE = 'terms'
    VALUE_ATTRS = ['doc_count', 'doc_count_error_upper_bound', 'sum_other_doc_count']
    DEFAULT_SIZE = 20

    def __init__(self, agg_name, field, meta=None, missing=None, size=None, children=None):
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
            children=children
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
        if key is None:
            return None
        if key == 'missing':
            filter_ = {'bool': {'must_not': {'exists': {'field': self.field}}}}
        else:
            filter_ = {'match': {self.field: key}}
        return filter_

    def list_filter_keys(self):
        keys = ['%(value)s']
        if self.missing is not None:
            keys.append('missing')
        return keys


class Filters(BucketAggregationNode):

    AGG_TYPE = 'filters'

    def __init__(self, agg_name, filters, meta=None, children=None):
        self.filters = filters
        super(Filters, self).__init__(
            agg_name=agg_name,
            agg_body={"filters": filters},
            meta=meta,
            children=children
        )

    def extract_buckets(self, response_value):
        for key, value in response_value['buckets'].iteritems():
            yield (key, value)

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        filter_ = self.filters[key]
        return filter_

    def list_filter_keys(self):
        return self.filters.keys()

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'filters' in agg_body
        return {'filters': agg_body['filters']}


class MatchAllAggregation(Filters):

    def __init__(self, agg_name, meta=None, children=None):
        super(MatchAllAggregation, self).__init__(
            agg_name=agg_name,
            filters={'All': {'match_all': {}}},
            meta=meta,
            children=children
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return agg_body


class Histogram(ListBucketAggregation):

    AGG_TYPE = 'histogram'

    def __init__(self, agg_name, field, interval, hist_format=None, meta=None, children=None):
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
            children=children
        )

    def get_filter(self, key):
        # TODO
        return None

    def list_filter_keys(self):
        # TODO
        return []

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

    AGG_TYPE = 'date_histogram'

    ALLOWED_INTERVAL_UNITS = ('y', 'q', 'M', 'w', 'd')  # not under a day to avoid breaking ES ('h', 'm', 's')

    def __init__(self, agg_name, field, interval, meta=None, date_format="yyyy-MM-dd", use_key_as_string=True, children=None):
        self._validate_interval(interval)
        if use_key_as_string:
            self.KEY_PATH = 'key_as_string'
        super(DateHistogram, self).__init__(
            agg_name=agg_name,
            field=field,
            interval=interval,
            hist_format=date_format,
            meta=meta,
            children=children
        )

    @classmethod
    def _validate_interval(cls, interval):
        units_pattern = '(%s)' % '|'.join(cls.ALLOWED_INTERVAL_UNITS)
        full_pattern = r'\d+' + units_pattern
        pattern = re.compile(full_pattern)
        if not pattern.match(interval):
            raise ValueError('Wrong interval pattern %s for %s.' % (interval, cls.__name__))

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        kwargs = Histogram.agg_body_to_init_kwargs(agg_body)
        if 'hist_format' in kwargs:
            kwargs['date_format'] = kwargs.pop('hist_format')
        return kwargs


class UniqueBucketAggregation(BucketAggregationNode):
    """Aggregations providing a single bucket."""

    def extract_buckets(self, response_value):
        yield (None, response_value)

    def get_filter(self, key):
        return None

    def list_filter_keys(self):
        return []

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class Global(UniqueBucketAggregation):

    AGG_TYPE = 'global'

    def __init__(self, agg_name, meta=None, children=None):
        super(Global, self).__init__(
            agg_name=agg_name,
            agg_body={},
            meta=meta,
            children=children
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return {}


class Filter(UniqueBucketAggregation):

    AGG_TYPE = 'filter'

    def __init__(self, agg_name, filter_, meta=None, children=None):
        self.filter_ = filter_
        super(Filter, self).__init__(
            agg_name=agg_name,
            agg_body=filter_,
            meta=meta,
            children=children
        )

    def get_filter(self, key):
        return self.filter_

    def list_filter_keys(self):
        return ['All']

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return {'filter_': agg_body}


class Nested(UniqueBucketAggregation):

    AGG_TYPE = 'nested'
    APPLICABLE_MAPPING_TYPES = ['nested']

    def __init__(self, agg_name, path, meta=None, children=None):
        super(Nested, self).__init__(
            agg_name=agg_name,
            agg_body={"path": path},
            meta=meta,
            children=children
        )
        self.path = path

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'path' in agg_body
        return {'path': agg_body['path']}


class ReverseNested(UniqueBucketAggregation):

    AGG_TYPE = 'reverse_nested'

    def __init__(self, agg_name, path=None, meta=None, children=None):
        self.path = path
        super(ReverseNested, self).__init__(
            agg_name=agg_name,
            agg_body={"path": path} if path else {},
            meta=meta,
            children=children
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        if 'path' in agg_body:
            return {'path': agg_body['path']}
        return {}


class FieldMetricAggregation(MetricAggregation):
    """Metric aggregation based on single field."""
    VALUE_ATTRS = NotImplementedError()

    def __init__(self, agg_name, field, meta=None, **agg_body_kwargs):
        agg_body = dict(agg_body_kwargs)
        agg_body['field'] = field
        super(FieldMetricAggregation, self).__init__(
            agg_name=agg_name,
            agg_body=agg_body,
            meta=meta
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return agg_body


class Avg(FieldMetricAggregation):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'avg'


class Max(FieldMetricAggregation):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'max'


class Min(FieldMetricAggregation):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'min'


class ValueCount(FieldMetricAggregation):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'value_count'


class Cardinality(FieldMetricAggregation):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'cardinality'

    def __init__(self, agg_name, field, meta=None, precision_threshold=1000):
        # precision_threshold: the higher the more accurate but longer to proceed (default ES: 1)
        body_kwargs = {}
        if precision_threshold is not None:
            body_kwargs['precision_threshold'] = precision_threshold

        super(Cardinality, self).__init__(
            agg_name=agg_name,
            field=field,
            meta=meta,
            **body_kwargs
        )


class Stats(FieldMetricAggregation):
    VALUE_ATTRS = ['count', 'min', 'max', 'avg', 'sum']
    AGG_TYPE = 'stats'


PUBLIC_AGGS = {
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
        Avg,
        Max,
        Min,
        ValueCount,
        Cardinality,
        Stats
    ]
}
