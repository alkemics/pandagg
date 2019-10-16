#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from pandagg.tree import Node


class AggNode(Node):
    """Wrapper around elasticsearch aggregation concept.
    https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations.html

    Each aggregation can be seen both a Node that can be encapsulated in a parent agg.

    Define a method to build aggregation request.
    """

    AGG_TYPE = NotImplementedError()
    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = NotImplementedError()
    WHITELISTED_MAPPING_TYPES = None
    BLACKLISTED_MAPPING_TYPES = None

    def __init__(self, agg_name, agg_body, meta=None):
        self.agg_name = agg_name
        super(AggNode, self).__init__(identifier=self.agg_name)
        self.agg_body = agg_body
        self.meta = meta

    def agg_dict(self, tree=None, depth=None):
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

    def get_filter(self, key):
        """Return filter query to list documents having this aggregation key.
        :param key: string
        :return: elasticsearch filter query
        """
        raise NotImplementedError()

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


class MetricAgg(AggNode):
    """Metric aggregation are aggregations providing a single bucket, with value attributes to be extracted."""
    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = True

    @staticmethod
    def extract_buckets(response_value):
        yield (None, response_value)

    def get_filter(self, key):
        return None

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class BucketAggNode(AggNode):
    """Bucket aggregation have special abilities: they can encapsulate other aggregations as children.
    Each time, the extracted value is a 'doc_count'.

    Provide methods:
    - to build aggregation request (with children aggregations)
    - to to extract buckets from raw response
    - to build query to filter documents belonging to that bucket

    Note: the children attribute's only purpose is for initiation with the following syntax:
    >>> from pandagg.nodes import Terms, Avg
    >>> agg = Terms(
    >>>     agg_name='term_agg',
    >>>     field='some_path',
    >>>     aggs=[
    >>>         Avg(agg_name='avg_agg', field='some_other_path')
    >>>     ]
    >>> )
    Yet, the children attribute will then be reset to None to avoid confusion since the real hierarchy is stored in the
    bpointer/fpointer attributes inherited from treelib.Tree class.
    """
    VALUE_ATTRS = ['doc_count']
    SINGLE_BUCKET = NotImplementedError()

    def __init__(self, agg_name, agg_body, meta=None, aggs=None):
        super(BucketAggNode, self).__init__(
            agg_name=agg_name,
            agg_body=agg_body,
            meta=meta,
        )
        aggs = aggs or []
        for child in aggs:
            assert isinstance(child, AggNode)
        self.aggs = aggs

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    def agg_dict(self, tree=None, depth=None):
        # TODO - reintegrate this in Agg class
        # compute also sub-aggregations
        aggs = super(BucketAggNode, self).agg_dict()
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

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class ListBucketAgg(BucketAggNode):

    # Aggregation that return a list of buckets as a list (terms, histogram, date-histogram).
    KEY_PATH = 'key'
    SINGLE_BUCKET = False

    def extract_buckets(self, response_value):
        for bucket in response_value['buckets']:
            yield (bucket[self.KEY_PATH], bucket)

    def get_filter(self, key):
        raise NotImplementedError()

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


class Terms(ListBucketAgg):
    """Terms aggregation.
    """
    AGG_TYPE = 'terms'
    VALUE_ATTRS = ['doc_count', 'doc_count_error_upper_bound', 'sum_other_doc_count']
    WHITELISTED_MAPPING_TYPES = ['string', 'boolean', 'integer']
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
        return {'match': {self.field: key}}


class Filters(BucketAggNode):

    AGG_TYPE = 'filters'

    def __init__(self, agg_name, filters, meta=None, aggs=None):
        self.filters = filters
        super(Filters, self).__init__(
            agg_name=agg_name,
            agg_body={"filters": filters},
            meta=meta,
            aggs=aggs
        )

    def extract_buckets(self, response_value):
        for key, value in response_value['buckets'].iteritems():
            yield (key, value)

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        filter_ = self.filters[key]
        return filter_

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        assert isinstance(agg_body, dict)
        assert 'filters' in agg_body
        return {'filters': agg_body['filters']}


class MatchAll(Filters):

    def __init__(self, agg_name, meta=None, aggs=None):
        super(MatchAll, self).__init__(
            agg_name=agg_name,
            filters={'All': {'match_all': {}}},
            meta=meta,
            aggs=aggs
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return agg_body


class Histogram(ListBucketAgg):

    AGG_TYPE = 'histogram'
    WHITELISTED_MAPPING_TYPES = ['date']

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

    AGG_TYPE = 'date_histogram'

    ALLOWED_INTERVAL_UNITS = ('y', 'q', 'M', 'w', 'd')  # not under a day to avoid breaking ES ('h', 'm', 's')

    def __init__(self,
                 agg_name, field, interval, meta=None, date_format="yyyy-MM-dd", use_key_as_string=True, aggs=None):
        self._validate_interval(interval)
        if use_key_as_string:
            self.KEY_PATH = 'key_as_string'
        super(DateHistogram, self).__init__(
            agg_name=agg_name,
            field=field,
            interval=interval,
            hist_format=date_format,
            meta=meta,
            aggs=aggs
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


class UniqueBucketAgg(BucketAggNode):
    """Aggregations providing a single bucket."""
    SINGLE_BUCKET = True

    def extract_buckets(self, response_value):
        yield (None, response_value)

    def get_filter(self, key):
        return None

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


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


class Nested(UniqueBucketAgg):

    AGG_TYPE = 'nested'
    WHITELISTED_MAPPING_TYPES = ['nested']

    def __init__(self, agg_name, path, meta=None, aggs=None):
        super(Nested, self).__init__(
            agg_name=agg_name,
            agg_body={"path": path},
            meta=meta,
            aggs=aggs
        )
        self.path = path

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


class FieldMetricAgg(MetricAgg):
    """Metric aggregation based on single field."""
    VALUE_ATTRS = NotImplementedError()

    def __init__(self, agg_name, field, meta=None, **agg_body_kwargs):
        agg_body = dict(agg_body_kwargs)
        agg_body['field'] = field
        super(FieldMetricAgg, self).__init__(
            agg_name=agg_name,
            agg_body=agg_body,
            meta=meta
        )

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        return agg_body


class Avg(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = ['integer', 'float']
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'avg'


class Max(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = ['integer', 'float']
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'max'


class Min(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = ['integer', 'float']
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'min'


class ValueCount(FieldMetricAgg):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'value_count'


class Cardinality(FieldMetricAgg):
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


class Stats(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = ['integer', 'float']
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
