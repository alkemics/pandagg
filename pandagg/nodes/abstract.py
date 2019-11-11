#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import json

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

    def __init__(self, name, meta=None, **body):
        self.name = name
        super(AggNode, self).__init__(identifier=self.name)
        self.body = body
        self.meta = meta

    @classmethod
    def valid_on_field_type(cls, field_type):
        if cls.WHITELISTED_MAPPING_TYPES is not None:
            return field_type in cls.WHITELISTED_MAPPING_TYPES
        if cls.BLACKLISTED_MAPPING_TYPES is not None:
            return field_type not in cls.BLACKLISTED_MAPPING_TYPES
        return False

    def query_dict(self):
        """ElasticSearch aggregation queries follow this formatting:
        {
            "<aggregation_name>" : {
                "<aggregation_type>" : {
                    <aggregation_body>
                }
                [,"meta" : {  [<meta_data_body>] } ]?
            }
        }

        Query dict returns the following part (without aggregation name):
        {
            "<aggregation_type>" : {
                <aggregation_body>
            }
            [,"meta" : {  [<meta_data_body>] } ]?
        }
        """
        aggs = {self.AGG_TYPE: self.body}
        if self.meta:
            aggs["meta"] = self.meta
        return aggs

    def get_filter(self, key):
        """Return filter query to list documents having this aggregation key.
        :param key: string
        :return: elasticsearch filter query
        """
        raise NotImplementedError()

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    @classmethod
    def extract_bucket_value(cls, response, value_as_dict=False):
        attrs = cls.VALUE_ATTRS
        if value_as_dict:
            return {attr_: response.get(attr_) for attr_ in attrs}
        return response.get(attrs[0])

    @classmethod
    def deserialize(cls, name, **params):
        return cls(name=name, **params)

    def __str__(self):
        return "<{class_}, name={name}, type={type}, body={body}>".format(
            class_=text(self.__class__.__name__),
            type=text(self.AGG_TYPE),
            name=text(self.name), body=json.dumps(self.body)
        )

    def __eq__(self, other):
        if isinstance(other, AggNode):
            return other.query_dict() == self.query_dict() and other.name == self.name
        # make sure we still equal to a dict with the same data
        return other == {self.name: self.query_dict()}


class MetricAgg(AggNode):
    """Metric aggregation are aggregations providing a single bucket, with value attributes to be extracted."""
    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = True

    def extract_buckets(self, response_value):
        yield (None, response_value)

    def get_filter(self, key):
        return None


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
    >>>     name='term_agg',
    >>>     field='some_path',
    >>>     aggs=[
    >>>         Avg(agg_name='avg_agg', field='some_other_path')
    >>>     ]
    >>> )
    Yet, the children attribute will then be reset to None to avoid confusion since the real hierarchy is stored in the
    bpointer/fpointer attributes inherited from treelib.Tree class.
    """
    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = NotImplementedError()

    def __init__(self, name, meta=None, aggs=None, **body):
        super(BucketAggNode, self).__init__(
            name=name,
            meta=meta,
            **body
        )
        aggs = aggs or []
        for child in aggs:
            assert isinstance(child, AggNode)
        self.aggs = aggs

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        raise NotImplementedError()


class UniqueBucketAgg(BucketAggNode):
    """Aggregations providing a single bucket."""
    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = True

    def extract_buckets(self, response_value):
        yield (None, response_value)

    def get_filter(self, key):
        raise NotImplementedError()


class ListBucketAgg(BucketAggNode):

    # Aggregation that return a list of buckets as a list (terms, histogram, date-histogram).
    VALUE_ATTRS = NotImplementedError()
    KEY_PATH = 'key'
    SINGLE_BUCKET = False

    def extract_buckets(self, response_value):
        for bucket in response_value['buckets']:
            yield (bucket[self.KEY_PATH], bucket)

    def get_filter(self, key):
        raise NotImplementedError()


class FieldMetricAgg(MetricAgg):
    """Metric aggregation based on single field."""
    VALUE_ATTRS = NotImplementedError()

    def __init__(self, name, field, meta=None, **body):
        self.field = field
        super(FieldMetricAgg, self).__init__(
            name=name,
            meta=meta,
            field=field,
            **body
        )


class Pipeline(UniqueBucketAgg):

    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = NotImplementedError()

    def __init__(self, name, buckets_path, gap_policy=None, meta=None, aggs=None, **body):
        self.buckets_path = buckets_path
        self.gap_policy = gap_policy
        body_kwargs = dict(body)
        if gap_policy is not None:
            assert gap_policy in ('skip', 'insert_zeros')
            body_kwargs['gap_policy'] = gap_policy

        super(Pipeline, self).__init__(
            name=name,
            meta=meta,
            aggs=aggs,
            buckets_path=buckets_path,
            **body
        )

    def get_filter(self, key):
        return None


class ScriptPipeline(Pipeline):
    AGG_TYPE = NotImplementedError()
    VALUE_ATTRS = 'value'

    def __init__(self, name, script, buckets_path, gap_policy=None, meta=None, aggs=None, **body):
        super(ScriptPipeline, self).__init__(
            self,
            name=name,
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            meta=meta,
            aggs=aggs,
            script=script,
            **body
        )
