#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import json

from treelib import Node


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
        aggs = {self.AGG_TYPE: self.agg_body}
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

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        """Method used to reconstruct aggregation classes from json agg_body declaration. All required kwargs except
        `agg_name` and `meta` must be reconstructed.
        """
        raise NotImplementedError()

    def __str__(self):
        return "<{class_}, name={name}, type={type}, body={body}>".format(
            class_=text(self.__class__.__name__),
            type=text(self.AGG_TYPE),
            name=text(self.agg_name), body=json.dumps(self.agg_body)
        )


class MetricAgg(AggNode):
    """Metric aggregation are aggregations providing a single bucket, with value attributes to be extracted."""
    VALUE_ATTRS = NotImplementedError()
    SINGLE_BUCKET = True

    def extract_buckets(self, response_value):
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

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        raise NotImplementedError()

    @staticmethod
    def agg_body_to_init_kwargs(agg_body):
        raise NotImplementedError()


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


class FieldMetricAgg(MetricAgg):
    """Metric aggregation based on single field."""
    VALUE_ATTRS = NotImplementedError()

    def __init__(self, agg_name, field, meta=None, **agg_body_kwargs):
        self.field = field
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
