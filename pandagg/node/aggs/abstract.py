#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import json

from pandagg.node._node import Node


class AggNode(Node):
    """Wrapper around elasticsearch aggregation concept.
    https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations.html

    Each aggregation can be seen both a Node that can be encapsulated in a parent agg.

    Define a method to build aggregation request.
    """

    _type_name = "agg"
    KEY = None
    VALUE_ATTRS = None
    WHITELISTED_MAPPING_TYPES = None
    BLACKLISTED_MAPPING_TYPES = None

    def __init__(self, name, meta=None, **body):
        self.name = name
        self.body = body
        self.meta = meta
        super(AggNode, self).__init__(identifier=self.name)

    def line_repr(self, depth, **kwargs):
        return "[%s] %s" % (text(self.name), text(self.KEY))

    @classmethod
    def valid_on_field_type(cls, field_type):
        if cls.WHITELISTED_MAPPING_TYPES is not None:
            return field_type in cls.WHITELISTED_MAPPING_TYPES
        if cls.BLACKLISTED_MAPPING_TYPES is not None:
            return field_type not in cls.BLACKLISTED_MAPPING_TYPES
        return False

    def to_dict(self, with_name=False):
        """ElasticSearch aggregation queries follow this formatting::

            {
                "<aggregation_name>" : {
                    "<aggregation_type>" : {
                        <aggregation_body>
                    }
                    [,"meta" : {  [<meta_data_body>] } ]?
                }
            }

        Query dict returns the following part (without aggregation name)::

            {
                "<aggregation_type>" : {
                    <aggregation_body>
                }
                [,"meta" : {  [<meta_data_body>] } ]?
            }
        """
        aggs = {self.KEY: self.body}
        if self.meta:
            aggs["meta"] = self.meta
        if with_name:
            return {self.name: aggs}
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

    def __str__(self):
        return "<{class_}, name={name}, type={type}, body={body}>".format(
            class_=text(self.__class__.__name__),
            type=text(self.KEY),
            name=text(self.name),
            body=json.dumps(self.body),
        )

    def __eq__(self, other):
        if isinstance(other, AggNode):
            return other.to_dict() == self.to_dict() and other.name == self.name
        # make sure we still equal to a dict with the same data
        return other == {self.name: self.to_dict()}


class MetricAgg(AggNode):
    """Metric aggregation are aggregations providing a single bucket, with value attributes to be extracted."""

    VALUE_ATTRS = None

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

    Note: the aggs attribute's only purpose is for children initiation with the following syntax:
    >>> from pandagg.aggs import Terms, Avg
    >>> agg = Terms(
    >>>     name='term_agg',
    >>>     field='some_path',
    >>>     aggs=[
    >>>         Avg(agg_name='avg_agg', field='some_other_path')
    >>>     ]
    >>> )
    """

    VALUE_ATTRS = None

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        raise NotImplementedError()


class UniqueBucketAgg(BucketAggNode):
    """Aggregations providing a single bucket."""

    VALUE_ATTRS = None

    def extract_buckets(self, response_value):
        yield None, response_value

    def get_filter(self, key):
        raise NotImplementedError()


class ShadowRoot(UniqueBucketAgg):
    """Not a real aggregation."""

    KEY = "shadow_root"

    def __init__(self):
        super(ShadowRoot, self).__init__("_")

    @classmethod
    def extract_bucket_value(cls, response, value_as_dict=False):
        return None

    def line_repr(self, depth, **kwargs):
        return "[%s]" % text(self.name)

    def get_filter(self, key):
        return None


class MultipleBucketAgg(BucketAggNode):

    VALUE_ATTRS = None
    IMPLICIT_KEYED = False

    def __init__(self, name, keyed=None, key_path="key", meta=None, **body):
        """Aggregation that return either a list or a map of buckets.

        If keyed, ES buckets are expected as dict, else as list (in this case key_path is used to extract key from each
        list item).
        :param name:
        :param keyed:
        :param meta:
        :param aggs:
        :param body:
        """
        self.keyed = keyed or self.IMPLICIT_KEYED
        self.key_path = key_path
        if keyed and not self.IMPLICIT_KEYED:
            body["keyed"] = keyed
        super(MultipleBucketAgg, self).__init__(name=name, meta=meta, **body)

    def extract_buckets(self, response_value):
        buckets = response_value["buckets"]
        if self.keyed:
            for key in sorted(buckets.keys()):
                yield (key, buckets[key])
        else:
            for bucket in buckets:
                yield (self._extract_bucket_key(bucket), bucket)

    def _extract_bucket_key(self, bucket):
        return bucket[self.key_path]

    def get_filter(self, key):
        raise NotImplementedError()


class FieldOrScriptMetricAgg(MetricAgg):
    """Metric aggregation based on single field."""

    VALUE_ATTRS = None

    def __init__(self, name, meta=None, **body):
        self.field = body.get("field")
        self.script = body.get("script")
        super(FieldOrScriptMetricAgg, self).__init__(name=name, meta=meta, **body)


class Pipeline(UniqueBucketAgg):

    VALUE_ATTRS = None

    def __init__(self, name, buckets_path, gap_policy=None, meta=None, **body):
        self.buckets_path = buckets_path
        self.gap_policy = gap_policy
        body_kwargs = dict(body)
        if gap_policy is not None:
            assert gap_policy in ("skip", "insert_zeros")
            body_kwargs["gap_policy"] = gap_policy

        super(Pipeline, self).__init__(
            name=name, meta=meta, buckets_path=buckets_path, **body
        )

    def get_filter(self, key):
        return None


class ScriptPipeline(Pipeline):
    KEY = None
    VALUE_ATTRS = "value"

    def __init__(self, name, script, buckets_path, gap_policy=None, meta=None, **body):
        super(ScriptPipeline, self).__init__(
            name=name,
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            meta=meta,
            script=script,
            **body
        )
