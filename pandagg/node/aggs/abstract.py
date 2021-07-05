#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from pandagg.node._node import Node


def A(name, type_or_agg=None, **body):
    """
    Accept multiple syntaxes, return a AggNode instance.

    :param type_or_agg:
    :param body:
    :return: AggNode
    """
    if isinstance(type_or_agg, str):
        # _translate_agg("per_user", "terms", field="user")
        return AggClause._get_dsl_class(type_or_agg)(**body)
    if isinstance(type_or_agg, AggClause):
        # _translate_agg("per_user", Terms(field='user'))
        if body:
            raise ValueError(
                'Body cannot be added using "AggNode" declaration, got %s.' % body
            )
        return type_or_agg
    if isinstance(type_or_agg, dict):
        # _translate_agg("per_user", {"terms": {"field": "user"}})
        if body:
            raise ValueError(
                'Body cannot be added using "dict" agg declaration, got %s.' % body
            )
        type_or_agg = type_or_agg.copy()
        children_aggs = (
            type_or_agg.pop("aggs", None) or type_or_agg.pop("aggregations", None) or {}
        )
        if len(type_or_agg) != 1:
            raise ValueError(
                "Invalid aggregation declaration (two many keys): got <%s>"
                % type_or_agg
            )
        type_, body_ = type_or_agg.popitem()
        body_ = body_.copy()
        if children_aggs:
            body_["aggs"] = children_aggs
        return AggClause._get_dsl_class(type_)(**body_)
    if type_or_agg is None:
        # if type_or_agg is not provided, by default execute a terms aggregation
        # _translate_agg("per_user")
        return AggClause._get_dsl_class("terms")(field=name, **body)
    raise ValueError('"type_or_agg" must be among "dict", "AggNode", "str"')


class AggClause(Node):
    """
    Wrapper around elasticsearch aggregation concept.
    https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations.html

    Each aggregation can be seen both a Node that can be encapsulated in a parent agg.

    Define a method to build aggregation request.
    """

    _type_name = "agg"
    KEY = None
    VALUE_ATTRS = None
    WHITELISTED_MAPPING_TYPES = None
    BLACKLISTED_MAPPING_TYPES = None

    def __init__(self, meta=None, **body):
        identifier = body.pop("identifier", None)
        self.body = body
        self.meta = meta
        self._children = {}
        super(AggClause, self).__init__(identifier=identifier)

    def line_repr(self, depth, **kwargs):
        # root node
        if self.KEY is None:
            return "_", ""
        repr_args = [str(self.KEY)]
        if self.body:
            repr_args.append(self._params_repr(self.body))
        unnamed = "<%s>" % ", ".join(repr_args)
        return "", unnamed

    @staticmethod
    def _params_repr(params):
        params = params or {}
        return ", ".join(
            "%s=%s" % (str(k), str(json.dumps(params[k], sort_keys=True)))
            for k in sorted(params.keys())
        )

    @classmethod
    def valid_on_field_type(cls, field_type):
        if cls.WHITELISTED_MAPPING_TYPES is not None:
            return field_type in cls.WHITELISTED_MAPPING_TYPES
        if cls.BLACKLISTED_MAPPING_TYPES is not None:
            return field_type not in cls.BLACKLISTED_MAPPING_TYPES
        # by default laxist
        # TODO - constraint to only allowed types
        return True

    def to_dict(self):
        """
        ElasticSearch aggregation queries follow this formatting::

            {
                "<aggregation_name>" : {
                    "<aggregation_type>" : {
                        <aggregation_body>
                    }
                    [,"meta" : {  [<meta_data_body>] } ]?
                }
            }

        to_dict() returns the following part (without aggregation name)::

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
        return aggs

    def get_filter(self, key):
        """
        Return filter query to list documents having this aggregation key.

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
        return "<{class_}, type={type}, body={body}>".format(
            class_=str(self.__class__.__name__),
            type=str(self.KEY),
            body=json.dumps(self.body),
        )

    def __eq__(self, other):
        if isinstance(other, AggClause):
            return other.to_dict() == self.to_dict()
        # make sure we still equal to a dict with the same data
        return other == self.to_dict()


class Root(AggClause):
    """
    Not a real aggregation. Just the initial empty dict (used as lighttree.Tree root).
    """

    KEY = "_root"

    def line_repr(self, depth, **kwargs):
        return "_", ""

    def extract_buckets(self, response_value):
        yield None, response_value

    @classmethod
    def extract_bucket_value(cls, response, value_as_dict=False):
        return None


class MetricAgg(AggClause):
    """
    Metric aggregation are aggregations providing a single bucket, with value attributes to be extracted.
    """

    VALUE_ATTRS = None

    def extract_buckets(self, response_value):
        yield None, response_value

    def get_filter(self, key):
        return None


class BucketAggClause(AggClause):
    """
    Bucket aggregation have special abilities: they can encapsulate other aggregations as children.
    Each time, the extracted value is a 'doc_count'.

    Provide methods:
    - to build aggregation request (with children aggregations)
    - to to extract buckets from raw response
    - to build query to filter documents belonging to that bucket

    Note: the aggs attribute's only purpose is for children initiation with the following syntax:
    >>> from pandagg.aggs import Terms, Avg
    >>> agg = Terms(
    >>>     field='some_path',
    >>>     aggs={
    >>>         'avg_agg': Avg(field='some_other_path')
    >>>     }
    >>> )
    """

    VALUE_ATTRS = None

    def __init__(self, meta=None, **body):
        identifier = body.pop("identifier", None)
        self.body = body
        self.meta = meta
        self._children = body.pop("aggs", None) or body.pop("aggregations", None) or {}
        super(AggClause, self).__init__(identifier=identifier)

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        raise NotImplementedError()


class UniqueBucketAgg(BucketAggClause):
    """Aggregations providing a single bucket."""

    VALUE_ATTRS = None

    def extract_buckets(self, response_value):
        yield None, response_value

    def get_filter(self, key):
        raise NotImplementedError()


class MultipleBucketAgg(BucketAggClause):

    VALUE_ATTRS = None
    IMPLICIT_KEYED = False

    def __init__(self, keyed=None, key_path="key", meta=None, **body):
        """
        Aggregation that return either a list or a map of buckets.

        If keyed, ES buckets are expected as dict, else as list (in this case key_path is used to extract key from each
        list item).
        :param keyed:
        :param meta:
        :param aggs:
        :param body:
        """
        # keyed has another meaning in lighttree Node
        self.keyed_ = keyed or self.IMPLICIT_KEYED
        self.key_path = key_path
        if keyed and not self.IMPLICIT_KEYED:
            body["keyed"] = keyed
        super(MultipleBucketAgg, self).__init__(meta=meta, **body)

    def extract_buckets(self, response_value):
        buckets = response_value["buckets"]
        if self.keyed_:
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
    """
    Metric aggregation based on single field.
    """

    VALUE_ATTRS = None

    def __init__(self, field=None, script=None, meta=None, **body):
        self.field = field
        self.script = script
        if field is not None:
            body["field"] = field
        if script is not None:
            body["script"] = script
        super(FieldOrScriptMetricAgg, self).__init__(meta=meta, **body)


class Pipeline(UniqueBucketAgg):

    VALUE_ATTRS = None

    def __init__(self, buckets_path, gap_policy=None, meta=None, **body):
        self.buckets_path = buckets_path
        self.gap_policy = gap_policy
        body_kwargs = dict(body)
        if gap_policy is not None:
            assert gap_policy in ("skip", "insert_zeros")
            body_kwargs["gap_policy"] = gap_policy

        super(Pipeline, self).__init__(meta=meta, buckets_path=buckets_path, **body)

    def get_filter(self, key):
        return None


class ScriptPipeline(Pipeline):
    KEY = None
    VALUE_ATTRS = "value"

    def __init__(self, script, buckets_path, gap_policy=None, meta=None, **body):
        super(ScriptPipeline, self).__init__(
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            meta=meta,
            script=script,
            **body
        )
