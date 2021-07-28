import json

from pandagg.node._node import Node
from typing import Optional, List, Union, Dict, Any, Tuple, Iterator

from pandagg.types import Meta, BucketKey, Bucket

AggClauseDict = Dict[str, Any]


class AggClause(Node):
    """
    Wrapper around elasticsearch aggregation concept.
    https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations.html

    Each aggregation can be seen both a Node that can be encapsulated in a parent agg.

    Define a method to build aggregation request.
    """

    _type_name = "agg"
    KEY: str

    VALUE_ATTRS: List[str]
    WHITELISTED_MAPPING_TYPES: List[str]
    BLACKLISTED_MAPPING_TYPES: List[str]

    def __init__(self, meta: Optional[Dict[str, Any]] = None, **body: Any) -> None:
        identifier: Optional[str] = body.pop("identifier", None)
        self.body: Dict[str, Any] = body
        self.meta: Optional[Dict[str, Any]] = meta
        self._children: Dict[str, Any] = {}
        super(AggClause, self).__init__(identifier=identifier)

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        # root node
        if not self.KEY:
            return "_", ""
        repr_args = [str(self.KEY)]
        if self.body:
            repr_args.append(self._params_repr(self.body))
        unnamed = "<%s>" % ", ".join(repr_args)
        return "", unnamed

    @staticmethod
    def _params_repr(params: Dict[str, Any]) -> str:
        params = params or {}
        return ", ".join(
            "%s=%s" % (str(k), str(json.dumps(params[k], sort_keys=True)))
            for k in sorted(params.keys())
        )

    @classmethod
    def valid_on_field_type(cls, field_type: str) -> bool:
        if hasattr(cls, "WHITELISTED_MAPPING_TYPES"):
            return field_type in cls.WHITELISTED_MAPPING_TYPES
        if hasattr(cls, "BLACKLISTED_MAPPING_TYPES"):
            return field_type not in cls.BLACKLISTED_MAPPING_TYPES
        # by default laxist
        # TODO - constraint to only allowed types
        return True

    def to_dict(self) -> Dict[str, Any]:
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
        if self.KEY is None:
            raise ValueError("For typing only")
        aggs = {self.KEY: self.body}
        if self.meta:
            aggs["meta"] = self.meta
        return aggs

    def get_filter(self, key: str):
        """
        Return filter query to list documents having this aggregation key.

        :param key: string
        :return: elasticsearch filter query
        """
        raise NotImplementedError()

    def extract_buckets(self, response_value) -> Iterator[Tuple[BucketKey, Bucket]]:
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


TypeOrAgg = Union[str, AggClauseDict, AggClause]


def A(name: str, type_or_agg: Optional[TypeOrAgg] = None, **body: Any) -> AggClause:
    """
    Accept multiple syntaxes, return a AggNode instance.

    :param name: aggregation clause name
    :param type_or_agg:
    :param body:
    :return: AggNode
    """
    if isinstance(type_or_agg, str):
        # _translate_agg("per_user", "terms", field="user")
        return AggClause.get_dsl_class(type_or_agg)(**body)
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
        return AggClause.get_dsl_class(type_)(**body_)
    if type_or_agg is None:
        # if type_or_agg is not provided, by default execute a terms aggregation
        # _translate_agg("per_user")
        return AggClause.get_dsl_class("terms")(field=name, **body)
    raise ValueError('"type_or_agg" must be among "dict", "AggNode", "str"')


class Root(AggClause):
    """
    Not a real aggregation. Just the initial empty dict (used as lighttree.Tree root).
    """

    KEY: str = "_root"

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
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

    def __init__(self, meta=None, **body):
        identifier: Optional[str] = body.pop("identifier", None)
        self.body: Dict[str, Any] = body
        self.meta: Optional[Dict, str] = meta
        self._children = body.pop("aggs", None) or body.pop("aggregations", None) or {}
        super(AggClause, self).__init__(identifier=identifier)

    def extract_buckets(self, response_value):
        raise NotImplementedError()

    def get_filter(self, key):
        """Provide filter to get documents belonging to document of given key."""
        raise NotImplementedError()


class UniqueBucketAgg(BucketAggClause):
    """Aggregations providing a single bucket."""

    def extract_buckets(self, response_value):
        yield None, response_value

    def get_filter(self, key):
        raise NotImplementedError()


class MultipleBucketAgg(BucketAggClause):

    IMPLICIT_KEYED: bool = False

    def __init__(
        self, keyed: bool = False, key_path: str = "key", meta: Meta = None, **body: Any
    ) -> None:
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
        self.keyed_: bool = keyed or self.IMPLICIT_KEYED
        self.key_path: str = key_path
        if keyed and not self.IMPLICIT_KEYED:
            body["keyed"] = keyed
        super(MultipleBucketAgg, self).__init__(meta=meta, **body)

    def extract_buckets(self, response_value):
        buckets = response_value["buckets"]
        if self.keyed_:
            for key in sorted(buckets.keys()):
                yield key, buckets[key]
        else:
            for bucket in buckets:
                yield self._extract_bucket_key(bucket), bucket

    def _extract_bucket_key(self, bucket):
        return bucket[self.key_path]

    def get_filter(self, key):
        raise NotImplementedError()


class FieldOrScriptMetricAgg(MetricAgg):
    """
    Metric aggregation based on single field.
    """

    def __init__(self, field=None, script=None, meta=None, **body):
        self.field = field
        self.script = script
        if field is not None:
            body["field"] = field
        if script is not None:
            body["script"] = script
        super(FieldOrScriptMetricAgg, self).__init__(meta=meta, **body)


class Pipeline(UniqueBucketAgg):
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
    VALUE_ATTRS: List[str] = ["value"]

    def __init__(self, script, buckets_path, gap_policy=None, meta=None, **body):
        super(ScriptPipeline, self).__init__(
            buckets_path=buckets_path,
            gap_policy=gap_policy,
            meta=meta,
            script=script,
            **body
        )
