from .abstract import BucketAggClause
from typing import Optional, Any, Dict, List, Iterator, Tuple
from pandagg.types import (
    Meta,
    AfterKey,
    CompositeSource,
    BucketKey,
    Bucket,
    AggClauseResponse,
    AggName,
)


class Composite(BucketAggClause):

    KEY = "composite"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        sources: List[CompositeSource],
        size: Optional[int] = None,
        after_key: Optional[AfterKey] = None,
        meta: Meta = None,
        **body: Any
    ):
        """https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html
        :param sources:
        :param size:
        :param after_key:
        :param meta:
        :param body:
        """
        self._sources = sources
        self._size: Optional[int] = size
        self._after_key: Optional[Dict[str, Any]] = after_key
        aggs = body.pop("aggs", None) or body.pop("aggregations", None)
        _children: Dict[AggName, Any] = aggs or {}  # type: ignore
        self._children: Dict[str, Any] = _children
        if size is not None:
            body["size"] = size
        if after_key is not None:
            body["after_key"] = after_key
        super(Composite, self).__init__(meta=meta, sources=sources, **body)

    def extract_buckets(
        self, response_value: AggClauseResponse
    ) -> Iterator[Tuple[BucketKey, Bucket]]:
        for bucket in response_value["buckets"]:
            yield bucket["key"], bucket

    def get_filter(self, key):
        """In composite aggregation, key is a map, source name -> value"""
        if not key:
            return
        conditions = []
        for source in self._sources:
            name, agg = source.popitem()
            if name not in key:
                continue
            agg_type, agg_body = source.popitem()
            agg_instance = self.get_dsl_class(agg_type)(**agg_body)
            conditions.append(agg_instance.get_filter(key=key[name]))
        if not conditions:
            return
        return {"bool": {"filter": conditions}}
