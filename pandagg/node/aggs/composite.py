from .abstract import BucketAggClause, AggClause
from typing import Optional, Any, Dict, List, Iterator, Tuple
from pandagg.types import (
    Meta,
    AfterKey,
    CompositeSource,
    BucketKey,
    BucketDict,
    AggClauseResponseDict,
    AggName,
    QueryClauseDict,
    CompositeBucketKey,
    AggType,
    ClauseBody,
)


class Composite(BucketAggClause):

    KEY = "composite"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        sources: List[Dict[AggName, CompositeSource]],
        size: Optional[int] = None,
        after: Optional[AfterKey] = None,
        meta: Meta = None,
        **body: Any
    ):
        """https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html"""  # noqa: E501
        aggs = body.pop("aggs", None) or body.pop("aggregations", None)
        _children: Dict[AggName, Any] = aggs or {}  # type: ignore
        self._children: Dict[AggName, Any] = _children
        if size is not None:
            body["size"] = size
        if after is not None:
            body["after"] = after
        super(Composite, self).__init__(meta=meta, sources=sources, **body)

    @property
    def after(self) -> Optional[AfterKey]:
        return self.body["after"]

    @property
    def size(self) -> Optional[int]:
        return self.body["size"]

    @property
    def sources(self) -> List[Dict[AggName, CompositeSource]]:
        return self.body["sources"]

    @property
    def source_names(self) -> List[AggName]:
        return [n for source in self.sources for n in source.keys()]

    def extract_buckets(
        self, response_value: AggClauseResponseDict
    ) -> Iterator[Tuple[CompositeBucketKey, BucketDict]]:
        buckets: List[BucketDict] = response_value["buckets"]  # type: ignore
        for bucket in buckets:
            bucket_composite_key: CompositeBucketKey = bucket["key"]  # type: ignore
            yield bucket_composite_key, bucket

    def get_filter(self, key: BucketKey) -> Optional[QueryClauseDict]:
        """In composite aggregation, key is a map, source name -> value"""
        if not key:
            return None
        key_: CompositeBucketKey = key  # type: ignore
        conditions: List[QueryClauseDict] = []
        source: Dict[AggName, CompositeSource]

        for source in self.sources:
            name: AggName
            source_clause: CompositeSource
            name, source_clause = source.popitem()
            if name not in key_:
                continue
            agg_type: AggType
            agg_body: ClauseBody
            agg_type, agg_body = source.popitem()
            agg_instance: AggClause = self.get_dsl_class(agg_type)(**agg_body)
            condition: Optional[QueryClauseDict] = agg_instance.get_filter(
                key=key_[name]
            )
            if condition is not None:
                conditions.append(condition)
        if not conditions:
            return None
        return {"bool": {"filter": conditions}}
