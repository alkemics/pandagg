from .abstract import BucketAggClause
from typing import Optional, Any, Dict, List, Iterator, Tuple
from pandagg.types import (
    AfterKey,
    CompositeSource,
    BucketDict,
    AggClauseResponseDict,
    AggName,
    CompositeBucketKey,
)


class Composite(BucketAggClause):

    KEY = "composite"
    VALUE_ATTRS = ["doc_count"]

    def __init__(
        self,
        sources: List[Dict[AggName, CompositeSource]],
        size: Optional[int] = None,
        after: Optional[AfterKey] = None,
        **body: Any
    ):
        """https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html"""  # noqa: E501
        super(Composite, self).__init__(sources=sources, size=size, after=after, **body)

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
