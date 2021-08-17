from typing import Optional, Dict, Any, TypedDict, Literal, List

Meta = Optional[Dict[str, Any]]

ClauseName = str
ClauseBody = Dict[str, Any]
AggName = ClauseName
QueryName = ClauseName
FieldName = ClauseName

# Aggs
BucketKey = Any
Bucket = Any

# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_value_sources
CompositeSource = Dict[str, Any]
# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_pagination
AfterKey = Dict[str, Any]

DocSource = Dict[str, Any]
SettingsDict = Dict[str, Any]
MappingsDict = Dict[str, Any]
SearchDict = Dict[str, Any]

AggregationsDict = Dict[str, Any]


class HitDict(TypedDict, total=False):
    _index: str
    _id: str
    _source: DocSource
    _score: float


Relation = Literal["eq", "gte"]


class TotalDict(TypedDict, total=False):
    value: int
    relation: Relation


class HitsDict(TypedDict, total=False):
    total: TotalDict
    hits: List[HitDict]
    max_score: Optional[float]


class ShardsDict(TypedDict, total=False):
    total: int
    successful: int
    skipped: int
    failed: int


class ProfileShardDict(TypedDict, total=False):
    id: str
    searches: List
    aggregations: List


class ProfileDict(TypedDict, total=False):
    shards: List[ProfileShardDict]


class QueryResponseDict(TypedDict, total=False):
    _shards: ShardsDict
    timed_out: bool
    terminated_early: bool
    took: int
    hits: HitsDict
    aggregations: AggregationsDict
    profile: ProfileDict
