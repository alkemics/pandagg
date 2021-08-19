from typing import Optional, Dict, Any, TypedDict, Literal, List

ClauseName = str
ClauseType = str
ClauseBody = Dict[str, Any]
Meta = Dict[str, Any]


# Script
class Script(TypedDict, total=False):
    lang: str
    id: str
    source: str
    params: Dict[str, Any]


GapPolicy = Literal["skip", "insert_zeros", "keep_values"]

# Query
QueryType = ClauseType
QueryName = ClauseName
QueryClauseDict = Dict[QueryType, ClauseBody]

# Aggs
AggName = ClauseName
AggType = ClauseType
AggClauseDict = Dict[AggType, ClauseBody]
NamedAggsDict = Dict[AggName, AggClauseDict]

AggClauseResponseDict = Dict[str, Any]
AggsResponseDict = Dict[AggName, AggClauseResponseDict]

BucketKey = Any
BucketDict = Dict[str, Any]

RangeDict = TypedDict("RangeDict", {"from": float, "to": float}, total=False)
DistanceType = Literal["arc", "plane"]
ValidationMethod = Literal["STRICT", "COERCE", "IGNORE_MALFORMED"]

# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_value_sources
CompositeSource = Dict[str, Any]
# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_pagination
AfterKey = Dict[str, Any]

DocSource = Dict[str, Any]
SettingsDict = Dict[str, Any]

# Mappings
FieldName = ClauseName
FieldType = ClauseType
FieldClauseDict = Dict[str, Any]
FieldPropertiesDict = Dict[FieldName, FieldClauseDict]


class MappingsDict(TypedDict, total=False):
    properties: FieldPropertiesDict
    dynamic: bool


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
