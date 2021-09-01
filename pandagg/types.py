from typing_extensions import TypedDict, Literal
from typing import Optional, Dict, Any, List, Union

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


BucketKeyAtom = Union[None, str, float]
CompositeBucketKey = Dict[AggName, BucketKeyAtom]

BucketKey = Union[BucketKeyAtom, CompositeBucketKey]
BucketDict = Dict[str, Any]

RangeDict = TypedDict("RangeDict", {"from": float, "to": float}, total=False)
DistanceType = Literal["arc", "plane"]
ValidationMethod = Literal["STRICT", "COERCE", "IGNORE_MALFORMED"]

# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_value_sources
CompositeSource = AggClauseDict
# https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_pagination
AfterKey = Dict[str, Any]

DocSource = Dict[str, Any]
SettingsDict = Dict[str, Any]

IndexName = str

# Mappings
FieldName = ClauseName
FieldType = ClauseType
FieldClauseDict = Dict[str, Any]
FieldPropertiesDict = Dict[FieldName, FieldClauseDict]


class MappingsDict(TypedDict, total=False):
    properties: FieldPropertiesDict
    dynamic: bool


class SourceIncludeDict(TypedDict, total=False):
    includes: Union[str, List[str]]
    excludes: Union[str, List[str]]


class RunTimeMappingDict(TypedDict, total=False):
    type: str
    script: str


class PointInTimeDict(TypedDict, total=False):
    id: str
    keep_alive: str


class FieldDict(TypedDict, total=False):
    field: str
    format: str


SearchDict = TypedDict(
    "SearchDict",
    {
        "aggs": NamedAggsDict,
        "aggregations": NamedAggsDict,
        "docvalue_fields": List[Union[str, FieldDict]],
        "fields": List[Union[str, FieldDict]],
        "explain": bool,
        "from": int,
        "highlight": Dict[str, Any],
        "indices_boost": List[Dict[IndexName, float]],
        "min_score": float,
        "pit": PointInTimeDict,
        "query": QueryClauseDict,
        "post_filter": QueryClauseDict,
        "runtime_mappings": Dict[FieldName, RunTimeMappingDict],
        "seq_no_primary_term": bool,
        "script_fields": Dict[str, Any],
        "size": int,
        "suggest": Dict[str, Any],
        "_source": Union[bool, str, List[str], SourceIncludeDict],
        "sort": List[Union[str, Dict[str, Any]]],
        "stats": List[str],
        "terminate_after": int,
        "timeout": Any,
        "version": bool,
    },
    total=False,
)

BucketsDict = Dict[BucketKeyAtom, BucketDict]
Buckets = Union[BucketsDict, List[BucketDict]]


class BucketsWrapperDict(TypedDict, total=False):
    buckets: Buckets
    doc_count_error_upper_bound: int
    sum_other_doc_count: int


AggClauseResponseDict = Union[BucketsWrapperDict, BucketDict]
AggregationsResponseDict = Dict[AggName, AggClauseResponseDict]


class HitDict(TypedDict, total=False):
    _index: str
    _id: str
    _source: DocSource
    _score: float
    fields: Dict[str, List[Any]]
    highlight: Dict[str, List[str]]


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


class SuggestedItemDict(TypedDict, total=False):
    text: str
    offset: int
    length: int
    options: List[Dict[str, Any]]


class SearchResponseDict(TypedDict, total=False):
    _scroll_id: str
    _shards: ShardsDict
    timed_out: bool
    terminated_early: bool
    took: int
    hits: HitsDict
    aggregations: AggregationsResponseDict
    profile: ProfileDict
    suggest: Dict[str, List[SuggestedItemDict]]


class RetriesDict(TypedDict, total=False):
    bulk: int
    search: int


class DeleteByQueryResponse(TypedDict, total=False):
    took: int
    timed_out: bool
    total: int
    deleted: int
    batches: int
    version_conflicts: int
    noops: int
    retries: RetriesDict
    throttled_millis: int
    requests_per_second: float
    throttled_until_millis: int
    failures: List[Dict[str, Any]]
