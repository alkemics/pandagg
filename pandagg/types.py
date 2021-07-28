from typing import Optional, Dict, Any


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
