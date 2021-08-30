from typing import List

NUMERIC_TYPES: List[str] = [
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "ip",
    "token_count",
    "date",
    "boolean",
]

MAPPING_TYPES: List[str] = [
    "binary",
    "geo_point",
    "geo_shape",
    "nested",
    "object",
    "text",
    "keyword",
    "flattened",
] + NUMERIC_TYPES
