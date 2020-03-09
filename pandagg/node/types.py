#!/usr/bin/env python
# -*- coding: utf-8 -*-

NUMERIC_TYPES = [
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

MAPPING_TYPES = [
    "binary",
    "geo_point",
    "geo_shape",
    "nested",
    "object",
    "text",
    "keyword",
    "flattened",
] + NUMERIC_TYPES
