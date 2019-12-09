#!/usr/bin/env python
# -*- coding: utf-8 -*-

NUMERIC_TYPES = [
    'long',
    'integer',
    'short',
    'byte',
    'double',
    'float',
    'half_float',
    'scaled_float',
    'ip',
    'token_count'
]

MAPPING_TYPES = [
    'binary',
    'boolean',
    'date',
    'geo_point',
    'geo_shape',
    'nested',
    'object',
    'text',
    'keyword'
] + NUMERIC_TYPES
