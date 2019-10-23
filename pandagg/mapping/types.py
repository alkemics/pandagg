#!/usr/bin/env python
# -*- coding: utf-8 -*-

NUMERIC_TYPES = ['long', 'integer', 'short', 'byte', 'double', 'float', 'ip', 'token_count']

MAPPING_TYPES = [
    'binary',
    'boolean',
    'date',
    'geo_point',
    'geo_shape',
    'nested',
    'object',
    'string'
] + NUMERIC_TYPES
