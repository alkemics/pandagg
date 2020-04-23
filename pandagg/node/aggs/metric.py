#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pandagg.node.types import NUMERIC_TYPES
from pandagg.node.aggs.abstract import FieldOrScriptMetricAgg, MetricAgg


class TopHits(MetricAgg):
    VALUE_ATTRS = ["hits"]
    KEY = "top_hits"


class Avg(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["value"]
    KEY = "avg"


class Sum(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["value"]
    KEY = "sum"


class Max(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["value"]
    KEY = "max"


class Min(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["value"]
    KEY = "min"


class Cardinality(FieldOrScriptMetricAgg):
    VALUE_ATTRS = ["value"]
    KEY = "cardinality"


class Stats(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["count", "min", "max", "avg", "sum"]
    KEY = "stats"


class ExtendedStats(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = [
        "count",
        "min",
        "max",
        "avg",
        "sum",
        "sum_of_squares",
        "variance",
        "std_deviation",
        "std_deviation_bounds",
    ]
    KEY = "extended_stats"


class GeoBound(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = ["geo_point"]
    VALUE_ATTRS = ["bounds"]
    KEY = "geo_bounds"


class GeoCentroid(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = ["geo_point"]
    VALUE_ATTRS = ["location"]
    KEY = "geo_centroid"


class Percentiles(FieldOrScriptMetricAgg):
    """Percents body argument can be passed to specify which percentiles to fetch."""

    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["values"]
    KEY = "percentiles"


class PercentileRanks(FieldOrScriptMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ["values"]
    KEY = "percentile_ranks"

    def __init__(self, name, field, values, meta=None, **body):
        super(PercentileRanks, self).__init__(
            name=name, field=field, meta=meta, values=values, **body
        )


class ValueCount(FieldOrScriptMetricAgg):
    BLACKLISTED_MAPPING_TYPES = []
    VALUE_ATTRS = ["value"]
    KEY = "value_count"
