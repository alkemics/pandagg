#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pandagg.mapping.types import NUMERIC_TYPES
from pandagg.nodes.abstract import FieldMetricAgg


class Avg(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'avg'


class Max(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'max'


class Min(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'min'


class Cardinality(FieldMetricAgg):
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'cardinality'

    def __init__(self, agg_name, field, meta=None, precision_threshold=1000):
        # precision_threshold: the higher the more accurate but longer to proceed (default ES: 1)
        body_kwargs = {}
        if precision_threshold is not None:
            body_kwargs['precision_threshold'] = precision_threshold

        super(Cardinality, self).__init__(
            agg_name=agg_name,
            field=field,
            meta=meta,
            **body_kwargs
        )


class Stats(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ['count', 'min', 'max', 'avg', 'sum']
    AGG_TYPE = 'stats'


class ExtendedStats(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = [
        'count', 'min', 'max', 'avg', 'sum', 'sum_of_squares', 'variance', 'std_deviation', 'std_deviation_bounds']
    AGG_TYPE = 'extended_stats'


class GeoBound(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = ['geo_point']
    VALUE_ATTRS = ['bounds']
    AGG_TYPE = 'geo_bounds'


class GeoCentroid(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = ['geo_point']
    VALUE_ATTRS = ['location']
    AGG_TYPE = 'geo_centroid'


class Percentiles(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ['values']
    AGG_TYPE = 'percentiles'


class PercentileRanks(FieldMetricAgg):
    WHITELISTED_MAPPING_TYPES = NUMERIC_TYPES
    VALUE_ATTRS = ['values']
    AGG_TYPE = 'percentile_ranks'


class TopHits(FieldMetricAgg):
    BLACKLISTED_MAPPING_TYPES = []
    VALUE_ATTRS = ['hits']
    AGG_TYPE = 'top_hits'


class ValueCount(FieldMetricAgg):
    BLACKLISTED_MAPPING_TYPES = []
    VALUE_ATTRS = ['value']
    AGG_TYPE = 'value_count'


METRIC_AGGS = {
    agg.AGG_TYPE: agg
    for agg in [
        Avg,
        Max,
        Min,
        Cardinality,
        Stats,
        ExtendedStats,
        Percentiles,
        PercentileRanks,
        GeoBound,
        GeoCentroid,
        TopHits,
        ValueCount
    ]
}
