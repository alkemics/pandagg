#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Pipeline aggregations:
https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations-pipeline.html
"""

from pandagg.nodes.abstract import Pipeline, ScriptPipeline


class AvgBucket(Pipeline):
    AGG_TYPE = 'avg_bucket'
    VALUE_ATTRS = ['value']


class Derivative(Pipeline):
    AGG_TYPE = 'derivative'
    VALUE_ATTRS = ['value']


class MaxBucket(Pipeline):
    AGG_TYPE = 'max_bucket'
    VALUE_ATTRS = ['value']


class MinBucket(Pipeline):
    AGG_TYPE = 'min_bucket'
    VALUE_ATTRS = ['value']


class SumBucket(Pipeline):
    AGG_TYPE = 'sum_bucket'
    VALUE_ATTRS = ['value']


class StatsBucket(Pipeline):
    AGG_TYPE = 'stats_bucket'
    VALUE_ATTRS = ['count', 'min', 'max', 'avg', 'sum']


class ExtendedStatsBucket(Pipeline):
    AGG_TYPE = 'extended_stats_bucket'
    VALUE_ATTRS = ['count', 'min', 'max', 'avg', 'sum', 'sum_of_squares', 'variance', 'std_deviation',
                   'std_deviation_bounds']


class PercentilesBucket(Pipeline):
    AGG_TYPE = 'percentiles_bucket'
    VALUE_ATTRS = ['values']


class MovingAvg(Pipeline):
    AGG_TYPE = 'moving_avg'
    VALUE_ATTRS = ['value']


class CumulativeSum(Pipeline):
    AGG_TYPE = 'cumulative_sum'
    VALUE_ATTRS = ['value']


class BucketScript(ScriptPipeline):
    AGG_TYPE = 'bucket_script'
    VALUE_ATTRS = ['value']


class BucketSelector(ScriptPipeline):
    AGG_TYPE = 'bucket_selector'
    VALUE_ATTRS = ['value']


class SerialDiff(Pipeline):
    AGG_TYPE = 'serial_diff'
    VALUE_ATTRS = ['value']


PIPELINE_AGGS = {
    agg.AGG_TYPE: agg
    for agg in [
        AvgBucket,
        Derivative,
        MaxBucket,
        MinBucket,
        SumBucket,
        StatsBucket,
        ExtendedStatsBucket,
        PercentilesBucket,
        MovingAvg,
        CumulativeSum,
        BucketScript,
        BucketSelector,
        SerialDiff
    ]
}
