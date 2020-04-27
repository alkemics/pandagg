#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Pipeline aggregations:
https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations-pipeline.html
"""

from pandagg.node.aggs.abstract import Pipeline, ScriptPipeline


class AvgBucket(Pipeline):
    KEY = "avg_bucket"
    VALUE_ATTRS = ["value"]


class Derivative(Pipeline):
    KEY = "derivative"
    VALUE_ATTRS = ["value"]


class MaxBucket(Pipeline):
    KEY = "max_bucket"
    VALUE_ATTRS = ["value"]


class MinBucket(Pipeline):
    KEY = "min_bucket"
    VALUE_ATTRS = ["value"]


class SumBucket(Pipeline):
    KEY = "sum_bucket"
    VALUE_ATTRS = ["value"]


class StatsBucket(Pipeline):
    KEY = "stats_bucket"
    VALUE_ATTRS = ["count", "min", "max", "avg", "sum"]


class ExtendedStatsBucket(Pipeline):
    KEY = "extended_stats_bucket"
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


class PercentilesBucket(Pipeline):
    KEY = "percentiles_bucket"
    VALUE_ATTRS = ["values"]


class MovingAvg(Pipeline):
    KEY = "moving_avg"
    VALUE_ATTRS = ["value"]


class CumulativeSum(Pipeline):
    KEY = "cumulative_sum"
    VALUE_ATTRS = ["value"]


class BucketScript(ScriptPipeline):
    KEY = "bucket_script"
    VALUE_ATTRS = ["value"]


class BucketSelector(ScriptPipeline):
    KEY = "bucket_selector"
    VALUE_ATTRS = None


class BucketSort(ScriptPipeline):
    KEY = "bucket_sort"
    VALUE_ATTRS = None


class SerialDiff(Pipeline):
    KEY = "serial_diff"
    VALUE_ATTRS = ["value"]
