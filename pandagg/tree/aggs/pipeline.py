#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AbstractParentAgg aggregations:
https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-aggregations-pipeline.html
"""

from .aggs import AbstractParentAgg


class AvgBucket(AbstractParentAgg):
    KEY = "avg_bucket"


class Derivative(AbstractParentAgg):
    KEY = "derivative"


class MaxBucket(AbstractParentAgg):
    KEY = "max_bucket"


class MinBucket(AbstractParentAgg):
    KEY = "min_bucket"


class SumBucket(AbstractParentAgg):
    KEY = "sum_bucket"


class StatsBucket(AbstractParentAgg):
    KEY = "stats_bucket"


class ExtendedStatsBucket(AbstractParentAgg):
    KEY = "extended_stats_bucket"


class PercentilesBucket(AbstractParentAgg):
    KEY = "percentiles_bucket"


class MovingAvg(AbstractParentAgg):
    KEY = "moving_avg"


class CumulativeSum(AbstractParentAgg):
    KEY = "cumulative_sum"


class BucketScript(AbstractParentAgg):
    KEY = "bucket_script"


class BucketSelector(AbstractParentAgg):
    KEY = "bucket_selector"


class BucketSort(AbstractParentAgg):
    KEY = "bucket_sort"


class SerialDiff(AbstractParentAgg):
    KEY = "serial_diff"
