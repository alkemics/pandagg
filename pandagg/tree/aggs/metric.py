#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .aggs import AbstractLeafAgg


class TopHits(AbstractLeafAgg):
    KEY = "top_hits"


class Avg(AbstractLeafAgg):
    KEY = "avg"


class Sum(AbstractLeafAgg):
    KEY = "sum"


class Max(AbstractLeafAgg):
    KEY = "max"


class Min(AbstractLeafAgg):
    KEY = "min"


class Cardinality(AbstractLeafAgg):
    KEY = "cardinality"


class Stats(AbstractLeafAgg):
    KEY = "stats"


class ExtendedStats(AbstractLeafAgg):
    KEY = "extended_stats"


class GeoBound(AbstractLeafAgg):
    KEY = "geo_bounds"


class GeoCentroid(AbstractLeafAgg):
    KEY = "geo_centroid"


class Percentiles(AbstractLeafAgg):
    """Percents body argument can be passed to specify which percentiles to fetch."""

    KEY = "percentiles"


class PercentileRanks(AbstractLeafAgg):
    KEY = "percentile_ranks"


class ValueCount(AbstractLeafAgg):
    KEY = "value_count"
