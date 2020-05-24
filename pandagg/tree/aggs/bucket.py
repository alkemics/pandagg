#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .aggs import AbstractParentAgg


class Global(AbstractParentAgg):
    KEY = "global"


class Filter(AbstractParentAgg):
    KEY = "filter"


class Nested(AbstractParentAgg):
    KEY = "nested"


class ReverseNested(AbstractParentAgg):
    KEY = "reverse_nested"


class Missing(AbstractParentAgg):
    KEY = "missing"


class Terms(AbstractParentAgg):
    KEY = "terms"


class Filters(AbstractParentAgg):
    KEY = "filters"


class Histogram(AbstractParentAgg):
    KEY = "histogram"


class DateHistogram(AbstractParentAgg):
    KEY = "date_histogram"


class Range(AbstractParentAgg):
    KEY = "range"


class DateRange(AbstractParentAgg):
    KEY = "date_range"


class Composite(AbstractParentAgg):
    KEY = "composite"
