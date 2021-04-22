#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.node.aggs.abstract import AggClause, BucketAggClause

from pandagg.node.types import MAPPING_TYPES


def list_available_aggs_on_field(field_type):
    """For a given field type, return all aggregations that can be operated on this field.
    If WHITELISTED_MAPPING_TYPES is defined, field type must be in it. Else if BLACKLISTED_MAPPING_TYPES is defined,
    field type must not be in it.
    """
    return [
        agg_class
        for agg_class in AggClause._classes.values()
        if agg_class.valid_on_field_type(field_type)
    ]


def field_klass_init(self, field, search):
    self._field = field
    self._search = search


def aggregator_factory(agg_klass):
    def aggregator(self, **kwargs):
        if issubclass(agg_klass, BucketAggClause):
            return self._search.groupby(
                "%s_%s" % (agg_klass.KEY, self._field),
                agg_klass(field=self._field, **kwargs),
            )
        return self._search.agg(
            "%s_%s" % (agg_klass.KEY, self._field),
            agg_klass(field=self._field, **kwargs),
        )

    aggregator.__doc__ = agg_klass.__init__.__doc__ or agg_klass.__doc__
    return aggregator


def field_type_klass_factory(field_type):
    d = {"__init__": field_klass_init}
    for agg_klass in list_available_aggs_on_field(field_type):
        d[agg_klass.KEY] = aggregator_factory(agg_klass)
    klass = type("%sAggs" % field_type.capitalize(), (), d)
    return klass


field_classes_per_name = {
    field_type: field_type_klass_factory(field_type) for field_type in MAPPING_TYPES
}
