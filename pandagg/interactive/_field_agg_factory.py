#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.node.aggs.abstract import AggNode

from pandagg.node.types import MAPPING_TYPES

from operator import itemgetter


def list_available_aggs_on_field(field_type):
    """For a given field type, return all aggregations that can be operated on this field.
    If WHITELISTED_MAPPING_TYPES is defined, field type must be in it. Else if BLACKLISTED_MAPPING_TYPES is defined,
    field type must not be in it.
    """
    return [
        agg_class
        for agg_class in AggNode._classes.values()
        if agg_class.valid_on_field_type(field_type)
    ]


def field_klass_init(self, mapping_tree, client, field, index):
    self._mapping_tree = mapping_tree
    self._client = client
    self._field = field
    self._index = index


def aggregator_factory(agg_klass):
    def aggregator(self, index=None, raw_output=False, query=None, **kwargs):
        node = agg_klass(name="%s_agg" % agg_klass.KEY, field=self._field, **kwargs)
        return self._operate(node, index, raw_output, query)

    aggregator.__doc__ = agg_klass.__init__.__doc__ or agg_klass.__doc__
    return aggregator


def _operate(self, agg_node, index, raw_output, query):
    index = index or self._index
    aggregation = {agg_node.name: agg_node.to_dict()}
    nesteds = self._mapping_tree.list_nesteds_at_field(self._field) or []
    for nested in nesteds:
        aggregation = {nested: {"nested": {"path": nested}, "aggs": aggregation}}

    body = {"aggs": aggregation, "size": 0}
    if query is not None:
        body["query"] = query
    raw_response = self._client.search(index=index, body=body)["aggregations"]
    for nested in nesteds:
        raw_response = raw_response[nested]
    result = list(agg_node.extract_buckets(raw_response[agg_node.name]))

    if raw_output:
        return result
    try:
        import pandas as pd
    except ImportError:
        return result
    keys = map(itemgetter(0), result)
    raw_values = map(itemgetter(1), result)
    return pd.DataFrame(index=keys, data=raw_values)


def field_type_klass_factory(field_type):
    d = {"__init__": field_klass_init, "_operate": _operate}
    for agg_klass in list_available_aggs_on_field(field_type):
        d[agg_klass.KEY] = aggregator_factory(agg_klass)
    klass = type("%sAggs" % field_type.capitalize(), (), d)
    return klass


field_classes_per_name = {
    field_type: field_type_klass_factory(field_type) for field_type in MAPPING_TYPES
}
