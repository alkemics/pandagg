#!/usr/bin/env python
# -*- coding: utf-8 -*-
from operator import itemgetter

import pandas as pd
from pandagg.nodes import PUBLIC_AGGS
from .types import MAPPING_TYPES


def list_available_aggs_on_field(field_type):
    """For a given field type, return all aggregations that can be operated on this field.
    If WHITELISTED_MAPPING_TYPES is defined, field type must be in it. Else if BLACKLISTED_MAPPING_TYPES is defined,
    field type must not be in it.
    """
    return [
        agg_class
        for agg_class in PUBLIC_AGGS.values()
        if agg_class.valid_on_field_type(field_type)
    ]


def field_klass_init(self, mapping_tree, client, field):
    self._mapping_tree = mapping_tree
    self._client = client
    self._field = field


def aggregator_factory(agg_klass):
    def aggregator(self, index=None, execute=True, output='dataframe', **kwargs):
        node = agg_klass(
            agg_name='%sAgg' % agg_klass.AGG_TYPE.capitalize(),
            field=self._field,
            **kwargs
        )
        return self._operate(node, index, execute, output)
    aggregator.__doc__ = agg_klass.__doc__
    return aggregator


def _operate(self, agg_node, index, execute, output):
    aggregation = {agg_node.agg_name: agg_node.query_dict()}
    nesteds = self._mapping_tree.list_nesteds_at_field(self._field) or []
    for nested in nesteds:
        aggregation = {
            nested: {
                'nested': {'path': nested},
                'aggs': aggregation
            }
        }

    if self._client is not None and execute:
        body = {"aggs": aggregation, "size": 0}
        raw_response = self._client.search(index=index, body=body)['aggregations']
        for nested in nesteds:
            raw_response = raw_response[nested]
        result = list(agg_node.extract_buckets(raw_response[agg_node.agg_name]))
        if output != 'dataframe':
            return result
        keys = map(itemgetter(0), result)
        raw_values = map(agg_node.extract_bucket_value, map(itemgetter(1), result))
        return pd.DataFrame(index=keys, data=raw_values, columns=[agg_node.VALUE_ATTRS[0]])
    return aggregation


def field_type_klass_factory(field_type):
    d = {
        '__init__': field_klass_init,
        '_operate': _operate
    }
    for agg_klass in list_available_aggs_on_field(field_type):
        d[agg_klass.AGG_TYPE] = aggregator_factory(agg_klass)
    klass = type(
        "%sAggs" % field_type.capitalize(),
        (),
        d
    )
    return klass


field_classes_per_name = {
    field_type: field_type_klass_factory(field_type)
    for field_type in MAPPING_TYPES
}