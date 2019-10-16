#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.nodes.agg_nodes import PUBLIC_AGGS

MAPPING_TYPES = [
    'boolean',
    'integer',
    'float',
    'date',
    'string'
]


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


def field_klass_init(self, client, field):
    self._client = client
    self._field = field


def aggregator_factory(agg_klass):
    def aggregator(self, index=None, **kwargs):
        node = agg_klass(
            agg_name='%sAgg' % agg_klass.AGG_TYPE.capitalize(),
            field=self._field,
            **kwargs
        )
        return self._operate(node, index)
    aggregator.__doc__ = agg_klass.__doc__
    return aggregator


def _operate(self, agg_node, index):
    aggregation = {agg_node.agg_name: agg_node.query_dict()}
    if self._client is not None:
        body = {"aggs": aggregation, "size": 0}
        raw_response = self._client.search(index=index, body=body)['aggregations'][agg_node.agg_name]
        return list(agg_node.extract_buckets(raw_response))
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
