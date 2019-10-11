#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.utils import Obj, validate_client
from pandagg.aggs.aggregation import (
    Aggregation, Mapping, TreeMapping, ClientBoundAggregation
)


class Index(Obj):

    def __init__(self,  name, settings, mapping, aliases, warmers):
        super(Index, self).__init__()
        self.name = name
        self.settings = settings
        mapping_name, mapping_detail = next(mapping.iteritems())
        self.mapping = Mapping(tree=TreeMapping(mapping_name, mapping_detail))
        self.aliases = aliases
        self.warmers = warmers

    def override_mapping(self, mapping):
        mapping_name, mapping_detail = next(mapping.iteritems())
        self.mapping = Mapping(tree=TreeMapping(mapping_name, mapping_detail))

    def groupby(self, by, **kwargs):
        return Aggregation(
            mapping=self.mapping,
            output=kwargs.get('output', Aggregation.DEFAULT_OUTPUT)
        ).groupby(by, **kwargs)

    def agg(self, arg, **kwargs):
        return Aggregation(
            mapping=self.mapping,
            output=kwargs.get('output', Aggregation.DEFAULT_OUTPUT)
        ).agg(arg, **kwargs)

    def __repr__(self):
        return '<Index %s>' % self.name


class ClientBoundIndex(Index):

    def __init__(self, client, name, settings, mapping, aliases, warmers):
        self.client = client
        if client is not None:
            validate_client(self.client)
        super(ClientBoundIndex, self).__init__(name, settings, mapping, aliases, warmers)

    def groupby(self, by, **kwargs):
        return ClientBoundAggregation(
            client=self.client,
            mapping=self.mapping,
            output=kwargs.get('output', Aggregation.DEFAULT_OUTPUT)
        ).groupby(by, **kwargs)

    def agg(self, arg, **kwargs):
        return ClientBoundAggregation(
            client=self.client,
            mapping=self.mapping,
            output=kwargs.get('output', Aggregation.DEFAULT_OUTPUT)
        ).agg(arg, **kwargs)
