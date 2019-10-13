#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.utils import Obj, validate_client
from pandagg.aggs.agg import (
    Agg, Mapping, TreeMapping, ClientBoundAggregation
)


class Index(Obj):

    def __init__(self, name, settings, mapping, aliases, warmers):
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
        return Agg(mapping=self.mapping).groupby(by, **kwargs)

    def agg(self, arg, output=None, **kwargs):
        return Agg(mapping=self.mapping).agg(arg, **kwargs)

    def __repr__(self):
        return '<Index %s>' % self.name


class ClientBoundIndex(Index):

    def __init__(self, client, name, settings, mapping, aliases, warmers):
        self.client = client
        if client is not None:
            validate_client(self.client)
        super(ClientBoundIndex, self).__init__(name, settings, mapping, aliases, warmers)

    def query(self, query, validate=False):
        return ClientBoundAggregation(client=self.client, mapping=self.mapping).query(query, validate=validate)

    def groupby(self, by, **kwargs):
        return ClientBoundAggregation(client=self.client, mapping=self.mapping).groupby(by, **kwargs)

    def agg(self, arg, output=Agg.DEFAULT_OUTPUT, execute=True, **kwargs):
        return ClientBoundAggregation(client=self.client, mapping=self.mapping).agg(arg, execute=execute, **kwargs)
