#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.utils import Obj, validate_client
from pandagg.mapping.mapping import ClientBoundMapping
from pandagg.aggs.agg import (
    Agg, Mapping, MappingTree, ClientBoundAggregation
)


class Index(Obj):

    def __init__(self, name, settings, mapping, aliases, warmers):
        super(Index, self).__init__()
        self.name = name
        self.settings = settings
        self.mapping = None
        self.set_mapping(mapping)
        self.aliases = aliases
        self.warmers = warmers

    def set_mapping(self, mapping):
        mapping_name, mapping_detail = next(mapping.iteritems())
        self.mapping = Mapping(tree=MappingTree(mapping_name, mapping_detail), depth=1)

    def groupby(self, by, **kwargs):
        return Agg(mapping=self.mapping).groupby(by, **kwargs)

    def agg(self, arg, output=None, **kwargs):
        return Agg(mapping=self.mapping).agg(arg, **kwargs)


class Indices(Obj):
    pass


class Aliases(Obj):
    pass


class ClientBoundIndex(Index):

    def __init__(self, client, name, settings, mapping, aliases, warmers):
        self.client = client
        if client is not None:
            validate_client(self.client)
        super(ClientBoundIndex, self).__init__(name, settings, mapping, aliases, warmers)

    def set_mapping(self, mapping):
        mapping_name, mapping_detail = next(mapping.iteritems())
        self.mapping = ClientBoundMapping(client=self.client, tree=MappingTree(mapping_name, mapping_detail), depth=1)

    def query(self, query, validate=False):
        return ClientBoundAggregation(client=self.client, mapping=self.mapping).query(query, validate=validate)

    def groupby(self, by, **kwargs):
        return ClientBoundAggregation(client=self.client, mapping=self.mapping).groupby(by, **kwargs)

    def agg(self, arg, output=Agg.DEFAULT_OUTPUT, execute=True, **kwargs):
        return ClientBoundAggregation(client=self.client, mapping=self.mapping).agg(arg, execute=execute, **kwargs)
