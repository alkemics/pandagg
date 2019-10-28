#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from six import iteritems

from elasticsearch import Elasticsearch

from pandagg.mapping.mapping import ClientBoundMapping, MappingTree, Mapping
from pandagg.aggs.agg import Agg, ClientBoundAgg
from pandagg.utils import Obj


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
        mapping_name, mapping_detail = next(iteritems(mapping))
        self.mapping = Mapping(tree=MappingTree(mapping_name, mapping_detail), depth=1)

    def groupby(self, by, **kwargs):
        return Agg(mapping=self.mapping).groupby(by, **kwargs)

    def agg(self, arg, output=None, **kwargs):
        return Agg(mapping=self.mapping).agg(arg, output=output, **kwargs)


class Indices(Obj):
    pass


class Aliases(Obj):
    pass


class ClientBoundIndex(Index):

    def __init__(self, client, name, settings, mapping, aliases, warmers):
        self.client = client
        if client is not None:
            assert isinstance(client, Elasticsearch)
        super(ClientBoundIndex, self).__init__(
            name=name,
            settings=settings,
            mapping=mapping,
            aliases=aliases,
            warmers=warmers
        )

    def set_mapping(self, mapping):
        mapping_name, mapping_detail = next(iteritems(mapping))
        self.mapping = ClientBoundMapping(
            client=self.client,
            index_name=self.name,
            tree=MappingTree(mapping_name, mapping_detail),
            depth=1
        )

    def query(self, query, validate=False):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.name,
            mapping=self.mapping
        ).query(query, validate=validate)

    def groupby(self, by, **kwargs):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.name,
            mapping=self.mapping
        ).groupby(by, **kwargs)

    def agg(self, arg, output=Agg.DEFAULT_OUTPUT, execute=True, **kwargs):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.name,
            mapping=self.mapping
        ).agg(arg, execute=execute, output=output, **kwargs)
