#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from pandagg.base.tree.mapping import Mapping
from pandagg.base.interactive.mapping import ClientBoundMapping, IMapping
from pandagg.base.tree.agg import Agg
from pandagg.base.interactive.agg import ClientBoundAgg
from pandagg.base.interactive.abstract import Obj


class Index(Obj):

    def __init__(self, name, settings, mapping, aliases):
        super(Index, self).__init__()
        self.name = name
        self.settings = settings
        self.mapping = None
        self.set_mapping(mapping)
        self.aliases = aliases

    def set_mapping(self, mapping):
        self.mapping = IMapping(tree=Mapping(mapping), depth=1)

    def groupby(self, by, **kwargs):
        return Agg(mapping=self.mapping).groupby(by, **kwargs)

    def agg(self, arg, output=None, **kwargs):
        return Agg(mapping=self.mapping).agg(arg, output=output, **kwargs)


class Indices(Obj):
    _COERCE_ATTR = True


class Aliases(Obj):
    _COERCE_ATTR = True


class ClientBoundIndex(Index):

    def __init__(self, client, name, settings, mapping, aliases):
        self.client = client
        super(ClientBoundIndex, self).__init__(
            name=name,
            settings=settings,
            mapping=mapping,
            aliases=aliases
        )

    def set_mapping(self, mapping):
        self.mapping = ClientBoundMapping(
            client=self.client,
            index_name=self.name,
            tree=Mapping(mapping),
            depth=1
        )

    def query(self, query, validate=False, **kwargs):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.name,
            mapping=self.mapping
        ).query(query, validate=validate, **kwargs)

    def groupby(self, by, insert_below=None, **kwargs):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.name,
            mapping=self.mapping
        ).groupby(by, insert_below=insert_below, **kwargs)

    def agg(self, arg, insert_below=None, output=Agg.DEFAULT_OUTPUT, execute=True, **kwargs):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.name,
            mapping=self.mapping
        ).agg(arg, insert_below=insert_below, execute=execute, output=output, **kwargs)
