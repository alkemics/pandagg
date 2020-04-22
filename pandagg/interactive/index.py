#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from lighttree.interactive import Obj
from pandagg.tree.mapping import Mapping
from pandagg.interactive.mapping import IMapping
from pandagg.tree.agg import Agg


class Index(Obj):
    def __init__(self, name, settings, mapping, aliases, client=None):
        super(Index, self).__init__()
        self.client = client
        self.name = name
        self.settings = settings
        self.mapping = None
        self.set_mapping(mapping)
        self.aliases = aliases

    def set_mapping(self, mapping):
        self.mapping = IMapping(
            Mapping(mapping), client=self.client, index_name=self.name, depth=1
        )

    def query(self, query, validate=False, **kwargs):
        return Agg(
            client=self.client, index_name=self.name, mapping=self.mapping
        ).query(query, validate=validate, **kwargs)

    def groupby(self, *args, **kwargs):
        return Agg(
            client=self.client, index_name=self.name, mapping=self.mapping
        ).groupby(*args, **kwargs)

    def agg(self, *args, **kwargs):
        return Agg(client=self.client, index_name=self.name, mapping=self.mapping).agg(
            *args, **kwargs
        )


class Indices(Obj):
    _COERCE_ATTR = True


class Aliases(Obj):
    _COERCE_ATTR = True
