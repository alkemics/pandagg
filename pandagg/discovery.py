#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree.interactive import Obj

from pandagg.interactive.mappings import IMappings
from pandagg.search import Search


def discover(using, index="*"):
    """
    :param using: Elasticsearch client
    :param index: Comma-separated list or wildcard expression of index names used to limit the request.
    """
    indices = Indices()
    for index_name, index_detail in using.indices.get(index=index).items():
        indices[index_name] = Index(
            client=using,
            name=index_name,
            mappings=index_detail["mappings"],
            settings=index_detail["settings"],
            aliases=index_detail["aliases"],
        )
    return indices


# until Proper Index class is written


class Index(object):
    def __init__(self, name, settings, mappings, aliases, client=None):
        super(Index, self).__init__()
        self.client = client
        self.name = name
        self.settings = settings
        self._mappings = mappings
        self.mappings = IMappings(mappings, client=client, index=name)
        self.aliases = aliases

    def search(self, nested_autocorrect=True, repr_auto_execute=True):
        return Search(
            using=self.client,
            mappings=self._mappings,
            index=self.name,
            nested_autocorrect=nested_autocorrect,
            repr_auto_execute=repr_auto_execute,
        )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Index '%s'>" % self.name


class Indices(Obj):
    _COERCE_ATTR = True
