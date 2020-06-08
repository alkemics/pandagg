#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from future.utils import iteritems, python_2_unicode_compatible
from lighttree.interactive import Obj

from pandagg.interactive.mapping import IMapping
from pandagg.search import Search


def discover(using, index="*"):
    """
    :param using: Elasticsearch client
    :param index: Comma-separated list or wildcard expression of index names used to limit the request.
    """
    indices = Indices()
    for index_name, index_detail in iteritems(using.indices.get(index=index)):
        indices[index_name] = Index(
            client=using,
            name=index_name,
            mapping=index_detail["mappings"],
            settings=index_detail["settings"],
            aliases=index_detail["aliases"],
        )
    return indices


# until Proper Index class is written


@python_2_unicode_compatible
class Index(object):
    def __init__(self, name, settings, mapping, aliases, client=None):
        super(Index, self).__init__()
        self.client = client
        self.name = name
        self.settings = settings
        self._mapping = mapping
        self.mapping = IMapping(mapping, client=client, index=name)
        self.aliases = aliases

    def search(self, nested_autocorrect=True):
        return Search(
            using=self.client,
            mapping=self._mapping,
            index=self.name,
            nested_autocorrect=nested_autocorrect,
        )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Index '%s'>" % self.name


class Indices(Obj):
    _COERCE_ATTR = True
