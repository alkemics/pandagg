#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from six import iteritems
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.transport import Transport
from pandagg.exceptions import VersionIncompatibilityError
from pandagg.index.index import ClientBoundIndex, Indices, Aliases


def pandagg_init(self, hosts=None, transport_class=Transport, **kwargs):
    self.client = Elasticsearch(hosts=hosts, transport_class=transport_class, **kwargs)
    self.indices = Indices()
    self.aliases = Aliases()
    self._indices = None
    self._info = None


pandagg_init.__doc__ = Elasticsearch.__init__.__doc__


class PandAgg:
    """Wrapper around elasticsearch.Elasticsearch client.
    """

    ES_COMPATIBILITY_VERSIONS = ('2',)

    __init__ = pandagg_init

    def fetch_indices(self, index='*'):
        """
        :param index: Comma-separated list or wildcard expression of index names used to limit the request.
        """
        # index_name -> {'warmers', 'mappings', 'aliases', 'settings'}
        self._indices = self.client.indices.get(index=index)

        alias_to_indices = defaultdict(set)
        for index_name, index_detail in iteritems(self._indices):
            self.indices[index_name] = ClientBoundIndex(
                client=self.client,
                name=index_name,
                mapping=index_detail['mappings'],
                settings=index_detail['settings'],
                warmers=index_detail['warmers'],
                aliases=index_detail['aliases'],
            )
            for alias in index_detail.get('aliases', {}).keys():
                alias_to_indices[alias].add(index_name)

        for alias, indices_names in alias_to_indices.items():
            self.aliases[alias] = list(indices_names)

    def validate_version(self):
        self._info = self.client.info()
        version = self._info.get('version', {}).get('number') or ''
        major_version = next(iter(version.split('.')), None)
        if major_version not in self.ES_COMPATIBILITY_VERSIONS:
            raise VersionIncompatibilityError(
                'ElasticSearch version %s is not compatible with this Pandagg release. Allowed ES versions are: %s.' % (
                    version, list(self.ES_COMPATIBILITY_VERSIONS))
            )
        return True
