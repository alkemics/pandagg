#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from six import iteritems
from collections import defaultdict

from pandagg.exceptions import VersionIncompatibilityError
from pandagg.utils import validate_client
from pandagg.index.index import ClientBoundIndex, Indices, Aliases


class PandAgg:

    ES_COMPATIBILITY_VERSIONS = ('2',)

    def __init__(self, client):
        self.client = client
        validate_client(self.client)
        self.indices = Indices()
        self.aliases = Aliases()
        self._indices = None
        self._info = None

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
