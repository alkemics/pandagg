#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from six import iteritems
from elasticsearch import Elasticsearch
from pandagg.index.index import ClientBoundIndex, Indices


class PandAgg(Elasticsearch):

    def fetch_indices(self, index='*'):
        """
        :param index: Comma-separated list or wildcard expression of index names used to limit the request.
        """
        indices = Indices()
        for index_name, index_detail in iteritems(self.indices.get(index=index)):
            indices[index_name] = ClientBoundIndex(
                client=self,
                name=index_name,
                mapping=index_detail['mappings'],
                settings=index_detail['settings'],
                aliases=index_detail['aliases'],
            )
        return indices
