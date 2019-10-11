#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict

from elasticsearch import Elasticsearch
from pandagg.utils import Obj
from pandagg.index.index import ClientBoundIndex
from pandagg.aggs.aggregation import (
    PUBLIC_AGGS, AggregationNode, Aggregation
)
from pandagg.wrapper.method_generator import _method_generator


class PandAgg:

    def __init__(self, client):
        assert isinstance(client, Elasticsearch)
        self.client = client
        self.indices = Obj()
        self.aliases = Obj()
        self._indices = None

    for agg_class in PUBLIC_AGGS.values():
        exec _method_generator(agg_class)

    def fetch_indices(self, index='*'):
        """
        :param index: Comma-separated list or wildcard expression of index names used to limit the request.
        """
        # index_name -> {'warmers', 'mappings', 'aliases', 'settings'}
        self._indices = self.client.indices.get(index=index)

        alias_to_indices = defaultdict(set)
        for index_name, index_detail in self._indices.iteritems():
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

        for alias, indices_names in alias_to_indices.iteritems():
            self.aliases[alias] = list(indices_names)

    def execute(self, index, aggs, query):
        """
        :param aggregation:
        :return: dataframe, or tree, or raw
        """
        if isinstance(aggs, dict):
            aggs_tree = Aggregation(from_=aggs)
            if aggs_tree.agg_dict() != aggs:
                raise NotImplementedError("Some stuff is not implemented yet.")
        elif isinstance(aggs, AggregationNode):
            # aggs_tree = aggs.as_tree()
            # Aggregation(aggs)
            raise NotImplementedError()
        elif isinstance(aggs, Aggregation):
            aggs_tree = aggs
        else:
            raise Exception("Unsupported type of aggs: %s" % type(aggs))
        assert len(aggs_tree.nodes.keys())
        dict_aggs = aggs_tree.agg_dict()
        body = {"aggs": dict_aggs, "size": 0}
        if query:
            validity = self.client.indices.validate_query(index=index, body={"query": query})
            if not validity['valid']:
                raise ValueError('Wrong query: %s\n%s' % (query, validity))
            body['query'] = query
        response = self.client.search(index=index, body={"aggs": dict_aggs, "size": 0})['aggregations']
        return response
