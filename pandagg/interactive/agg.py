#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from builtins import str as text
from six import python_2_unicode_compatible

from pandagg.tree.agg import Agg
from pandagg.tree.query import Query
from pandagg.tree.response import ResponseTree
from pandagg.interactive.response import ClientBoundResponse


@python_2_unicode_compatible
class ClientBoundAgg(Agg):

    def __init__(self, client, index_name, mapping=None, from_=None, query=None, identifier=None):
        self.client = client
        self.index_name = index_name
        self._query = Query(from_=query)
        super(ClientBoundAgg, self).__init__(
            from_=from_,
            mapping=mapping,
            identifier=identifier
        )

    def _deserialize_extended(self, insert_below, element, **kwargs):
        if isinstance(element, ClientBoundAgg):
            self.paste(nid=insert_below, new_tree=element)
            return self
        return super(ClientBoundAgg, self)._deserialize_extended(insert_below, element, **kwargs)

    def _serialize_response_as_tree(self, aggs):
        response_tree = ResponseTree(self).parse_aggregation(aggs)
        return ClientBoundResponse(
            client=self.client,
            index_name=self.index_name,
            tree=response_tree,
            depth=1,
            query=self._query
        )

    def _clone(self, identifier=None, with_tree=False, deep=False):
        return ClientBoundAgg(
            client=self.client,
            index_name=self.index_name,
            mapping=self.tree_mapping,
            identifier=identifier,
            query=self._query,
            from_=self if with_tree and len(self.nodes) else None
        )

    def query(self, query, validate=False, **kwargs):
        new_query = self._query.query(query, **kwargs)
        query_dict = new_query.query_dict()
        if validate:
            validity = self.client.indices.validate_query(index=self.index_name, body={"query": query_dict})
            if not validity['valid']:
                raise ValueError('Wrong query: %s\n%s' % (query, validity))
        new_agg = self._clone(with_tree=True)
        new_agg._query = new_query
        return new_agg

    def _execute(self, aggregation, index=None):
        body = {"aggs": aggregation, "size": 0}
        query = self._query.query_dict()
        if query:
            body['query'] = query
        return self.client.search(index=index, body=body)

    def execute(self, index=None, output=Agg.DEFAULT_OUTPUT, **kwargs):
        es_response = self._execute(
            aggregation=self.query_dict(),
            index=index or self.index_name
        )
        return self.serialize_response(
            aggs=es_response['aggregations'],
            output=output,
            **kwargs
        )

    def __str__(self):
        base = '<ClientBoundAggregation>\n%s' % text(self.show())
        if self._query.root:
            base += '\n\nWith Query:\n%s' % text(self._query.show())
        return base
