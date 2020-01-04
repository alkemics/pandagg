#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from pandagg.base.tree.agg import Agg
from pandagg.base.tree.response import ResponseTree
from pandagg.base.interactive.response import ClientBoundResponse
from pandagg.base.utils import bool_if_required


class ClientBoundAgg(Agg):

    def __init__(self, client, index_name, mapping=None, from_=None, query=None, identifier=None):
        self.client = client
        self.index_name = index_name
        self._query = query
        super(ClientBoundAgg, self).__init__(
            from_=from_,
            mapping=mapping,
            identifier=identifier
        )

    def _interpret_agg(self, insert_below, element, **kwargs):
        if isinstance(element, ClientBoundAgg):
            self.paste(nid=insert_below, new_tree=element)
            return self
        return super(ClientBoundAgg, self)._interpret_agg(insert_below, element, **kwargs)

    def _serialize_as_tree(self, aggs):
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
            from_=self if with_tree else None
        )

    def query(self, query, validate=False):
        assert isinstance(query, dict)
        if validate:
            validity = self.client.indices.validate_query(index=self.index_name, body={"query": query})
            if not validity['valid']:
                raise ValueError('Wrong query: %s\n%s' % (query, validity))
        new_agg = self._clone(with_tree=True)

        conditions = [query]
        if new_agg._query is not None:
            conditions.append(new_agg._query)
        new_agg._query = bool_if_required(conditions)
        return new_agg

    def _execute(self, aggregation, index=None, query=None):
        body = {"aggs": aggregation, "size": 0}
        if query:
            body['query'] = query
        return self.client.search(index=index, body=body)

    def execute(self, index=None, output=Agg.DEFAULT_OUTPUT, **kwargs):
        es_response = self._execute(
            aggregation=self.query_dict(),
            index=index or self.index_name,
            query=self._query
        )
        return self.serialize(
            aggs=es_response['aggregations'],
            output=output,
            **kwargs
        )
