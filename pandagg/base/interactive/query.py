#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.base.tree.query import Query


class ClientBoundQuery(Query):

    def __init__(self, client, index_name, mapping=None, from_=None, identifier=None):
        self.client = client
        self.index_name = index_name
        super(ClientBoundQuery, self).__init__(
            from_=from_,
            mapping=mapping,
            identifier=identifier
        )

    def _clone(self, identifier=None, with_tree=False, deep=False):
        return ClientBoundQuery(
            client=self.client,
            index_name=self.index_name,
            mapping=self.tree_mapping,
            identifier=identifier,
            from_=self if with_tree else None
        )

    def execute(self, index=None, **kwargs):
        body = {'query': self.query_dict()}
        body.update(kwargs)
        return self.client.search(index=index, body=body)
