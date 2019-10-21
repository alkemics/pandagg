#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.tree import Node
from pandagg.utils import PrettyNode


class Bucket(Node):

    REPR_SIZE = 60

    def __init__(self, aggregation_node, value, lvl, key=None, override_current_level=None, identifier=None):
        self.aggregation_node = aggregation_node
        self.value = value
        self.lvl = lvl
        # `override_current_level` is only used to create root node of response tree
        self.current_level = override_current_level or aggregation_node.agg_name
        self.current_key = key
        if self.current_key is not None:
            self.path = '%s_%s' % (self.current_level.replace('.', '_'), self.current_key)
        else:
            self.path = self.current_level.replace('.', '_')
        pretty = self._str_current_level(
            level=self.current_level,
            key=self.current_key,
            lvl=self.lvl, sep='=',
            value=self.extract_bucket_value()
        )
        super(Bucket, self).__init__(data=PrettyNode(pretty=pretty), identifier=identifier)

    @classmethod
    def _str_current_level(cls, level, key, lvl, sep=':', value=None):
        s = level
        if key is not None:
            s = '%s%s%s' % (s, sep, key)
        if value is not None:
            pad = max(cls.REPR_SIZE - 4 * lvl - len(s) - len(str(value)), 4)
            s = s + ' ' * pad + str(value)
        return s

    def extract_bucket_value(self, value_as_dict=False):
        attrs = self.aggregation_node.VALUE_ATTRS
        if value_as_dict:
            return {attr_: self.value.get(attr_) for attr_ in attrs}
        return self.value.get(attrs[0])

    def bind(self, tree, client=None, index_name=None):
        if client is not None:
            return ClientBoundBucket(
                client=client,
                index_name=index_name,
                tree=tree,
                aggregation_node=self.aggregation_node,
                value=self.value,
                lvl=self.lvl,
                key=self.current_key,
                identifier=self.identifier
            )
        return TreeBoundBucket(
            tree=tree,
            aggregation_node=self.aggregation_node,
            value=self.value,
            lvl=self.lvl,
            key=self.current_key,
            identifier=self.identifier
        )

    def __repr__(self):
        return u'<Bucket, identifier={identifier}>\n{pretty}' \
            .format(identifier=self.identifier, pretty=self.data.pretty).encode('utf-8')


class TreeBoundBucket(Bucket):

    def __init__(self, tree, aggregation_node, value, lvl, identifier, key=None):
        self._tree = tree
        super(TreeBoundBucket, self).__init__(
            aggregation_node=aggregation_node,
            value=value,
            lvl=lvl,
            key=key,
            identifier=identifier
        )


class ClientBoundBucket(TreeBoundBucket):

    def __init__(self, client, index_name, tree, aggregation_node, value, lvl, identifier, key=None):
        self.client = client
        self.index_name = index_name
        super(ClientBoundBucket, self).__init__(
            tree=tree,
            aggregation_node=aggregation_node,
            value=value,
            lvl=lvl,
            key=key,
            identifier=identifier
        )

    def list_documents(self, size=None, execute=True, _source=None, **kwargs):
        filter_query = super(ClientBoundBucket, self).list_documents()
        if not execute:
            return filter_query
        body = {"query": filter_query}
        if size is not None:
            body["size"] = size
        if _source is not None:
            body["_source"] = _source
        body.update(kwargs)
        return self.client.search(index=self.index_name, body=body)['hits']
