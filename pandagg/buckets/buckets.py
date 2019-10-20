#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.tree import Node
from pandagg.utils import bool_if_required, PrettyNode
from collections import defaultdict


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

    def bucket_properties(self, end_level=None, depth=None):
        """Bucket properties (including parents) relative to this tree.
        TODO - optimize using rsearch
        """
        return self._tree.bucket_properties(self, end_level=end_level, depth=depth)

    @classmethod
    def _build_filter(cls, nid_to_children, filters_per_nested_level, current_nested_path=None):
        """Recursive function to build bucket filters from highest to deepest nested conditions.
        """
        current_conditions = filters_per_nested_level.get(current_nested_path, [])
        nested_children = nid_to_children[current_nested_path]
        for nested_child in nested_children:
            nested_child_conditions = cls._build_filter(
                nid_to_children=nid_to_children,
                filters_per_nested_level=filters_per_nested_level,
                current_nested_path=nested_child
            )
            if nested_child_conditions:
                current_conditions.append({'nested': {'path': nested_child, 'query': nested_child_conditions}})
        return bool_if_required(current_conditions)

    def list_documents(self, *args, **kwargs):
        """Build query filtering documents belonging to that bucket.
        Suppose the following configuration:

        Base                        <- filter on base
          |── Nested_A                 no filter on A (nested still must be applied for children)
          |     |── SubNested A1
          |     └── SubNested A2    <- filter on A2
          └── Nested_B              <- filter on B

        Note: on Filter and Filters query, if some filter condition is nested, it might require some propagation in
        children conditions on that same nested. This feature is not implemented yet. The main difficulty being to
        disambiguate if OR or AND operator must be applied on conditions of same nested level in some cases.
        """
        agg_tree = self._tree.agg_tree
        mapping_tree = agg_tree.tree_mapping

        aggs_keys = [
            (agg_tree[level], key) for
            level, key in self.bucket_properties().items()
        ]

        filters_per_nested_level = defaultdict(list)

        for level_agg, key in aggs_keys:
            level_agg_filter = level_agg.get_filter(key)
            # remove unnecessary match_all filters
            if level_agg_filter is not None and 'match_all' not in level_agg_filter:
                current_nested = agg_tree.applied_nested_path_at_node(level_agg.identifier)
                filters_per_nested_level[current_nested].append(level_agg_filter)

        nested_with_conditions = [n for n in filters_per_nested_level.keys() if n]

        all_nesteds = [
            n.identifier
            for n in mapping_tree.filter_nodes(
                lambda x: (x.type == 'nested') and any((i in x.identifier or '' for i in nested_with_conditions))
            )
        ]

        nid_to_children = defaultdict(set)
        for nested in all_nesteds:
            nested_with_parents = list(mapping_tree.rsearch(nid=nested, filter=lambda x: x.type == 'nested'))
            nearest_nested_parent = next(iter(nested_with_parents[1:]), None)
            nid_to_children[nearest_nested_parent].add(nested)
        return self._build_filter(nid_to_children, filters_per_nested_level)


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
