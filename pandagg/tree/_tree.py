#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree import Tree as OriginalTree

from pandagg.utils import DSLMixin


class Tree(DSLMixin, OriginalTree):

    KEY = None
    _type_name = None

    @classmethod
    def get_node_dsl_class(cls, name):
        return cls.node_class._get_dsl_class(name)

    def id_from_key(self, key):
        """
        Find node identifier based on key. If multiple nodes have the same key, takes the first one.

        Useful because of how pandagg implements lighttree.Tree.
        A bit of context:

        ElasticSearch allows queries to contain multiple similarly named clauses (for queries and aggregations).
        As a consequence clauses names are not used as clauses identifier in Trees, and internally pandagg (as lighttree
        ) uses auto-generated uuids to distinguish them.

        But for usability reasons, notably when declaring that an aggregation clause must be placed relatively to
        another one, the latter is identified by its name rather than its internal id. Since it is technically
        possible that multiple clauses share the same name (not recommended, but allowed), some pandagg features are
        ambiguous and not recommended in such context.
        """
        for k, n in self.list():
            if k == key:
                return n.identifier
        raise KeyError('No node found with key "%s"' % key)

    def __str__(self):
        return "<{class_}>\n{tree}".format(
            class_=str(self.__class__.__name__), tree=self.show(limit=40)
        )

    def __repr__(self):
        return self.__str__()
