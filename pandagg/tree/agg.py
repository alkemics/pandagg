#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import copy
import collections

from builtins import str as text
from elasticsearch import Elasticsearch
from six import iteritems, string_types, python_2_unicode_compatible, iterkeys

from pandagg.tree._tree import Tree
from pandagg.interactive.mapping import as_mapping
from pandagg.interactive.response import IResponse
from pandagg.node.agg.deserializer import deserialize_agg
from pandagg.node.agg.abstract import (
    BucketAggNode,
    UniqueBucketAgg,
    AggNode,
    MetricAgg,
    ShadowRoot,
)
from pandagg.node.agg.bucket import Terms, Nested, ReverseNested
from pandagg.node.agg.pipeline import BucketSelector, BucketSort
from pandagg.tree.query import Query
from pandagg.tree.response import ResponseTree


@python_2_unicode_compatible
class Agg(Tree):
    """Tree combination of aggregation nodes.

    Mapping declaration is optional, but doing so validates aggregation validity.
    """

    node_class = AggNode
    DEFAULT_OUTPUT = "dataframe"
    _crafted_root_name = "root"

    def __init__(
        self, from_=None, mapping=None, client=None, query=None, index_name=None,
    ):
        self.index_name = index_name
        self._query = Query(from_=query)
        self.tree_mapping = None
        if client is not None and not isinstance(client, Elasticsearch):
            raise ValueError("Unsupported client type <%s>" % type(client))
        self.client = client
        if mapping is not None:
            self.set_mapping(mapping)

        super(Agg, self).__init__()
        if from_ is not None:
            self._insert(from_)

    @classmethod
    def deserialize(cls, from_, mapping=None):
        if isinstance(from_, Agg):
            return from_
        if isinstance(from_, AggNode):
            new = Agg(mapping=mapping)
            new._insert_from_node(agg_node=from_)
            return new
        if isinstance(from_, dict):
            from_ = copy.deepcopy(from_)
            new = Agg(mapping=mapping)
            new._insert_from_dict(from_)
            return new
        else:
            raise ValueError("Unsupported type <%s>." % type(from_))

    def _insert_from_dict(self, from_dict, parent_id=None):
        if parent_id is None and len(from_dict.keys()) > 1:
            r = ShadowRoot()
            self.insert_node(r)
            parent_id = r.name
        for k, v in iteritems(from_dict):
            node = deserialize_agg({k: v})
            self._insert_from_node(node, parent_id=parent_id)

    def _insert_from_node(self, agg_node, parent_id=None):
        self.insert_node(agg_node, parent_id)
        if isinstance(agg_node, BucketAggNode):
            for child_agg_node in agg_node.aggs or []:
                if isinstance(child_agg_node, AggNode):
                    self._insert_from_node(
                        child_agg_node, parent_id=agg_node.identifier
                    )
                elif isinstance(child_agg_node, dict):
                    self._insert_from_dict(
                        child_agg_node, parent_id=agg_node.identifier
                    )
                else:
                    raise ValueError("Unknown node: got <%s>" % type(child_agg_node))

    def _insert(self, from_, pid=None):
        inserted_tree = self.deserialize(from_=from_, mapping=self.tree_mapping)
        if self.root is None or isinstance(
            inserted_tree.get(inserted_tree.root), ShadowRoot
        ):
            self.merge(nid=pid, new_tree=inserted_tree)
            return self
        self.insert(item=inserted_tree, parent_id=pid)
        return self

    def _clone_init(self, deep=False):
        if not deep:
            return Agg(
                mapping=self.tree_mapping,
                index_name=self.index_name,
                client=self.client,
                query=self._query,
            )
        return Agg(
            mapping=self.tree_mapping.clone(deep=deep)
            if self.tree_mapping is not None
            else None,
            index_name=self.index_name,
            client=copy.deepcopy(self.client),
            query=self._query.clone(deep=deep) if self._query is not None else None,
        )

    def bind(self, client, index_name=None):
        self.client = client
        if index_name is not None:
            self.index_name = index_name
        return self

    def set_mapping(self, mapping):
        self.tree_mapping = as_mapping(mapping)
        return self

    def _is_eligible_grouping_node(self, nid):
        node = self.get(nid)
        if not isinstance(node, BucketAggNode):
            return False
        # special aggregations not returning anything
        if isinstance(node, (BucketSelector, BucketSort)):
            return False
        return True

    @property
    def deepest_linear_bucket_agg(self):
        """Return deepest bucket aggregation node (pandagg.nodes.abstract.BucketAggNode) of that aggregation that
        neither has siblings, nor has an ancestor with siblings.
        """
        if not self.root or not self._is_eligible_grouping_node(self.root):
            return None
        last_bucket_agg_name = self.root
        children = [
            c
            for c in self.children(last_bucket_agg_name, id_only=False)
            if self._is_eligible_grouping_node(c.identifier)
        ]
        while len(children) == 1:
            last_agg = children[0]
            if not self._is_eligible_grouping_node(last_agg.identifier):
                break
            last_bucket_agg_name = last_agg.name
            children = [
                c
                for c in self.children(last_bucket_agg_name, id_only=False)
                if self._is_eligible_grouping_node(c.identifier)
            ]
        return last_bucket_agg_name

    def _validate_aggs_parent_id(self, pid):
        """If pid is not None, ensure that pid belongs to tree, and that it refers to a bucket aggregation.

        Else, if not provided, return deepest bucket aggregation if there is no ambiguity (linear aggregations).
        KO: non-ambiguous::
            A──> B──> C1
                 └──> C2
            raise error

        OK: non-ambiguous (linear)::

            A──> B──> C1
            return C1
        """
        if pid is not None:
            if not self._is_eligible_grouping_node(pid):
                raise ValueError("Node id <%s> is not a bucket aggregation." % pid)
            return pid
        leaves = self.leaves(id_only=False)
        # root
        # TODO
        if len(leaves) == 0:
            return None

        if len(leaves) > 1 or not isinstance(leaves[0], BucketAggNode):
            raise ValueError(
                "Declaration is ambiguous, you must declare the node id under which these "
                "aggregations should be placed."
            )
        return leaves[0].identifier

    def groupby(self, by, insert_below=None, insert_above=None, **kwargs):
        """Arrange passed aggregations in `by` arguments "vertically" (nested manner), above or below another agg
        clause.

        Given the initial aggregation::

            A──> B
            └──> C

        If `insert_below` = 'A'::

            A──> by──> B
                  └──> C

        If `insert_above` = 'B'::

            A──> by──> B
            └──> C

        `by` argument accepts single occurrence or sequence of following formats:

        * string (for terms agg concise declaration)
        * regular Elasticsearch dict syntax
        * AggNode instance (for instance Terms, Filters etc)

        If `insert_below` nor `insert_above` is provided by will be placed between the the deepest linear
        bucket aggregation if there is no ambiguity, and its children::

            A──> B      : OK generates     A──> B ─> C ─> by

            A──> B      : KO, ambiguous, must precise either A, B or C
            └──> C

        :param by: aggregation(s) clauses to insert "vertically"
        :param insert_below: parent aggregation id under which these aggregations should be placed
        :param insert_above: aggregation id above which these aggregations should be placed
        :param kwargs: agg body arguments when using "string" syntax for terms aggregation
        :rtype: pandagg.agg.Agg
        """
        if insert_below is not None and insert_above is not None:
            raise ValueError(
                'Must define at most one of "insert_above" and "insert_below", got both.'
            )

        new_agg = self.clone(with_tree=True)
        if insert_above is not None:
            parent = new_agg.parent(insert_above, id_only=False)
            # None if insert_above was root
            insert_below = parent.identifier if parent is not None else None
            insert_above_subtree = new_agg.drop_subtree(insert_above)
            if isinstance(by, (list, tuple)):
                for arg_el in by:
                    arg_el = Agg._deserialize_extended(arg_el, **kwargs)
                    new_agg._insert(arg_el, pid=insert_below)
                    insert_below = arg_el.deepest_linear_bucket_agg
            else:
                arg_el = Agg._deserialize_extended(by, **kwargs)
                new_agg._insert(arg_el, pid=insert_below)
                insert_below = arg_el.deepest_linear_bucket_agg
            new_agg.insert_tree(parent_id=insert_below, new_tree=insert_above_subtree)
            return new_agg

        insert_below = self._validate_aggs_parent_id(insert_below)

        # empty initial tree
        if insert_below is None:
            insert_below_subtrees = []
        else:
            insert_below_subtrees = [
                new_agg.drop_subtree(c.identifier)
                for c in new_agg.children(insert_below, id_only=False)
            ]

        if (
            isinstance(by, collections.Iterable)
            and not isinstance(by, string_types)
            and not isinstance(by, dict)
        ):
            for arg_el in by:
                arg_el = Agg._deserialize_extended(arg_el, **kwargs)
                new_agg._insert(arg_el, pid=insert_below)
                insert_below = arg_el.deepest_linear_bucket_agg
        else:
            arg_el = Agg._deserialize_extended(by, **kwargs)
            new_agg._insert(arg_el, pid=insert_below)
            insert_below = arg_el.deepest_linear_bucket_agg
        for st in insert_below_subtrees:
            new_agg.insert_tree(parent_id=insert_below, new_tree=st)
        return new_agg

    def agg(self, arg, insert_below=None, **kwargs):
        """Arrange passed aggregations in `arg` arguments "horizontally".

        Those will be placed under the `insert_below` aggregation clause id if provided, else under the deepest linear
        bucket aggregation if there is no ambiguity:

        OK::

            A──> B ─> C ─> arg

        KO::

            A──> B
            └──> C

        `arg` argument accepts single occurrence or sequence of following formats:

        * string (for terms agg concise declaration)
        * regular Elasticsearch dict syntax
        * AggNode instance (for instance Terms, Filters etc)


        :param arg: aggregation(s) clauses to insert "horizontally"
        :param insert_below: parent aggregation id under which these aggregations should be placed
        :param kwargs: agg body arguments when using "string" syntax for terms aggregation
        :rtype: pandagg.agg.Agg
        """
        insert_below = self._validate_aggs_parent_id(insert_below)
        new_agg = self.clone(with_tree=True)
        if isinstance(arg, (tuple, list)):
            if len(arg) > 1 and insert_below is None:
                root = ShadowRoot()
                new_agg.insert_node(root)
                insert_below = root.name
            for arg_el in arg:
                arg_el = Agg._deserialize_extended(arg_el, **kwargs)
                new_agg._insert(arg_el, pid=insert_below)
            return new_agg
        arg_el = Agg._deserialize_extended(arg, **kwargs)
        new_agg._insert(arg_el, pid=insert_below)
        return new_agg

    @classmethod
    def _deserialize_extended(cls, element, **kwargs):
        # deserialization accepting a string -> term aggregation
        if isinstance(element, string_types):
            node = Terms(name=element, field=element, size=kwargs.get("size"))
            return cls.deserialize(node)
        return cls.deserialize(element)

    def query_dict(self, from_=None, depth=None, with_name=True):
        if self.root is None:
            return {}
        from_ = self.root if from_ is None else from_
        node = self.get(from_)
        children_queries = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            for child_node in self.children(node.name, id_only=False):
                children_queries[child_node.name] = self.query_dict(
                    from_=child_node.name, depth=depth, with_name=False
                )
        if isinstance(node, ShadowRoot):
            node_query_dict = children_queries
        else:
            node_query_dict = node.query_dict()
            if children_queries:
                node_query_dict["aggs"] = children_queries
        if with_name:
            return {node.name: node_query_dict}
        return node_query_dict

    def applied_nested_path_at_node(self, nid):
        # from current node to root
        for id_ in [nid] + self.ancestors(nid):
            node = self.get(id_)
            if isinstance(node, (Nested, ReverseNested)):
                return node.path
        return None

    def _insert_node_below(self, node, parent_id):
        """If mapping is provided, nested aggregations are automatically applied.
        """
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if (
            isinstance(node, Nested)
            or isinstance(node, ReverseNested)
            or self.tree_mapping is None
            or parent_id is None
            or not hasattr(node, "field")
        ):
            return super(Agg, self)._insert_node_below(node, parent_id)

        self.tree_mapping.validate_agg_node(node)

        # from deepest to highest
        required_nested_level = self.tree_mapping.nested_at_field(node.field)
        current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Agg, self)._insert_node_below(node, parent_id)
        if current_nested_level and (
            required_nested_level or "" in current_nested_level
        ):
            # requires reverse-nested
            # check if already exists in direct children, else create it
            child_reverse_nested = next(
                (
                    n
                    for n in self.children(parent_id, id_only=False)
                    if isinstance(n, ReverseNested) and n.path == required_nested_level
                ),
                None,
            )
            if child_reverse_nested:
                return super(Agg, self)._insert_node_below(
                    node, child_reverse_nested.identifier
                )
            else:
                rv_node = ReverseNested(name="reverse_nested_below_%s" % parent_id)
                super(Agg, self).insert_node(rv_node, parent_id)
                return super(Agg, self)._insert_node_below(node, rv_node.identifier)

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(self.tree_mapping.list_nesteds_at_field(node.field)):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (
                        n
                        for n in (
                            self.children(parent_id, id_only=False)
                            if parent_id is not None
                            else []
                        )
                        if isinstance(n, Nested) and n.path == nested_lvl
                    ),
                    None,
                )
                if child_nested:
                    parent_id = child_nested.identifier
                    continue
                nested_node_name = (
                    "nested_below_root"
                    if parent_id is None
                    else "nested_below_%s" % parent_id
                )
                nested_node = Nested(name=nested_node_name, path=nested_lvl)
                super(Agg, self)._insert_node_below(nested_node, parent_id)
                parent_id = nested_node.identifier
        super(Agg, self)._insert_node_below(node, parent_id)

    def validate_tree(self, exc=False):
        """Validate tree definition against defined mapping.
        :param exc: if set to True, will raise exception if tree is invalid
        :return: boolean
        """
        if self.tree_mapping is None:
            return True
        for agg_node in self.list():
            # path for 'nested'/'reverse-nested', field for metric aggregations
            if not self.tree_mapping.validate_agg_node(agg_node, exc=exc):
                return False
        return True

    def _parse_group_by(
        self, response, row=None, agg_name=None, until=None, row_as_tuple=False
    ):
        """Recursive parsing of succession of unique child bucket aggregations.

        Yields each row for which last bucket aggregation generated buckets.
        """
        if not row:
            row = [] if row_as_tuple else {}
        agg_name = self.root if agg_name is None else agg_name
        if agg_name in response:
            agg_node = self.get(agg_name)
            for key, raw_bucket in agg_node.extract_buckets(response[agg_name]):
                child_name = next(
                    (child.name for child in self.children(agg_name, id_only=False)),
                    None,
                )
                sub_row = copy.deepcopy(row)
                # aggs generating a single bucket don't require to be listed in grouping keys
                if not isinstance(agg_node, UniqueBucketAgg):
                    if row_as_tuple:
                        sub_row.append(key)
                    else:
                        sub_row[agg_name] = key
                if child_name and agg_name != until:
                    # yield children
                    for sub_row, sub_raw_bucket in self._parse_group_by(
                        row=sub_row,
                        response=raw_bucket,
                        agg_name=child_name,
                        until=until,
                        row_as_tuple=row_as_tuple,
                    ):
                        yield sub_row, sub_raw_bucket
                else:
                    # end real yield
                    if row_as_tuple:
                        sub_row = tuple(sub_row)
                    yield sub_row, raw_bucket

    def _normalize_buckets(self, agg_response, agg_name=None):
        """Recursive function to parse aggregation response as a normalized entities.
        Each response bucket is represented as a dict with keys (key, level, value, children)::

            {
                "level": "owner.id",
                "key": 35,
                "value": 235,
                "children": [
                ]
            }
        """
        agg_name = agg_name or self.root
        agg_node = self.get(agg_name)
        agg_children = self.children(agg_node.name, id_only=False)
        for key, raw_bucket in agg_node.extract_buckets(agg_response[agg_name]):
            result = {
                "level": agg_name,
                "key": key,
                "value": agg_node.extract_bucket_value(raw_bucket),
            }
            normalized_children = [
                normalized_child
                for child in agg_children
                for normalized_child in self._normalize_buckets(
                    agg_name=child.name, agg_response=raw_bucket
                )
            ]
            if normalized_children:
                result["children"] = normalized_children
            yield result

    def serialize_response_as_tabular(
        self, aggs, row_as_tuple=False, grouped_by=None, normalize_children=True,
    ):
        """Build tabular view of ES response grouping levels (rows) until 'grouped_by' aggregation node included is
        reached, and using children aggregations of grouping level as values for each of generated groups (columns).

        Suppose an aggregation of this shape (A & B bucket aggregations)::

            A──> B──> C1
                 ├──> C2
                 └──> C3

        With grouped_by='B', breakdown ElasticSearch response (tree structure), into a tabular structure of this shape::

                                  C1     C2    C3
            A           B
            wood        blue      10     4     0
                        red       7      5     2
            steel       blue      1      9     0
                        red       23     4     2

        :param aggs: ElasticSearch response
        :param row_as_tuple: if True, level-key samples are returned as tuples, else in a dictionnary
        :param grouped_by: name of the aggregation node used as last grouping level
        :param normalize_children: if True, normalize columns buckets
        :return: index, index_names, values
        """
        grouped_by = (
            self.deepest_linear_bucket_agg if grouped_by is None else grouped_by
        )
        if grouped_by not in self:
            raise ValueError(
                "Cannot group by <%s>, agg node does not exist" % grouped_by
            )

        index_values = self._parse_group_by(
            response=aggs, row_as_tuple=row_as_tuple, until=grouped_by
        )
        index_values_l = list(index_values)
        if not index_values_l:
            return [], [], []
        index, values = zip(*index_values_l)

        grouping_agg = self.get(grouped_by)
        grouping_agg_children = self.children(grouped_by, id_only=False)

        index_names = [
            a.name
            for a in self.ancestors(grouping_agg.name, id_only=False, from_root=True)
            + [grouping_agg]
            if not isinstance(a, UniqueBucketAgg)
        ]

        def serialize_columns(row_data):
            # extract value (usually 'doc_count') of grouping agg node
            result = {
                grouping_agg.VALUE_ATTRS[0]: grouping_agg.extract_bucket_value(row_data)
            }
            if not grouping_agg_children:
                return result
            # extract values of children, one columns per child
            for child in grouping_agg_children:
                if not normalize_children:
                    result[child.name] = row_data[child.name]
                    continue
                if isinstance(child, (UniqueBucketAgg, MetricAgg)):
                    result[child.name] = child.extract_bucket_value(
                        row_data[child.name]
                    )
                else:
                    result[child.name] = next(
                        self._normalize_buckets(row_data, child.name), None
                    )
            return result

        serialized_values = list(map(serialize_columns, values))
        return index, index_names, serialized_values

    def serialize_response_as_dataframe(
        self, aggs, grouped_by=None, normalize_children=True
    ):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'Using dataframe output format requires to install pandas. Please install "pandas" or '
                "use another output format."
            )
        index, index_names, values = self.serialize_response_as_tabular(
            aggs=aggs,
            row_as_tuple=True,
            grouped_by=grouped_by,
            normalize_children=normalize_children,
        )
        if not index:
            return pd.DataFrame()
        if len(index[0]) == 0:
            index = (None,) * len(index)
        else:
            index = pd.MultiIndex.from_tuples(index, names=index_names)
        return pd.DataFrame(index=index, data=values)

    def serialize_response_as_normalized(self, aggs):
        children = []
        for k in sorted(iterkeys(aggs)):
            for child in self._normalize_buckets(aggs, k):
                children.append(child)
        return {"level": "root", "key": None, "value": None, "children": children}

    def serialize_response_as_tree(self, aggs):
        response_tree = ResponseTree(self).parse_aggregation(aggs)
        return IResponse(
            tree=response_tree,
            depth=1,
            client=self.client,
            index_name=self.index_name,
            query=self._query,
        )

    def serialize_response(self, aggs, output, **kwargs):
        """
        :param aggs: aggregation response, dict
        :param output: output format, one of "raw", "tree", "normalized_tree", "dict_rows", "dataframe"
        :param kwargs: serialization kwargs
        :return:
        """
        if output == "raw":
            return aggs
        elif output == "tree":
            return self.serialize_response_as_tree(aggs)
        elif output == "normalized_tree":
            return self.serialize_response_as_normalized(aggs)
        elif output == "dict_rows":
            return self.serialize_response_as_tabular(aggs, **kwargs)
        elif output == "dataframe":
            return self.serialize_response_as_dataframe(aggs, **kwargs)
        else:
            raise NotImplementedError("Unkown %s output format." % output)

    def query(self, query, validate=False, **kwargs):
        new_query = self._query.query(query, **kwargs)
        query_dict = new_query.query_dict()
        if validate:
            validity = self.client.indices.validate_query(
                index=self.index_name, body={"query": query_dict}
            )
            if not validity["valid"]:
                raise ValueError("Wrong query: %s\n%s" % (query, validity))
        new_agg = self.clone(with_tree=True)
        new_agg._query = new_query
        return new_agg

    def _execute(self, aggregation, index=None):
        body = {"aggs": aggregation, "size": 0}
        query = self._query.query_dict()
        if query:
            body["query"] = query
        return self.client.search(index=index, body=body)

    def execute(self, index=None, output=DEFAULT_OUTPUT, **kwargs):
        if self.client is None:
            raise ValueError('Execution requires to specify "client" at __init__.')
        es_response = self._execute(
            aggregation=self.query_dict(), index=index or self.index_name
        )
        return self.serialize_response(
            aggs=es_response["aggregations"], output=output, **kwargs
        )

    def __str__(self):
        return "<Aggregation>\n%s" % text(self.show())
