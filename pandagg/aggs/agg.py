#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import copy
import collections
import warnings

from six import iteritems, string_types, python_2_unicode_compatible, iterkeys
from builtins import str as text

from elasticsearch import Elasticsearch

from pandagg.buckets.response import ResponseTree, Response, ClientBoundResponse
from pandagg.exceptions import AbsentMappingFieldError, InvalidAggregation, MappingError
from pandagg.mapping import MappingTree, Mapping
from pandagg.nodes import PUBLIC_AGGS, Terms, Nested, ReverseNested, MatchAll
from pandagg.nodes.abstract import BucketAggNode, UniqueBucketAgg, AggNode
from pandagg.tree import Tree
from pandagg.utils import bool_if_required


@python_2_unicode_compatible
class Agg(Tree):
    """Tree combination of aggregation nodes.

    Mapping declaration is optional, but doing so validates aggregation validity.
    """

    node_class = AggNode
    tree_mapping = None
    DEFAULT_OUTPUT = 'dataframe'
    _crafted_root_name = 'root'

    def __init__(self, from_=None, mapping=None, identifier=None):
        from_tree = None
        from_agg_node = None
        from_dict = None
        if isinstance(from_, Agg):
            from_tree = from_
        if isinstance(from_, AggNode):
            from_agg_node = from_
        if isinstance(from_, dict):
            from_dict = from_
        super(Agg, self).__init__(tree=from_tree, identifier=identifier)
        self.set_mapping(mapping)
        if from_dict:
            self._init_build_tree_from_dict(from_dict)
        if from_agg_node:
            self._build_tree_from_node(from_agg_node)

    def _clone(self, identifier=None, with_tree=False, deep=False):
        return Agg(
            mapping=self.tree_mapping,
            identifier=identifier,
            from_=self if with_tree else None
        )

    def set_mapping(self, mapping):
        if mapping is not None:
            if isinstance(mapping, MappingTree):
                self.tree_mapping = mapping
            elif isinstance(mapping, Mapping):
                self.tree_mapping = mapping._tree
            elif isinstance(mapping, dict):
                self.tree_mapping = MappingTree(mapping)
            else:
                raise NotImplementedError()
        return self

    def _init_build_tree_from_dict(self, from_dict):
        assert isinstance(from_dict, dict)
        from_dict = copy.deepcopy(from_dict)
        if len(from_dict.keys()) > 1:
            self.add_node(MatchAll(self._crafted_root_name))
        agg_name, agg_detail = next(iteritems(from_dict))
        self._build_tree_from_dict(agg_name, agg_detail, self.root)

    def _build_tree_from_dict(self, agg_name, agg_detail, pid=None):
        if not isinstance(agg_detail, dict):
            raise InvalidAggregation
        meta = agg_detail.pop('meta', None)
        children_aggs = agg_detail.pop('aggs', None) or agg_detail.pop('aggregations', None) or {}
        assert len(agg_detail.keys()) == 1
        agg_type, agg_body = next(iteritems(agg_detail))
        node = self._node_from_dict(agg_type=agg_type, agg_name=agg_name, agg_body=agg_body, meta=meta)
        self.add_node(node, pid)
        for child_name in sorted(children_aggs.keys()):
            self._build_tree_from_dict(child_name, children_aggs[child_name], node.identifier)

    @staticmethod
    def _node_from_dict(agg_type, agg_name, agg_body, meta):
        if agg_type not in PUBLIC_AGGS.keys():
            raise NotImplementedError('Unknown aggregation type <%s>' % agg_type)
        agg_class = PUBLIC_AGGS[agg_type]
        return agg_class.deserialize(name=agg_name, meta=meta, **agg_body)

    def _build_tree_from_node(self, agg_node, pid=None):
        self.add_node(agg_node, pid)
        if isinstance(agg_node, BucketAggNode):
            for child_agg_node in agg_node.aggs or []:
                self._build_tree_from_node(child_agg_node, pid=agg_node.identifier)
            # reset children to None to avoid confusion since this serves only __init__ syntax.
            agg_node.aggs = None

    @property
    def deepest_linear_bucket_agg(self):
        """Return deepest bucket aggregation node (pandagg.nodes.abstract.BucketAggNode) of that aggregation that
        neither has siblings, nor has an ancestor with siblings.
        """
        if not self.root or not isinstance(self[self.root], BucketAggNode):
            return None
        last_bucket_agg_name = self.root
        children = self.children(last_bucket_agg_name)
        while len(children) == 1:
            last_agg = children[0]
            if not isinstance(last_agg, BucketAggNode):
                break
            last_bucket_agg_name = last_agg.name
            children = self.children(last_bucket_agg_name)
        return last_bucket_agg_name

    def _validate_aggs_parent_id(self, pid):
        """If pid is not None, ensure that pid belongs to tree, and that it refers to a bucket aggregation.

        Else, if not provided, return deepest bucket aggregation if there is no ambiguity (linear aggregations).
        KO: non-ambiguous:
        A──> B──> C1
             └──> C2
        raise error

        OK: non-ambiguous (linear):
        A──> B──> C1
        return C1
        """
        if pid is not None:
            if pid not in self:
                raise ValueError('Node id <%s> is not present in aggregation.' % pid)
            if not isinstance(self[pid], BucketAggNode):
                raise ValueError('Node id <%s> is not a bucket aggregation.' % pid)
            return pid
        paths = self.paths_to_leaves()
        # root
        if len(paths) == 0:
            return None

        if len(paths) > 1 or not isinstance(self[paths[0][-1]], BucketAggNode):
            raise ValueError('Declaration is ambiguous, you must declare the node id under which these '
                             'aggregations should be placed.')
        return paths[0][-1]

    def groupby(self, by, insert_below=None, **kwargs):
        """Group by is available only if there is a succession of unique childs.

        Accepts single occurence or sequence of following formats:
        - string
        - dict ES aggregation
        - dict pandas like aggregation
        - Aggregation object instance

        String:
        Interpreted as a terms aggregation counting number of documents per value occurence:
        >>> a = Agg().groupby(['owner.type', 'owner.id'])
        >>> a
        <Aggregation>
        owner.type
        └── owner.id
        >>> a.agg_dict()
        {
            "owner.type": {
                "aggs": {
                    "owner.id": {
                        "terms": {
                            "field": "owner.id",
                            "size": 20
                        }
                    }
                },
                "terms": {
                    "field": "owner.type",
                    "size": 20
                }
            }
        }

        :param by: aggregation(s) used to group results
        :param insert_below: parent node id under which these nodes should be declared
        :param kwargs: arguments to customize dict aggregation parsing TODO - detail this part
        :rtype: pandagg.aggs.agg.Agg
        """
        insert_below = self._validate_aggs_parent_id(insert_below)
        new_agg = self._clone(with_tree=True)

        if isinstance(by, collections.Iterable) and not isinstance(by, string_types) and not isinstance(by, dict):
            for arg_el in by:
                new_agg = new_agg._interpret_agg(insert_below, arg_el, **kwargs)
                insert_below = new_agg.deepest_linear_bucket_agg
        else:
            new_agg = new_agg._interpret_agg(insert_below, by, **kwargs)
        return new_agg

    def agg(self, arg, insert_below=None, **kwargs):
        """Horizontally adds aggregation on top of succession of unique children.

        Suppose pre-existing aggregations:
        OK: A──> B ─> C ─> NEW_AGGS

        KO: A──> B
            └──> C

        Accepts single occurence or sequence of following formats:
        - string
        - dict ES aggregation
        - dict pandas like aggregation
        - Aggregation object instance

        String:
        Interpreted as a terms aggregation counting number of documents per value occurence:
        >>> a = Agg().groupby(['owner.type', 'owner.id'])
        >>> a
        <Aggregation>
        owner.type
        └── owner.id
        >>> a.agg(['validation.status', 'retailerStatus'])
        <Aggregation>
        owner.type
        └── owner.id
            ├── retailerStatus
            └── validation.status
        :param insert_below: parent node id under which these nodes should be declared
        :rtype: pandagg.aggs.agg.Agg
        """
        insert_below = self._validate_aggs_parent_id(insert_below)
        new_agg = self._clone(with_tree=True)
        if isinstance(arg, collections.Iterable) and not isinstance(arg, string_types) and not isinstance(arg, dict):
            if len(arg) > 1 and insert_below is None:
                root = MatchAll(self._crafted_root_name)
                new_agg.add_node(root)
                insert_below = root.identifier
            for arg_el in arg:
                new_agg = new_agg._interpret_agg(insert_below, arg_el, **kwargs)
            return new_agg
        return new_agg._interpret_agg(insert_below, arg, **kwargs)

    def _interpret_agg(self, insert_below, element, **kwargs):
        if isinstance(element, string_types):
            node = Terms(name=element, field=element, size=kwargs.get('size'))
            self.add_node(node, pid=insert_below)
            return self
        if isinstance(element, dict):
            try:
                new_agg = self._clone(from_=element)
                if self.root is None:
                    return new_agg
                self.paste(nid=insert_below, new_tree=new_agg)
            except AbsentMappingFieldError:
                pass
            return self
        if isinstance(element, AggNode):
            assert element.AGG_TYPE in PUBLIC_AGGS.keys()
            self._build_tree_from_node(element, pid=insert_below)
            return self
        if isinstance(element, Agg) or isinstance(element, ClientBoundAgg):
            self.paste(nid=insert_below, new_tree=element)
            return self
        raise NotImplementedError('Unkown element of type <%s>' % type(element))

    def query_dict(self, from_=None, depth=None, with_name=True):
        if self.root is None:
            return {}
        from_ = self.root if from_ is None else from_
        node = self[from_]
        children_queries = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            for child_node in self.children(node.name):
                children_queries[child_node.name] = self.query_dict(
                    from_=child_node.name, depth=depth, with_name=False)
        node_query_dict = node.query_dict()
        if children_queries:
            node_query_dict['aggs'] = children_queries
        if with_name:
            return {node.name: node_query_dict}
        return node_query_dict

    def applied_nested_path_at_node(self, nid):
        applied_nested_path = None
        # travel parent nodes from root to required node
        for nid in reversed(list(self.rsearch(nid))):
            node = self[nid]
            if isinstance(node, Nested):
                applied_nested_path = node.path
            elif isinstance(node, ReverseNested):
                # a reverse nested removes nested, except if one path is specified
                applied_nested_path = node.path
        return applied_nested_path

    def paste(self, nid, new_tree, deep=False):
        """Pastes a tree handling nested implications if mapping is provided.
        The provided tree should be validated beforehands.
        """
        if self.tree_mapping is None:
            return super(Agg, self).paste(nid, new_tree, deep)
        # validates that mappings are similar
        if new_tree.tree_mapping is not None:
            if new_tree.tree_mapping.mapping_detail != self.tree_mapping.mapping_detail:
                raise MappingError('Pasted tree has a different mapping.')

        # check root node nested position in mapping
        pasted_root = new_tree[new_tree.root]
        # if it is a nested or reverse-nested agg, assumes you know what you are doing, validates afterwards
        if isinstance(pasted_root, Nested) or isinstance(pasted_root, ReverseNested):
            super(Agg, self).paste(nid, new_tree, deep)
            return self.validate_tree(exc=True)

        if not hasattr(pasted_root, 'field'):
            warnings.warn('Paste operation could not validate nested integrity: unknown nested position of pasted root'
                          'node: %s.' % pasted_root)
            super(Agg, self).paste(nid, new_tree, deep)
            return self.validate_tree(exc=True)

        self.tree_mapping.validate_agg_node(pasted_root)
        # from deepest to highest
        required_nested_level = self.tree_mapping.nested_at_field(pasted_root.field)
        current_nested_level = self.applied_nested_path_at_node(nid)
        if current_nested_level == required_nested_level:
            return super(Agg, self).paste(nid, new_tree, deep)
        if current_nested_level and (required_nested_level or '' in current_nested_level):
            # check if already exists in direct children, else create it
            child_reverse_nested = next(
                (n for n in self.children(nid) if isinstance(n, ReverseNested) and n.path == required_nested_level),
                None
            )
            if child_reverse_nested:
                return super(Agg, self).paste(child_reverse_nested.identifier, new_tree, deep)
            else:
                rv_node = ReverseNested(name='reverse_nested_below_%s' % nid)
                super(Agg, self).add_node(rv_node, nid)
                return super(Agg, self).paste(rv_node.identifier, new_tree, deep)

        # requires nested - apply all required nested fields
        pid = nid
        for nested_lvl in reversed(self.tree_mapping.list_nesteds_at_field(pasted_root.field)):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (n for n in self.children(nid) if isinstance(n, Nested) and n.path == nested_lvl),
                    None
                )
                if child_nested:
                    pid = child_nested.identifier
                    continue
                nested_node = Nested(name='nested_below_%s' % pid, path=nested_lvl)
                super(Agg, self).add_node(nested_node, pid)
                pid = nested_node.identifier
        super(Agg, self).paste(pid, new_tree, deep)

    def add_node(self, node, pid=None):
        """If mapping is provided, nested and outnested are automatically applied.
        """
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if isinstance(node, Nested) or isinstance(node, ReverseNested) or self.tree_mapping is None:
            super(Agg, self).add_node(node, pid)
            return self.validate_tree(exc=True)
        if not hasattr(node, 'field'):
            super(Agg, self).add_node(node, pid)
            return self.validate_tree(exc=True)

        self.tree_mapping.validate_agg_node(node)

        # from deepest to highest
        required_nested_level = self.tree_mapping.nested_at_field(node.field)
        current_nested_level = self.applied_nested_path_at_node(pid)
        if current_nested_level == required_nested_level:
            return super(Agg, self).add_node(node, pid)
        if current_nested_level and (required_nested_level or '' in current_nested_level):
            # requires reverse-nested
            # check if already exists in direct children, else create it
            child_reverse_nested = next(
                (n for n in self.children(pid) if isinstance(n, ReverseNested) and n.path == required_nested_level),
                None
            )
            if child_reverse_nested:
                return super(Agg, self).add_node(node, child_reverse_nested.identifier)
            else:
                rv_node = ReverseNested(name='reverse_nested_below_%s' % pid)
                super(Agg, self).add_node(rv_node, pid)
                return super(Agg, self).add_node(node, rv_node.identifier)

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(self.tree_mapping.list_nesteds_at_field(node.field)):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (n for n in (self.children(pid) if pid is not None else [])
                     if isinstance(n, Nested) and n.path == nested_lvl),
                    None
                )
                if child_nested:
                    pid = child_nested.identifier
                    continue
                nested_node = Nested(name='nested_below_%s' % pid, path=nested_lvl)
                super(Agg, self).add_node(nested_node, pid)
                pid = nested_node.identifier
        super(Agg, self).add_node(node, pid)

    def validate_tree(self, exc=False):
        """Validate tree definition against defined mapping.
        :param exc: if set to True, will raise exception if tree is invalid
        :return: boolean
        """
        if self.tree_mapping is None:
            return True
        for agg_node in self.nodes.values():
            # path for 'nested'/'reverse-nested', field for metric aggregations
            valid = self.tree_mapping.validate_agg_node(agg_node, exc=exc)
            if not valid:
                return False
        return True

    def _parse_group_by(self,
                        response, row=None, agg_name=None, until=None, row_as_tuple=False):
        """Recursive parsing of succession of unique child bucket aggregations.

        Yields each row for which last bucket aggregation generated buckets.
        """
        if not row:
            row = [] if row_as_tuple else {}
        agg_name = self.root if agg_name is None else agg_name
        if agg_name in response:
            agg_node = self[agg_name]
            for key, raw_bucket in agg_node.extract_buckets(response[agg_name]):
                child_name = next((child.name for child in self.children(agg_name)), None)
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
                            row_as_tuple=row_as_tuple
                    ):
                        yield sub_row, sub_raw_bucket
                else:
                    # end real yield
                    if row_as_tuple:
                        sub_row = tuple(sub_row)
                    yield sub_row, raw_bucket

    def _normalize_buckets(self, agg_response, agg_name=None):
        """Recursive function to parse aggregation response as a normalized entities.
        Each response bucket is represented as a dict with keys (key, level, value, children):
        {
            "level": "owner.id",
            "key": 35,
            "value": 235,
            "children": [
            ]
        }
        """
        agg_name = agg_name or self.root
        agg_node = self[agg_name]
        agg_children = self.children(agg_node.name)
        for key, raw_bucket in agg_node.extract_buckets(agg_response[agg_name]):
            result = {
                "level": agg_name,
                "key": key,
                "value": agg_node.extract_bucket_value(raw_bucket)
            }
            normalized_children = [
                normalized_child
                for child in agg_children
                for normalized_child in self._normalize_buckets(
                    agg_name=child.name,
                    agg_response=raw_bucket
                )
            ]
            if normalized_children:
                result['children'] = normalized_children
            yield result

    def _serialize_as_tabular(self, aggs_response, row_as_tuple=False, grouped_by=None, normalize_children=True):
        """Build tabular view of ES response grouping levels (rows) until 'grouped_by' aggregation node included is
        reached, and using children aggregations of grouping level as values for each of generated groups (columns).

        Suppose an aggregation of this shape (A & B bucket aggregations)
        A──> B──> C1
             ├──> C2
             └──> C3

        With grouped_by='B', breakdown ElasticSearch response (tree structure), into a tabular structure of this shape:
                              C1     C2    C3
        A           B
        wood        blue      10     4     0
                    red       7      5     2
        steel       blue      1      9     0
                    red       23     4     2

        :param aggs_response: ElasticSearch response
        :param row_as_tuple: if True, level-key samples are returned as tuples, else in a dictionnary
        :param grouped_by: name of the aggregation node used as last grouping level
        :param normalize_children: if True, normalize columns buckets
        :return: index, index_names, values
        """
        grouped_by = self.deepest_linear_bucket_agg if grouped_by is None else grouped_by
        if grouped_by not in self:
            raise ValueError('Cannot group by <%s>, agg node does not exist' % grouped_by)

        index_values = self._parse_group_by(
            response=aggs_response,
            row_as_tuple=row_as_tuple,
            until=grouped_by
        )
        index_values_l = list(index_values)
        if not index_values_l:
            return [], [], []
        index, values = zip(*index_values_l)

        grouping_agg = self[grouped_by]
        grouping_agg_children = self.children(grouped_by)

        index_names = list(reversed(list(self.rsearch(
            grouping_agg.name, filter=lambda x: not isinstance(x, UniqueBucketAgg)
        ))))

        def serialize_columns(row_data):
            # extract value (usually 'doc_count') of grouping agg node
            result = {grouping_agg.VALUE_ATTRS[0]: grouping_agg.extract_bucket_value(row_data)}
            if not grouping_agg_children:
                return result
            # extract values of children, one columns per child
            for child in grouping_agg_children:
                if not normalize_children:
                    result[child.name] = row_data[child.name]
                    continue
                if child.SINGLE_BUCKET:
                    result[child.name] = child.extract_bucket_value(row_data[child.name])
                else:
                    result[child.name] = next(self._normalize_buckets(row_data, child.name), None)
            return result
        serialized_values = list(map(serialize_columns, values))
        return index, index_names, serialized_values

    def _serialize_as_dataframe(self, aggs, grouped_by=None, normalize_children=True):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('Using dataframe output format requires to install pandas. Please install "pandas" or '
                              'use another output format.')
        index, index_names, values = self._serialize_as_tabular(
            aggs_response=aggs,
            row_as_tuple=True,
            grouped_by=grouped_by,
            normalize_children=normalize_children
        )
        if not index:
            return pd.DataFrame()
        if len(index[0]) == 0:
            index = (None,) * len(index)
        else:
            index = pd.MultiIndex.from_tuples(index, names=index_names)
        return pd.DataFrame(index=index, data=values)

    def _serialize_as_normalized(self, aggs):
        children = []
        for k in sorted(iterkeys(aggs)):
            for child in self._normalize_buckets(aggs, k):
                children.append(child)
        return {
            'level': 'root',
            'key': None,
            'value': None,
            'children': children
        }

    def _serialize_as_tree(self, aggs):
        response_tree = ResponseTree(self).parse_aggregation(aggs)
        return Response(tree=response_tree, depth=1)

    def serialize(self, aggs, output, **kwargs):
        if output == 'raw':
            return aggs
        elif output == 'tree':
            return self._serialize_as_tree(aggs)
        elif output == 'normalized_tree':
            return self._serialize_as_normalized(aggs)
        elif output == 'dict_rows':
            return self._serialize_as_tabular(aggs, **kwargs)
        elif output == 'dataframe':
            return self._serialize_as_dataframe(aggs, **kwargs)
        else:
            raise NotImplementedError('Unkown %s output format.' % output)

    def __str__(self):
        self.show()
        return '<Aggregation>\n%s' % text(self._reader)


class ClientBoundAgg(Agg):

    def __init__(self, client, index_name, mapping=None, from_=None, query=None, identifier=None):
        self.client = client
        self.index_name = index_name
        if client is not None:
            assert isinstance(client, Elasticsearch)
        self._query = query
        super(ClientBoundAgg, self).__init__(
            from_=from_,
            mapping=mapping,
            identifier=identifier
        )

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
