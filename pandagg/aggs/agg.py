#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import collections
import warnings

from pandagg.exceptions import (
    AbsentMappingFieldError, InvalidOperationMappingFieldError, InvalidAggregation, MappingError
)
from pandagg.tree import Tree
from pandagg.utils import validate_client
from pandagg.aggs.agg_nodes import (
    AggNode, PUBLIC_AGGS, Terms, Nested, ReverseNested, MatchAll, BucketAggNode, UniqueBucketAgg
)
from pandagg.mapping.mapping import Mapping, MappingTree
from pandagg.aggs.response_tree import ResponseTree, AggResponse


class Agg(Tree):
    """Tree combination of aggregation nodes.

    Mapping declaration is optional, but doing so validates aggregation validity.
    """

    node_class = AggNode
    tree_mapping = None
    DEFAULT_OUTPUT = 'dataframe'

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

    def _get_instance(self, identifier=None):
        return Agg(mapping=self.tree_mapping, identifier=identifier)

    def copy(self, identifier=None):
        return Agg(mapping=self.tree_mapping, from_=self, identifier=identifier)

    def set_mapping(self, mapping):
        if mapping is not None:
            if isinstance(mapping, MappingTree):
                self.tree_mapping = mapping
            elif isinstance(mapping, Mapping):
                self.tree_mapping = mapping._tree
            elif isinstance(mapping, dict):
                mapping_name, mapping_detail = next(mapping.iteritems())
                self.tree_mapping = MappingTree(mapping_name, mapping_detail)
            else:
                raise NotImplementedError()

    def _init_build_tree_from_dict(self, from_dict):
        assert isinstance(from_dict, dict)
        from_dict = copy.deepcopy(from_dict)
        if len(from_dict.keys()) > 1:
            self.add_node(MatchAll('root'))
        agg_name, agg_detail = next(from_dict.iteritems())
        self._build_tree_from_dict(agg_name, agg_detail, self.root)

    def _build_tree_from_dict(self, agg_name, agg_detail, pid=None):
        if not isinstance(agg_detail, dict):
            raise InvalidAggregation
        meta = agg_detail.pop('meta', None)
        children_aggs = agg_detail.pop('aggs', None) or agg_detail.pop('aggregations', None) or {}
        assert len(agg_detail.keys()) == 1
        agg_type = agg_detail.keys()[0]
        agg_body = agg_detail.values()[0]
        node = self._node_from_dict(agg_type=agg_type, agg_name=agg_name, agg_body=agg_body, meta=meta)
        self.add_node(node, pid)
        for child_name, child_detail in children_aggs.iteritems():
            self._build_tree_from_dict(child_name, child_detail, node.identifier)

    def _node_from_dict(self, agg_type, agg_name, agg_body, meta):
        if agg_type not in PUBLIC_AGGS.keys():
            raise NotImplementedError('Unknown aggregation type <%s>' % agg_type)
        agg_class = PUBLIC_AGGS[agg_type]
        kwargs = agg_class.agg_body_to_init_kwargs(agg_body)
        return agg_class(agg_name=agg_name, meta=meta, **kwargs)

    def _build_tree_from_node(self, agg_node, pid=None):
        self.add_node(agg_node, pid)
        if isinstance(agg_node, BucketAggNode):
            for child_agg_node in agg_node.aggs or []:
                self._build_tree_from_node(child_agg_node, pid=agg_node.identifier)
            # reset children to None to avoid confusion since this serves only __init__ syntax.
            agg_node.aggs = None

    def groupby(self, by, **kwargs):
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

        # TODO - allow AggregationNodes objects, and regular ES dict aggs

        :param by:
        :param kwargs:
        :return:
        """
        new_agg = self.copy()
        paths = new_agg.paths_to_leaves()
        assert len(paths) <= 1
        if paths:
            sub_aggs_parent_id = paths[0][-1]
        else:
            sub_aggs_parent_id = None

        if isinstance(by, collections.Iterable) and not isinstance(by, basestring) and not isinstance(by, dict):
            for arg_el in by:
                new_agg._interpret_agg(sub_aggs_parent_id, arg_el, **kwargs)
                sub_aggs_parent_id = new_agg.deepest_linear_bucket_agg
        else:
            new_agg._interpret_agg(sub_aggs_parent_id, by, **kwargs)
        return new_agg

    def agg(self, arg=None, **kwargs):
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
        """
        if arg is None:
            if not self.root:
                raise ValueError('Empty aggregation')
            return self
        new_agg = self.copy()
        if not new_agg.root:
            new_agg.add_node(MatchAll('root'))
            sub_aggs_parent_id = new_agg.root
        else:
            paths = new_agg.paths_to_leaves()
            assert len(paths) == 1
            sub_aggs_parent_id = paths[0][-1]

        if isinstance(arg, collections.Iterable) and not isinstance(arg, basestring) and not isinstance(arg, dict):
            for arg_el in arg:
                new_agg._interpret_agg(sub_aggs_parent_id, arg_el, **kwargs)
        else:
            new_agg._interpret_agg(sub_aggs_parent_id, arg, **kwargs)
        return new_agg

    def _interpret_agg(self, insert_below, element, **kwargs):
        if isinstance(element, basestring):
            node = Terms(agg_name=element, field=element, size=kwargs.get('default_size', Terms.DEFAULT_SIZE))
            self.add_node(node, pid=insert_below)
            return self
        if isinstance(element, dict):
            try:
                self.paste(nid=insert_below, new_tree=Agg(from_=element))
            except AbsentMappingFieldError:
                pass
            return self
        if isinstance(element, AggNode):
            assert element.AGG_TYPE in PUBLIC_AGGS.keys()
            self._build_tree_from_node(element, pid=insert_below)
            return self
        if isinstance(element, Agg):
            self.paste(nid=insert_below, new_tree=element)
            return self
        raise NotImplementedError()

    def agg_dict(self, from_=None, depth=None):
        from_ = self.root if from_ is None else from_
        root_agg = self[from_]
        return root_agg.agg_dict(tree=self, depth=depth)

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
            return self.validate(exc=True)

        agg_field = new_tree[new_tree.root].agg_body.get('field')
        if agg_field is None:
            warnings.warn('Paste operation could not validate nested integrity: unknown nested position of pasted root'
                          'node: %s.' % pasted_root)
            super(Agg, self).paste(nid, new_tree, deep)
            return self.validate(exc=True)

        if agg_field not in self.tree_mapping:
            raise AbsentMappingFieldError('Agg of type <%s> on non-existing field <%s>.'
                                          % (pasted_root.AGG_TYPE, agg_field))

        # from deepest to highest
        mapping_nested_fields = list(self.tree_mapping.rsearch(agg_field, filter=lambda n: n.type == 'nested'))
        required_nested_level = next(iter(mapping_nested_fields), None)
        current_nested_level = self.applied_nested_path_at_node(nid)
        if current_nested_level == required_nested_level:
            return super(Agg, self).paste(nid, new_tree, deep)
        if current_nested_level and (required_nested_level or '' in current_nested_level):
            # requires reverse-nested
            # check if already exists in direct children, else create it
            child_reverse_nested = next(
                (n for n in self.children(nid) if isinstance(n, ReverseNested) and n.path == required_nested_level),
                None
            )
            if child_reverse_nested:
                return super(Agg, self).paste(child_reverse_nested.identifier, new_tree, deep)
            else:
                rv_node = ReverseNested(agg_name='reverse_nested_below_%s' % nid)
                super(Agg, self).add_node(rv_node, nid)
                return super(Agg, self).paste(rv_node.identifier, new_tree, deep)

        # requires nested - apply all required nested fields
        pid = nid
        for nested_lvl in reversed(mapping_nested_fields):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (n for n in self.children(nid) if isinstance(n, Nested) and n.path == nested_lvl),
                    None
                )
                if child_nested:
                    pid = child_nested.identifier
                    continue
                nested_node = Nested(agg_name='nested_below_%s' % pid, path=nested_lvl)
                super(Agg, self).add_node(nested_node, pid)
                pid = nested_node.identifier
        super(Agg, self).paste(pid, new_tree, deep)

    def add_node(self, node, pid=None):
        """If mapping is provided, nested and outnested are automatically applied.
        """
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if isinstance(node, Nested) or isinstance(node, ReverseNested):
            super(Agg, self).add_node(node, pid)
            return self.validate(exc=True)

        agg_field = node.agg_body.get('field')
        if agg_field is None or self.tree_mapping is None:
            return super(Agg, self).add_node(node, pid)

        if agg_field not in self.tree_mapping:
            raise AbsentMappingFieldError('Agg of type <%s> on non-existing field <%s>.' % (node.AGG_TYPE, agg_field))

        # from deepest to highest
        mapping_nested_fields = list(self.tree_mapping.rsearch(agg_field, filter=lambda n: n.type == 'nested'))
        required_nested_level = next(iter(mapping_nested_fields), None)
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
                rv_node = ReverseNested(agg_name='reverse_nested_below_%s' % pid)
                super(Agg, self).add_node(rv_node, pid)
                return super(Agg, self).add_node(node, rv_node.identifier)

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(mapping_nested_fields):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (n for n in self.children(pid) if isinstance(n, Nested) and n.path == nested_lvl),
                    None
                )
                if child_nested:
                    pid = child_nested.identifier
                    continue
                nested_node = Nested(agg_name='nested_below_%s' % pid, path=nested_lvl)
                super(Agg, self).add_node(nested_node, pid)
                pid = nested_node.identifier
        super(Agg, self).add_node(node, pid)

    @property
    def deepest_linear_bucket_agg(self):
        if not self.root or not isinstance(self[self.root], BucketAggNode):
            return None
        last_bucket_agg_name = self.root
        children = self.children(last_bucket_agg_name)
        while len(children) == 1:
            last_agg = children[0]
            if not isinstance(last_agg, BucketAggNode):
                break
            last_bucket_agg_name = last_agg.agg_name
            children = self.children(last_bucket_agg_name)
        return last_bucket_agg_name

    def validate(self, exc=False):
        if self.tree_mapping is None:
            return True
        for agg_node in self.nodes.values():
            # path for 'nested'/'reverse-nested', field for metric aggregations
            for field_arg in ('field', 'path'):
                if field_arg not in agg_node.agg_body or {}:
                    continue
                field = agg_node.agg_body[field_arg]
                if field is None:
                    continue
                if field not in self.tree_mapping:
                    if exc:
                        raise AbsentMappingFieldError('Agg of type <%s> on non-existing field <%s>.' %
                                                      (agg_node.AGG_TYPE, field))
                    return False
                field_type = self.tree_mapping[field].type
                if agg_node.APPLICABLE_MAPPING_TYPES is not None and \
                        field_type not in agg_node.APPLICABLE_MAPPING_TYPES:
                    if exc:
                        raise InvalidOperationMappingFieldError('Agg of type <%s> not possible on field of type <%s>.'
                                                                % (agg_node.AGG_TYPE, field_type))
                    return False
        return True

    def _parse_group_by(self,
                        response, row=None, agg_name=None, until=None, yield_incomplete=False, row_as_tuple=False):
        """Recursive parsing of succession of unique child bucket aggregations.

        Yields each row for which last bucket aggregation generated buckets.
        """
        if not row:
            row = [] if row_as_tuple else {}
        agg_name = self.root if agg_name is None else agg_name
        if agg_name in response:
            agg_node = self[agg_name]
            yielded_child_bucket = False
            for key, raw_bucket in agg_node.extract_buckets(response[agg_name]):
                yielded_child_bucket = True
                child_name = next((child.agg_name for child in self.children(agg_name)), None)
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
                            yield_incomplete=yield_incomplete,
                            row_as_tuple=row_as_tuple
                    ):
                        yield sub_row, sub_raw_bucket
                else:
                    # end real yield
                    if row_as_tuple:
                        sub_row = tuple(sub_row)
                    yield sub_row, raw_bucket
            if not yielded_child_bucket and yield_incomplete:
                # in this case, delete last key
                if agg_name in row:
                    if row_as_tuple:
                        row.pop()
                    else:
                        del row[agg_name]
                yield row, None

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
        agg_children = self.children(agg_node.agg_name)
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
                    agg_name=child.agg_name,
                    agg_response=raw_bucket
                )
            ]
            if normalized_children:
                result['children'] = normalized_children
            yield result

    def _parse_as_dict(self, aggs_response, row_as_tuple=False, **kwargs):
        return self._parse_group_by(response=aggs_response, row_as_tuple=row_as_tuple, until=kwargs.get('grouped_by'))

    def _parse_as_dataframe(self, aggs, normalize_children=True, **kwargs):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('Using dataframe output format requires to install pandas. Please install "pandas" or '
                              'use another output format.')
        grouping_agg_name = self.deepest_linear_bucket_agg
        index_values = list(self._parse_as_dict(aggs, row_as_tuple=True, grouped_by=grouping_agg_name, **kwargs))
        if not index_values:
            return None
        index, values = zip(*index_values)
        index_names = reversed(list(self.rsearch(
            grouping_agg_name, filter=lambda x: not isinstance(x, UniqueBucketAgg)
        )))
        index = pd.MultiIndex.from_tuples(index, names=index_names)

        grouping_agg = self[grouping_agg_name]
        grouping_agg_children = self.children(grouping_agg_name)

        def parse_columns(row_data):
            result = {grouping_agg.VALUE_ATTRS[0]: grouping_agg.extract_bucket_value(row_data)}
            if not grouping_agg_children:
                return result
            for child in grouping_agg_children:
                if child.SINGLE_BUCKET:
                    result[child.agg_name] = child.extract_bucket_value(row_data[child.agg_name])
                elif normalize_children:
                    result[child.agg_name] = next(self._normalize_buckets(row_data, child.agg_name), None)
                else:
                    result[child.agg_name] = row_data[child.agg_name]
            return result

        return pd.DataFrame(index=index, data=map(parse_columns, values))

    def parse(self, aggs, output, **kwargs):
        if output == 'raw':
            return aggs
        elif output == 'tree':
            response_tree = ResponseTree(self).parse_aggregation(aggs)
            return AggResponse(tree=response_tree, depth=1)
        elif output == 'dict_rows':
            return self._parse_as_dict(aggs, **kwargs)
        elif output == 'dataframe':
            return self._parse_as_dataframe(aggs, **kwargs)
        else:
            NotImplementedError('Unkown %s output format.' % output)

    def __repr__(self):
        self.show()
        return (u'<Aggregation>\n%s' % self._reader).encode('utf-8')


class ClientBoundAggregation(Agg):

    def __init__(self, client, mapping=None, index_name=None, from_=None, query=None, identifier=None):
        self.client = client
        if client is not None:
            validate_client(self.client)
        self.index_name = index_name
        self._query = query
        super(ClientBoundAggregation, self).__init__(
            from_=from_,
            mapping=mapping,
            identifier=identifier
        )

    def copy(self, identifier=None):
        return ClientBoundAggregation(
            client=self.client,
            mapping=self.tree_mapping,
            from_=self,
            query=self._query,
            identifier=identifier
        )

    def query(self, query, validate=False):
        if validate:
            validity = self.client.indices.validate_query(index=self.index_name, body={"query": query})
            if not validity['valid']:
                raise ValueError('Wrong query: %s\n%s' % (query, validity))
        new_agg = self.copy()
        new_agg._query = query
        return new_agg

    def agg(self, arg=None, execute=True, output=Agg.DEFAULT_OUTPUT, **kwargs):
        aggregation = super(ClientBoundAggregation, self.copy()).agg(arg, **kwargs)
        if not execute:
            return aggregation
        return aggregation.execute(
            index=kwargs.get('index') or self.index_name,
            output=output,
            **kwargs
        )

    def _execute(self, aggregation, index=None, query=None):
        body = {"aggs": aggregation, "size": 0}
        if query:
            body['query'] = query
        return self.client.search(index=index, body=body)

    def execute(self, index=None, output=Agg.DEFAULT_OUTPUT, **kwargs):
        es_response = self._execute(
            aggregation=self.agg_dict(),
            index=index,
            query=self._query
        )
        return self.parse(
            aggs=es_response['aggregations'],
            output=output,
            **kwargs
        )
