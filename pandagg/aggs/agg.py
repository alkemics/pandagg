#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import collections

from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError, InvalidAggregation
from pandagg.tree import Tree
from pandagg.utils import NestedMixin, validate_client
from pandagg.aggs.agg_nodes import (
    AggNode, PUBLIC_AGGS, Terms, Nested, ReverseNested, MatchAll, BucketAggNode,
)
from pandagg.mapping.mapping import Mapping, TreeMapping
from pandagg.aggs.response_tree import AggResponse


class Agg(NestedMixin, Tree):
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
            self._build_tree_from_agg_node(from_agg_node)

    def _get_instance(self, identifier=None):
        return Agg(mapping=self.tree_mapping, identifier=identifier)

    def copy(self, identifier=None):
        return Agg(mapping=self.tree_mapping, from_=self, identifier=identifier)

    def set_mapping(self, mapping):
        if mapping is not None:
            if isinstance(mapping, TreeMapping):
                self.tree_mapping = mapping
            elif isinstance(mapping, Mapping):
                self.tree_mapping = mapping._tree
            elif isinstance(mapping, dict):
                mapping_name, mapping_detail = next(mapping.iteritems())
                self.tree_mapping = TreeMapping(mapping_name, mapping_detail)
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

    def _build_tree_from_agg_node(self, agg_node, pid=None):
        self.add_node(agg_node, pid)
        if isinstance(agg_node, BucketAggNode):
            for child_agg_node in agg_node.aggs or []:
                self._build_tree_from_agg_node(child_agg_node, pid=agg_node.identifier)
            # reset children to None to avoid confusion since this serves only __init__ syntax.
            agg_node.children = None

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

        # TODO - double check nested in case of iterable
        if isinstance(arg, collections.Iterable) and not isinstance(arg, basestring) and not isinstance(arg, dict):
            for arg_el in arg:
                new_agg._interpret_agg(sub_aggs_parent_id, arg_el, **kwargs)
        else:
            new_agg._interpret_agg(sub_aggs_parent_id, arg, **kwargs)
        return new_agg

    def _interpret_agg(self, insert_below, element, **kwargs):
        if isinstance(element, basestring):
            node = Terms(agg_name=element, field=element, size=kwargs.get('default_size', Terms.DEFAULT_SIZE))
            self.add_node(node, parent=insert_below)
            return self
        if isinstance(element, dict):
            # TODO - double check nested
            try:
                self.paste(nid=insert_below, new_tree=Agg(from_=element))
            except AbsentMappingFieldError:
                pass
            return self
        if isinstance(element, AggNode):
            assert element.AGG_TYPE in PUBLIC_AGGS.keys()
            self._build_tree_from_agg_node(element, pid=insert_below)
            return self
        if isinstance(element, Agg):
            # TODO - recheck nested checks
            self.paste_with_nested_check(
                nid=insert_below,
                tree=element,
                required_nested_path=self.applied_nested_path_at_node(insert_below)
            )
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
                applied_nested_path = self.safe_apply_nested(applied_nested_path, node.path)
            elif isinstance(node, ReverseNested):
                # a reverse nested remove nested paths, except the one specified if there is one
                applied_nested_path = self.safe_apply_outnested(applied_nested_path, node.path)
        return applied_nested_path

    def paste_multiple_with_nested_check(self, nid, *trees_with_nested_requirement):
        # TODO - rethink this, with provided mapping
        """Paste multiple trees at a given node, and generating all necessary nested or reverse nested aggregations.
        """
        root_nid_nested = self.applied_nested_path_at_node(nid)

        # apply paste on all trees currently at right level
        trees_right_nested = [
            tree for tree, required_nested in trees_with_nested_requirement if required_nested == root_nid_nested
        ]
        for tree in trees_right_nested:
            super(Agg, self).paste(nid, tree)

        # if some trees require to reverse nested, apply reverse nested to highest path (nearest to root)
        trees_to_reverse_nest = [
            (tree, required_nested) for tree, required_nested in trees_with_nested_requirement
            if self.requires_outnested(root_nid_nested, required_nested)
        ]
        if trees_to_reverse_nest:
            ordered_nested_path = sorted([el[1] for el in trees_to_reverse_nest])
            common_path = self.safe_apply_outnested(root_nid_nested, ordered_nested_path[0])
            reverse_nested_identifier = '%s_reverse_nested_below_%s' % (common_path.replace('.', '_') if common_path
                                                                        else 'root', nid)
            reverse_nested_node = ReverseNested(agg_name=reverse_nested_identifier, path=common_path)
            self.add_node(reverse_nested_node, nid)
            self.paste_multiple_with_nested_check(reverse_nested_identifier, *trees_to_reverse_nest)

        # if some trees require nested, apply nested to highest path (nearest to root)
        trees_to_nest = [
            (tree, required_nested) for tree, required_nested in trees_with_nested_requirement
            if self.requires_nested(root_nid_nested, required_nested)
        ]
        if trees_to_nest:
            ordered_nested_path = sorted([el[1] for el in trees_to_nest])
            common_path = self.safe_apply_nested(root_nid_nested, ordered_nested_path[0])
            nested_identifier = '%s_nested_below_%s' % (common_path.replace('.', '_') if common_path else 'root', nid)
            nested_node = Nested(agg_name=nested_identifier, path=common_path)
            self.add_node(nested_node, nid)
            self.paste_multiple_with_nested_check(nested_identifier, *trees_to_nest)

    def paste_with_nested_check(self, nid, tree, required_nested_path=None):
        self.paste_multiple_with_nested_check(nid, (tree, required_nested_path))

    def add_node(self, node, parent=None):
        """If mapping is provided, nested and outnested are automatically applied.
        """
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if isinstance(node, Nested) or isinstance(node, ReverseNested):
            super(Agg, self).add_node(node, parent)
            return self.validate(exc=True)

        agg_field = node.agg_body.get('field')
        if agg_field is None or self.tree_mapping is None:
            return super(Agg, self).add_node(node, parent)

        if agg_field not in self.tree_mapping:
            raise AbsentMappingFieldError('Agg of type <%s> on non-existing field <%s>.' % (node.AGG_TYPE, agg_field))

        # from deepest to highest
        mapping_nested_fields = list(self.tree_mapping.rsearch(agg_field, filter=lambda n: n.type == 'nested'))
        required_nested_level = next(iter(mapping_nested_fields), None)
        current_nested_level = self.applied_nested_path_at_node(parent)
        if current_nested_level == required_nested_level:
            return super(Agg, self).add_node(node, parent)
        if current_nested_level and (required_nested_level or '' in current_nested_level):
            # requires reverse-nested
            rv_node = ReverseNested(agg_name='reverse_nested_below_%s' % parent)
            super(Agg, self).add_node(rv_node, parent)
            return super(Agg, self).add_node(node, rv_node.identifier)

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(mapping_nested_fields):
            if current_nested_level != nested_lvl:
                nested_node = Nested(agg_name='nested_below_%s' % parent, path=nested_lvl)
                super(Agg, self).add_node(nested_node, parent)
                parent = nested_node.identifier
        super(Agg, self).add_node(node, parent)

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
        index_names = reversed(list(self.rsearch(grouping_agg_name)))
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
            return AggResponse(self).parse_aggregation(aggs)
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
        es_response = self._execute(
            aggregation=aggregation.agg_dict(),
            index=aggregation.index_name,
            query=aggregation._query
        )
        return aggregation.parse(
            aggs=es_response['aggregations'],
            output=output,
            **kwargs
        )

    def _execute(self, aggregation, index=None, query=None):
        body = {"aggs": aggregation, "size": 0}
        if query:
            body['query'] = query
        return self.client.search(index=index, body=body)
