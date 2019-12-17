#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from builtins import str as text
from six import python_2_unicode_compatible
import re
import unicodedata
from six import string_types
from pandagg.tree import Tree


def is_valid_attr_name(item):
    if not isinstance(item, string_types):
        return False
    return re.match(string=item, pattern=r'^[a-zA-Z_]+[a-zA-Z0-9_]*$') is not None


@python_2_unicode_compatible
class Obj(object):
    """Object class that allows to get items both by attribute `__getattribute__` access: `obj.attribute` or by dict
    `__getitem__` access:
    >>> obj = Obj(key='value')
    >>> obj.key
    'value'
    >>> obj['key']
    'value'

    In Ipython interpreter, attributes will be available in autocomplete (except private ones):
    >>> obj = Obj(key='value', key2='value2')
    >>> obj.k  # press tab for autocompletion
    key
    key2

    Items names that are not compliant with python attributes (accepted characters are [a-zA-Z0-9_] without beginning
    with a figure), will be only available through dict `__getitem__` access.
    """
    _REPR_NAME = None
    _STRING_KEY_CONSTRAINT = True

    def __init__(self, **kwargs):
        # will store non-valid names
        self.__d = dict()
        for k, v in kwargs.items():
            assert isinstance(k, string_types) and k not in ('_REPR_NAME', '_Obj__d') and not k.startswith('__')
            self[k] = v

    def __getitem__(self, item):
        # when calling d[key]
        if is_valid_attr_name(item):
            return self.__getattribute__(item)
        else:
            return self.__d[item]

    def __setitem__(self, key, value):
        # d[key] = value
        if not isinstance(key, string_types):
            if self._STRING_KEY_CONSTRAINT:
                raise AssertionError
            self.__d[key] = value
            return
        assert not key.startswith('__')
        if not is_valid_attr_name(key):
            self.__d[key] = value
        else:
            super(Obj, self).__setattr__(key, value)

    def __keys(self):
        return list(self.__d.keys()) + [
            k for k in self.__dict__.keys()
            if k not in ('_REPR_NAME', '_Obj__d')
        ]

    def __contains__(self, item):
        return item in self.__keys()

    def __str__(self):
        return '<%s> %s' % (
            text(self.__class__._REPR_NAME or self.__class__.__name__),
            text(sorted(map(text, self.__keys())))
        )

    def __repr__(self):
        return self.__str__()


@python_2_unicode_compatible
class TreeBasedObj(Obj):
    """
    Recursive Obj whose structure is defined by a treelib.Tree object.

    The main purpose of this object is to iteratively expand the tree as attributes of this object. To avoid creating
    useless instances, only direct children of accessed nodes are expanded.
    """
    _NODE_PATH_ATTR = 'tag'
    _COERCE_ATTR = False

    def __init__(self, tree, root_path=None, depth=None, initial_tree=None):
        super(TreeBasedObj, self).__init__()
        assert isinstance(tree, Tree)
        self._tree = tree
        self._root_path = root_path
        self._initial_tree = initial_tree if initial_tree is not None else tree
        self._expand_attrs(depth)

    def _clone(self, nid, root_path, depth):
        return self.__class__(
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree
        )

    @staticmethod
    def _coerce_attr(attr):
        attr = unicodedata.normalize("NFD", attr).encode("ASCII", "ignore").decode()
        return re.sub(
            string=attr,
            pattern=r'[^a-zA-Z_0-9]',
            repl='_'
        )

    def _expand_attrs(self, depth):
        if depth:
            for child in self._tree.children(nid=self._tree.root):
                child_path = getattr(child, self._NODE_PATH_ATTR)
                if self._COERCE_ATTR:
                    child_path = self._coerce_attr(child_path)
                if child_path in self:
                    continue
                if self._root_path is not None:
                    child_root = '%s.%s' % (self._root_path, child_path)
                else:
                    child_root = child_path
                self[child_path] = self._clone(child.identifier, root_path=child_root, depth=depth - 1)

    def __getattribute__(self, item):
        # called by __getattribute__ will always refer to valid attribute item
        r = super(TreeBasedObj, self).__getattribute__(item)
        if isinstance(r, TreeBasedObj):
            r._expand_attrs(depth=1)
        return r

    def __getitem__(self, item):
        if is_valid_attr_name(item):
            r = super(TreeBasedObj, self).__getattribute__(item)
        else:
            r = super(TreeBasedObj, self).__getitem__(item)
        if isinstance(r, TreeBasedObj):
            r._expand_attrs(depth=1)
        return r

    def __str__(self):
        tree_repr = self._tree.show()
        if self._root_path is None:
            return '<%s>\n%s' % (
                text(self.__class__._REPR_NAME or self.__class__.__name__),
                text(tree_repr)
            )
        current_path = self._root_path
        return '<%s subpart: %s>\n%s' % (
            text(self.__class__._REPR_NAME or self.__class__.__name__),
            text(current_path),
            text(tree_repr)
        )


class PrettyNode(object):
    # class to display pretty nodes while working with trees
    __slots__ = ['pretty']

    def __init__(self, pretty):
        super(PrettyNode, self).__init__()
        self.pretty = pretty


def bool_if_required(conditions, operator='must'):
    assert operator in ('must', 'should')
    # wrap conditions in bool only if necessary
    if len(conditions) == 1:
        return conditions[0]
    if len(conditions) > 1:
        flattened_sub_conditions = []
        for sub_condition in conditions:
            if 'bool' in sub_condition and len(sub_condition['bool'].keys()) == 1 \
                    and list(sub_condition['bool'].keys())[0] == operator:
                operator_cond = list(sub_condition['bool'].values())[0]
                # both are valid: {"must": [query_A, query_B]}, or {"must": queryA}
                if isinstance(operator_cond, list):
                    flattened_sub_conditions.extend(operator_cond)
                else:
                    flattened_sub_conditions.append(operator_cond)
            else:
                flattened_sub_conditions.append(sub_condition)
        return {'bool': {operator: flattened_sub_conditions}}
    return None
