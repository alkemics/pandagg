#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from builtins import str as text
from six import python_2_unicode_compatible
import re
from six import string_types
from pandagg.exceptions import InvalidElasticSearchClientError
from pandagg.tree import Tree


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
        try:
            return self.__getattribute__(item)
        except (AttributeError, TypeError):
            return self.__d[item]

    def __setitem__(self, key, value):
        # d[key] = value
        if not isinstance(key, string_types):
            if self._STRING_KEY_CONSTRAINT:
                raise AssertionError
            self.__d[key] = value
            return
        assert not key.startswith('__')
        if re.match(string=key, pattern=r'.*[^a-zA-Z0-9_]'):
            self.__d[key] = value
        else:
            super(Obj, self).__setattr__(key, value)

    def __keys(self):
        return list(self.__d.keys()) + [
            k for k in self.__dict__.keys()
            if k not in ('_REPR_NAME', '_Obj__d')
        ]

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

    def __init__(self, tree, root_path=None, depth=None, initial_tree=None):
        super(TreeBasedObj, self).__init__()
        assert isinstance(tree, Tree)
        self._tree = tree
        self._root_path = root_path
        self._initial_tree = initial_tree if initial_tree is not None else tree
        self._expand_attrs(depth)

    def _get_instance(self, nid, root_path, depth, **kwargs):
        return self.__class__(
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree
        )

    def _expand_attrs(self, depth):
        if depth:
            for child in self._tree.children(nid=self._tree.root):
                child_path = getattr(child, self._NODE_PATH_ATTR)
                if hasattr(self, child_path):
                    continue
                if self._root_path is not None:
                    child_root = '%s.%s' % (self._root_path, child_path)
                else:
                    child_root = child_path
                self[child_path] = self._get_instance(child.identifier, root_path=child_root, depth=depth - 1)

    def __getattribute__(self, item):
        r = super(TreeBasedObj, self).__getattribute__(item)
        if isinstance(r, TreeBasedObj):
            r._expand_attrs(depth=1)
        return r

    def __getitem__(self, item):
        try:
            r = super(TreeBasedObj, self).__getattribute__(item)
        except AttributeError:
            r = self.__d[item]
        if isinstance(r, TreeBasedObj):
            r._expand_attrs(depth=1)
        return r

    def __str__(self):
        tree_repr = self._tree.show()
        if self._root_path is None:
            return '\n<%s>\n%s' % (
                text(self.__class__._REPR_NAME or self.__class__.__name__),
                tree_repr
            )
        current_path = self._root_path
        return '\n<%s subpart: %s>\n%s' % (
            text(self.__class__._REPR_NAME or self.__class__.__name__),
            current_path, tree_repr
        )


class PrettyNode(object):
    # class to display pretty nodes while working with trees
    def __init__(self, pretty):
        super(PrettyNode, self).__init__()
        self.pretty = pretty


def bool_if_required(sub_filters, operator='must'):
    # wrap conditions in bool only if necessary
    if len(sub_filters) == 1:
        return sub_filters[0]
    if len(sub_filters) > 1:
        return {'bool': {operator: sub_filters}}
    return None


def validate_client(client):
    for method_name in ('info', 'search', 'validate', 'indices'):
        if not hasattr(client, method_name) and callable(client):
            raise InvalidElasticSearchClientError('You client doesn\'t seem compatible.')
