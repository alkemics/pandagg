#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from pandagg.exceptions import InvalidElasticSearchClientError


class Obj(object):

    def __init__(self):
        # will store non-valid names
        self.__d = dict()

    def __getitem__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return self.__d[item]

    def __setitem__(self, key, value):
        if re.match(string=key, pattern=r'.*[^a-zA-Z0-9_]'):
            self.__d[key] = value
        else:
            super(Obj, self).__setattr__(key, value)

    def __keys(self):
        return self.__d.keys() + [k for k in self.__dict__.keys() if k != '_Obj__d']

    def __repr__(self):
        return list.__repr__(self.__keys())

    def __str__(self):
        return self.__repr__()


class PrettyNode(Obj):
    # class to display pretty nodes while working with trees
    def __init__(self, pretty):
        super(PrettyNode, self).__init__()
        self.pretty = pretty


class NestedMixin(object):

    @staticmethod
    def requires_nested(previous, next):
        if next is None:
            return False
        if previous is None:
            return True
        return previous in next and next not in previous

    @staticmethod
    def requires_outnested(previous, next):
        if previous is None:
            return False
        if next is None:
            return True
        return next in previous and previous not in next

    @staticmethod
    def safe_apply_nested(previous, next):
        if next and (previous is None or previous in next):
            return next
        raise ValueError('Cannot navigate from nested "%s" to nested "%s".' % (previous, next))

    @staticmethod
    def safe_apply_outnested(previous, next):
        if previous is None:
            raise ValueError('Cannot navigate outnested if no nested is applied.')
        if next is None:
            return next
        if next in previous:
            return next
        else:
            raise ValueError('Cannot navigate from nested "%s" to outnested "%s".' % (previous, next))


def bool_if_required(sub_filters, advanced_search_syntax=True, operator='must'):
    # wrap conditions in bool only if necessary
    if len(sub_filters) == 1:
        return sub_filters[0]
    if len(sub_filters) > 1:
        if advanced_search_syntax:
            return {operator: sub_filters}
        return {'bool': {operator: sub_filters}}
    return None


def validate_client(client):
    for method_name in ('info', 'search', 'validate'):
        if not hasattr(client, method_name) and callable(client):
            raise InvalidElasticSearchClientError('You client doesn\'t seem compatible.')
