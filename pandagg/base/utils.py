#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


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
