#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree import Node as OriginalNode

from pandagg.utils import DSLMixin


class Node(DSLMixin, OriginalNode):

    KEY = None
    _type_name = None

    NID_SIZE = 8

    @staticmethod
    def expand__to_dot(params):
        nparams = {}
        for pname, pvalue in params.items():
            if "__" in pname:
                pname = pname.replace("__", ".")
            nparams[pname] = pvalue
        return nparams
