#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree import Node as OriginalNode

from pandagg.utils import DSLMixin
from typing import Optional


class Node(DSLMixin, OriginalNode):

    KEY: Optional[str] = None
    _type_name: Optional[str] = None

    NID_SIZE = 8

    @staticmethod
    def expand__to_dot(params):
        nparams = {}
        for pname, pvalue in params.items():
            if "__" in pname:
                pname = pname.replace("__", ".")
            nparams[pname] = pvalue
        return nparams
