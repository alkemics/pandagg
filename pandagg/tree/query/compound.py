#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pandagg.tree.query.abstract import Compound


class Bool(Compound):
    KEY = "bool"


class Boosting(Compound):
    KEY = "boosting"


class ConstantScore(Compound):
    KEY = "constant_score"


class DisMax(Compound):
    KEY = "dis_max"


class FunctionScore(Compound):
    KEY = "function_score"
