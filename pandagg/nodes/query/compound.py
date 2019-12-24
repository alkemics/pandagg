
from .abstract import CompoundClause, ParameterClause


class Filter(ParameterClause):
    P_TYPE = 'filter'
    MULTIPLE = True


class Must(ParameterClause):
    P_TYPE = 'must'
    MULTIPLE = True


class Should(ParameterClause):
    P_TYPE = 'should'
    MULTIPLE = True


class MustNot(ParameterClause):
    P_TYPE = 'must_not'
    MULTIPLE = True


class Bool(CompoundClause):
    Q_TYPE = 'bool'
    PARAMS = [Filter, Must, Should, MustNot]


class Boosting(CompoundClause):
    Q_TYPE = 'boosting'


class ConstantScore(CompoundClause):
    Q_TYPE = 'constant_score'


class DisMax(CompoundClause):
    Q_TYPE = 'dis_max'


class FunctionScore(CompoundClause):
    Q_TYPE = 'function_score'
