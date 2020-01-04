
from six import iteritems
from .abstract import LeafQueryClause
from .term_level import Exists, Fuzzy, Ids, Prefix, Range, Regexp, Term, Terms, TermsSet, Type, Wildcard
from .compound import CompoundClause, Bool, Boosting, ConstantScore, FunctionScore, DisMax
from ._parameter_clause import Filter, MustNot, Must, Should

QUERIES = {
    q.Q_TYPE: q
    for q in [
        # term level queries
        Exists,
        Fuzzy,
        Ids,
        Prefix,
        Range,
        Regexp,
        Term,
        Terms,
        TermsSet,
        Type,
        Wildcard,
        # compound
        Bool,
        Boosting,
        ConstantScore,
        FunctionScore,
        DisMax,
    ]
}


def deserialize_query_node(query):
    assert isinstance(query, dict)
    assert len(query.keys()) == 1
    q_type, body = next(iteritems(query))
    assert q_type in QUERIES
    klass = QUERIES[q_type]
    if issubclass(klass, LeafQueryClause):
        return klass.deserialize(body)
    assert issubclass(klass, CompoundClause)
    return klass.deserialize(body)
