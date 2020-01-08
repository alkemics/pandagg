
from six import iteritems
from .abstract import LeafQueryClause
from .term_level import Exists, Fuzzy, Ids, Prefix, Range, Regexp, Term, Terms, TermsSet, Type, Wildcard
from .full_text import Intervals, Match, MatchBoolPrefix, MatchPhrase, MatchPhrasePrefix, MultiMatch, Common, QueryString, SimpleString
from .compound import CompoundClause, Bool, Boosting, ConstantScore, FunctionScore, DisMax
from .joining import Nested, HasChild, HasParent, ParentId
from ._parameter_clause import Filter, MustNot, Must, Should

QUERIES = {
    q.KEY: q
    for q in [
        # term level
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
        # full text
        Intervals,
        Match,
        MatchBoolPrefix,
        MatchPhrase,
        MatchPhrasePrefix,
        MultiMatch,
        Common,
        QueryString,
        SimpleString,
        # compound
        Bool,
        Boosting,
        ConstantScore,
        FunctionScore,
        DisMax,
        # joining
        Nested,
        HasParent,
        HasChild,
        ParentId
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
