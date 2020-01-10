
from six import iteritems
from .abstract import LeafQueryClause
from .term_level import TERM_LEVEL_QUERIES
from .full_text import FULL_TEXT_QUERIES
from .compound import COMPOUND_QUERIES, CompoundClause
from .joining import JOINING_QUERIES
from .geo import GEO_QUERIES
from ._parameter_clause import PARENT_PARAMETERS

QUERIES = {
    q.KEY: q for q in
    TERM_LEVEL_QUERIES +
    FULL_TEXT_QUERIES +
    COMPOUND_QUERIES +
    JOINING_QUERIES +
    GEO_QUERIES +
    PARENT_PARAMETERS
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
