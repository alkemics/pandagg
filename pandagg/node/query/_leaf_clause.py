
from .term_level import TERM_LEVEL_QUERIES
from .full_text import FULL_TEXT_QUERIES
from .geo import GEO_QUERIES
from .shape import SHAPE_QUERIES
from .specialized import SPECIALIZED_QUERIES


LEAF_CLAUSES = {
    q.KEY: q
    for q in
    TERM_LEVEL_QUERIES + FULL_TEXT_QUERIES + GEO_QUERIES + SHAPE_QUERIES + SPECIALIZED_QUERIES
}


def deserialize_leaf_clause(key, body):
    if key not in LEAF_CLAUSES.keys():
        raise NotImplementedError('Unknown query type <%s>' % key)
    klass = LEAF_CLAUSES[key]
    return klass.deserialize(**body)
