
from .compound import COMPOUND_QUERIES
from .specialized_compound import SPECIALIZED_COMPOUND_QUERIES
from .joining import JOINING_QUERIES

COMPOUND_CLAUSES = {
    q.KEY: q
    for q in
    COMPOUND_QUERIES + SPECIALIZED_COMPOUND_QUERIES + JOINING_QUERIES
}


def deserialize_compound_clause(key, body):
    if key not in COMPOUND_CLAUSES.keys():
        raise NotImplementedError('Unknown compound clause of type <%s>' % key)
    klass = COMPOUND_CLAUSES[key]
    return klass.deserialize(**body)
