
from .term_level import Exists, Fuzzy, Ids, Prefix, Range, Regexp, Term, Terms, TermsSet, Type, Wildcard


LEAF_CLAUSES = {
    q.KEY: q
    for q in [
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
        Wildcard
    ]
}


def deserialize_leaf_clause(key, body):
    if key not in LEAF_CLAUSES.keys():
        raise NotImplementedError('Unknown query type <%s>' % key)
    klass = LEAF_CLAUSES[key]
    return klass.deserialize(**body)
