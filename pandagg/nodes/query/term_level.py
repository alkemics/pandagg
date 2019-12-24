
from .abstract import LeafQueryClause


class Exists(LeafQueryClause):
    Q_TYPE = 'exists'


class Fuzzy(LeafQueryClause):
    Q_TYPE = 'fuzzy'


class Ids(LeafQueryClause):
    Q_TYPE = 'ids'


class Prefix(LeafQueryClause):
    Q_TYPE = 'prefix'


class Range(LeafQueryClause):
    Q_TYPE = 'range'


class Regexp(LeafQueryClause):
    Q_TYPE = 'regexp'


class Term(LeafQueryClause):
    Q_TYPE = 'term'


class Terms(LeafQueryClause):
    Q_TYPE = 'terms'


class TermsSet(LeafQueryClause):
    Q_TYPE = 'terms_set'


class Type(LeafQueryClause):
    Q_TYPE = 'type'


class Wildcard(LeafQueryClause):
    Q_TYPE = 'wildcard'
