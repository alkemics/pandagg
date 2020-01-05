
from .abstract import LeafQueryClause


class Exists(LeafQueryClause):
    KEY = 'exists'


class Fuzzy(LeafQueryClause):
    KEY = 'fuzzy'


class Ids(LeafQueryClause):
    KEY = 'ids'


class Prefix(LeafQueryClause):
    KEY = 'prefix'


class Range(LeafQueryClause):
    KEY = 'range'


class Regexp(LeafQueryClause):
    KEY = 'regexp'


class Term(LeafQueryClause):
    KEY = 'term'


class Terms(LeafQueryClause):
    KEY = 'terms'


class TermsSet(LeafQueryClause):
    KEY = 'terms_set'


class Type(LeafQueryClause):
    KEY = 'type'


class Wildcard(LeafQueryClause):
    KEY = 'wildcard'
