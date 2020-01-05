
from .abstract import LeafQueryClause


class Exists(LeafQueryClause):
    ALLOW_SIMPLE_VALUE = True
    KEY = 'exists'


class Fuzzy(LeafQueryClause):
    KEY = 'fuzzy'


class Ids(LeafQueryClause):
    KEY = 'ids'

    def __init__(self, values, identifier=None):
        self.values = values
        super(Ids, self).__init__(field='ids', identifier=identifier, values=values)

    def serialize(self):
        return {self.KEY: {'values': self.values}}

    @property
    def tag(self):
        return '%s, values=%s' % (self.KEY, self.values)


class Prefix(LeafQueryClause):
    ALLOW_SIMPLE_VALUE = True
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
