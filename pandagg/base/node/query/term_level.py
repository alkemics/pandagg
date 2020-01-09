from six import iteritems

from .abstract import LeafQueryClause, FieldQueryClause


class Exists(LeafQueryClause):
    KEY = 'exists'

    def __init__(self, field, identifier=None):
        self.field = field
        super(Exists, self).__init__(identifier=identifier, field=field)

    @property
    def tag(self):
        return '%s, field=%s' % (self.KEY, self.field)


class Fuzzy(FieldQueryClause):
    KEY = 'fuzzy'


class Ids(LeafQueryClause):
    KEY = 'ids'

    def __init__(self, values, identifier=None):
        self.field = 'id'
        self.values = values
        super(Ids, self).__init__(identifier=identifier, values=values)

    def serialize(self):
        return {self.KEY: {'values': self.values}}

    @property
    def tag(self):
        return '%s, values=%s' % (self.KEY, self.values)


class Prefix(FieldQueryClause):
    KEY = 'prefix'


class Range(FieldQueryClause):
    KEY = 'range'


class Regexp(FieldQueryClause):
    KEY = 'regexp'


class Term(FieldQueryClause):
    SHORT_TAG = 'value'
    KEY = 'term'


class Terms(LeafQueryClause):
    KEY = 'terms'

    def __init__(self, field, terms, identifier=None, **body):
        self.field = field
        self.terms = terms
        b = {field: terms}
        b.update(body)
        super(Terms, self).__init__(identifier=identifier, **b)

    @property
    def tag(self):
        return '%s, field=%s' % (self.KEY, self.field)

    @classmethod
    def deserialize(cls, **body):
        boost = body.pop('boost', None)
        assert len(body.keys()) == 1
        k, v = next(iteritems(body))
        return cls(boost=boost, field=k, terms=v)


class TermsSet(FieldQueryClause):
    KEY = 'terms_set'


class Type(FieldQueryClause):
    KEY = 'type'


class Wildcard(FieldQueryClause):
    KEY = 'wildcard'


TERM_LEVEL_QUERIES = [
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
