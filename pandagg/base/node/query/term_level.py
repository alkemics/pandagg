
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
        allowed_params = {'boost'}
        other_keys = set(body.keys()).difference(allowed_params)
        assert len(other_keys) == 1
        field_key = other_keys.pop()
        field_value = body.pop(field_key)
        return cls(field=field_key, terms=field_value, **body)


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
