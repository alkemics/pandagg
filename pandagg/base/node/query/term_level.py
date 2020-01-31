
from .abstract import LeafQueryClause, SingleFieldQueryClause
from builtins import str as text


class Exists(LeafQueryClause):
    KEY = 'exists'

    def __init__(self, field, identifier=None):
        self.field = field
        super(Exists, self).__init__(identifier=identifier, field=field)

    @property
    def tag(self):
        return '%s, field=%s' % (self.KEY, self.field)


class Fuzzy(SingleFieldQueryClause):
    KEY = 'fuzzy'


class Ids(LeafQueryClause):
    KEY = 'ids'

    def __init__(self, values, identifier=None):
        self.field = 'id'
        self.values = values
        super(Ids, self).__init__(identifier=identifier, values=values)

    def serialize(self, with_identifier=False):
        b = {'values': self.values}
        if with_identifier:
            b['_name'] = self.identifier
        return {self.KEY: b}

    @property
    def tag(self):
        return '%s, values=%s' % (self.KEY, self.values)


class Prefix(SingleFieldQueryClause):
    KEY = 'prefix'


class Range(SingleFieldQueryClause):
    KEY = 'range'


class Regexp(SingleFieldQueryClause):
    KEY = 'regexp'


class Term(SingleFieldQueryClause):
    SHORT_TAG = 'value'
    KEY = 'term'

    def __init__(self, field, value, identifier=None, **body):
        # only impact is setting value as required arg
        super(Term, self).__init__(field=field, value=value, identifier=identifier, **body)


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
        return '%s, field=%s, values=%s' % (self.KEY, self.field, map(text, self.terms))

    @classmethod
    def deserialize(cls, **body):
        allowed_params = {'boost'}
        other_keys = set(body.keys()).difference(allowed_params)
        assert len(other_keys) == 1
        field_key = other_keys.pop()
        field_value = body.pop(field_key)
        return cls(field=field_key, terms=field_value, **body)


class TermsSet(SingleFieldQueryClause):
    KEY = 'terms_set'


class Type(SingleFieldQueryClause):
    KEY = 'type'


class Wildcard(SingleFieldQueryClause):
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
