from .abstract import (
    LeafQueryClause,
    AbstractSingleFieldQueryClause,
    KeyFieldQueryClause,
)


class Exists(LeafQueryClause):
    KEY = "exists"

    def __init__(self, field, _name=None):
        self.field = field
        super(Exists, self).__init__(_name=_name, field=field)

    def line_repr(self, depth, **kwargs):
        return "%s, field=%s" % (self.KEY, self.field)


class Fuzzy(KeyFieldQueryClause):
    KEY = "fuzzy"
    _DEFAULT_PARAM = "value"


class Ids(LeafQueryClause):
    KEY = "ids"

    def __init__(self, values, _name=None):
        self.field = "id"
        self.values = values
        super(Ids, self).__init__(_name=_name, values=values)

    def to_dict(self, with_name=True):
        b = {"values": self.values}
        if with_name and self._named:
            b["_name"] = self.name
        return {self.KEY: b}

    def line_repr(self, depth, **kwargs):
        return "%s, values=%s" % (self.KEY, self.values)


class Prefix(KeyFieldQueryClause):
    KEY = "prefix"
    _DEFAULT_PARAM = "value"


class Range(KeyFieldQueryClause):
    KEY = "range"


class Regexp(KeyFieldQueryClause):
    KEY = "regexp"
    _DEFAULT_PARAM = "value"


class Term(KeyFieldQueryClause):
    KEY = "term"
    _DEFAULT_PARAM = "value"


class Terms(AbstractSingleFieldQueryClause):
    KEY = "terms"

    def __init__(self, **body):
        _name = body.pop("_name", None)
        boost = body.pop("boost", None)
        if len(body) != 1:
            raise ValueError("Wrong declaration: %s" % body)
        field, terms = self.expand__to_dot(body).popitem()
        b = {field: terms}
        if boost is not None:
            b["boost"] = boost
        super(Terms, self).__init__(_name=_name, field=field, **b)


class TermsSet(KeyFieldQueryClause):
    KEY = "terms_set"
    _DEFAULT_PARAM = "terms"


class Type(KeyFieldQueryClause):
    KEY = "type"
    _DEFAULT_PARAM = "value"


class Wildcard(KeyFieldQueryClause):
    KEY = "wildcard"
    _DEFAULT_PARAM = "value"
