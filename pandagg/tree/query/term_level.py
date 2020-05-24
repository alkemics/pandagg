from .abstract import Leaf


class Exists(Leaf):
    KEY = "exists"


class Fuzzy(Leaf):
    KEY = "fuzzy"


class Ids(Leaf):
    KEY = "ids"


class Prefix(Leaf):
    KEY = "prefix"


class Range(Leaf):
    KEY = "range"


class Regexp(Leaf):
    KEY = "regexp"


class Term(Leaf):
    KEY = "term"


class Terms(Leaf):
    KEY = "terms"


class TermsSet(Leaf):
    KEY = "terms_set"


class Type(Leaf):
    KEY = "type"


class Wildcard(Leaf):
    KEY = "wildcard"
