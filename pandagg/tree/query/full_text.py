from .abstract import Leaf


class Intervals(Leaf):
    KEY = "intervals"


class Match(Leaf):
    KEY = "match"


class MatchBoolPrefix(Leaf):
    KEY = "match_bool_prefix"


class MatchPhrase(Leaf):
    KEY = "match_phrase"


class MatchPhrasePrefix(Leaf):
    KEY = "match_phrase_prefix"


class MultiMatch(Leaf):
    KEY = "multi_match"


class Common(Leaf):
    KEY = "common"


class QueryString(Leaf):
    KEY = "query_string"


class SimpleQueryString(Leaf):
    KEY = "simple_string"
