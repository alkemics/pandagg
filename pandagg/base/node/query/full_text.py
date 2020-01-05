
from .abstract import LeafQueryClause


class Intervals(LeafQueryClause):
    KEY = 'intervals'


class Match(LeafQueryClause):
    KEY = 'match'


class MatchBoolPrefix(LeafQueryClause):
    KEY = 'match_bool_prefix'


class MatchPhrase(LeafQueryClause):
    KEY = 'match_phrase'


class MatchPhrasePrefix(LeafQueryClause):
    KEY = 'match_phrase_prefix'


class MultiMatch(LeafQueryClause):
    KEY = 'multi_match'


class Common(LeafQueryClause):
    KEY = 'common'


class QueryString(LeafQueryClause):
    KEY = 'query_string'


class SimpleString(LeafQueryClause):
    KEY = 'simple_string'
