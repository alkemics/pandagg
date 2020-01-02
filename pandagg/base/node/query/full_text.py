
from .abstract import LeafQueryClause


class Intervals(LeafQueryClause):
    Q_TYPE = 'intervals'


class Match(LeafQueryClause):
    Q_TYPE = 'match'


class MatchBoolPrefix(LeafQueryClause):
    Q_TYPE = 'match_bool_prefix'


class MatchPhrase(LeafQueryClause):
    Q_TYPE = 'match_phrase'


class MatchPhrasePrefix(LeafQueryClause):
    Q_TYPE = 'match_phrase_prefix'


class MultiMatch(LeafQueryClause):
    Q_TYPE = 'multi_match'


class Common(LeafQueryClause):
    Q_TYPE = 'common'


class QueryString(LeafQueryClause):
    Q_TYPE = 'query_string'


class SimpleString(LeafQueryClause):
    Q_TYPE = 'simple_string'
