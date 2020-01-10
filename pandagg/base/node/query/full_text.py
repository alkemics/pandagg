
from .abstract import LeafQueryClause, SingleFieldQueryClause, MultiFieldsQueryClause


class Intervals(SingleFieldQueryClause):
    KEY = 'intervals'


class Match(SingleFieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match'


class MatchBoolPrefix(SingleFieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match_bool_prefix'


class MatchPhrase(SingleFieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match_phrase'


class MatchPhrasePrefix(SingleFieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match_phrase_prefix'


class MultiMatch(MultiFieldsQueryClause):
    KEY = 'multi_match'


class Common(SingleFieldQueryClause):
    KEY = 'common'


class QueryString(LeafQueryClause):
    # improvement: detect fields for validation
    KEY = 'query_string'


class SimpleQueryString(LeafQueryClause):
    # improvement: detect fields for validation
    KEY = 'simple_string'


FULL_TEXT_QUERIES = [
    Intervals,
    Match,
    MatchBoolPrefix,
    MatchPhrase,
    MatchPhrasePrefix,
    MultiMatch,
    Common,
    QueryString,
    SimpleQueryString
]
