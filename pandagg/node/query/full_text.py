from .abstract import LeafQueryClause, KeyFieldQueryClause, MultiFieldsQueryClause


class Intervals(KeyFieldQueryClause):
    KEY = "intervals"


class Match(KeyFieldQueryClause):
    _DEFAULT_PARAM = "query"
    KEY = "match"


class MatchBoolPrefix(KeyFieldQueryClause):
    _DEFAULT_PARAM = "query"
    KEY = "match_bool_prefix"


class MatchPhrase(KeyFieldQueryClause):
    _DEFAULT_PARAM = "query"
    KEY = "match_phrase"


class MatchPhrasePrefix(KeyFieldQueryClause):
    _DEFAULT_PARAM = "query"
    KEY = "match_phrase_prefix"


class MultiMatch(MultiFieldsQueryClause):
    KEY = "multi_match"


class Common(KeyFieldQueryClause):
    KEY = "common"


class QueryString(LeafQueryClause):
    # improvement: detect fields for validation
    KEY = "query_string"


class SimpleQueryString(LeafQueryClause):
    # improvement: detect fields for validation
    KEY = "simple_string"
