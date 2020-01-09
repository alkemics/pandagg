from builtins import str as text

from .abstract import LeafQueryClause, FieldQueryClause


class Intervals(FieldQueryClause):
    KEY = 'intervals'


class Match(FieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match'


class MatchBoolPrefix(FieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match_bool_prefix'


class MatchPhrase(FieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match_phrase'


class MatchPhrasePrefix(FieldQueryClause):
    SHORT_TAG = 'query'
    KEY = 'match_phrase_prefix'


class MultiMatch(LeafQueryClause):
    KEY = 'multi_match'

    def __init__(self, fields, query, identifier=None, **body):
        self.fields = fields
        b = {'fields': fields, 'query': query}
        b.update(body)
        super(MultiMatch, self).__init__(identifier=identifier, **b)

    @property
    def tag(self):
        return '%s, fields=%s' % (self.KEY, map(text, self.fields))

    @classmethod
    def deserialize(cls, **body):
        return cls(**body)


class Common(FieldQueryClause):
    KEY = 'common'


class QueryString(LeafQueryClause):
    # improvement: detect fields for validation
    KEY = 'query_string'


class SimpleQueryString(LeafQueryClause):
    # improvement: detect fields for validation
    KEY = 'simple_string'
