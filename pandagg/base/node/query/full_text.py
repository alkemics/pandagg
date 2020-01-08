from six import iteritems

from .abstract import FieldQueryClause


class Intervals(FieldQueryClause):
    KEY = 'intervals'


class Match(FieldQueryClause):
    KEY = 'match'

    @classmethod
    def deserialize(cls, **body):
        assert len(body.keys()) == 1
        k, v = next(iteritems(body))
        if isinstance(v, dict):
            return cls(field=k, **v)
        return cls(field=k, query=v)


class MatchBoolPrefix(FieldQueryClause):
    KEY = 'match_bool_prefix'


class MatchPhrase(FieldQueryClause):
    KEY = 'match_phrase'


class MatchPhrasePrefix(FieldQueryClause):
    KEY = 'match_phrase_prefix'


class MultiMatch(FieldQueryClause):
    KEY = 'multi_match'


class Common(FieldQueryClause):
    KEY = 'common'


class QueryString(FieldQueryClause):
    KEY = 'query_string'


class SimpleString(FieldQueryClause):
    KEY = 'simple_string'
