from .abstract import LeafQueryClause, SingleFieldQueryClause, MultiFieldsQueryClause


class DistanceFeature(SingleFieldQueryClause):
    FLAT = True
    KEY = 'distance_feature'


class MoreLikeThis(MultiFieldsQueryClause):
    KEY = 'more_like_this'


class Percolate(LeafQueryClause):
    KEY = 'percolate'


class RankFeature(SingleFieldQueryClause):
    KEY = 'rank_feature'


class Script(LeafQueryClause):
    KEY = 'script'


class ScriptScore(LeafQueryClause):
    KEY = 'script_score'


# TODO wrapper and pinned query (compound queries)

SPECIALIZED_QUERIES = [
    DistanceFeature,
    MoreLikeThis,
    Percolate,
    RankFeature,
    Script,
    ScriptScore
]
