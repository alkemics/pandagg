from .abstract import LeafQueryClause, SingleFieldQueryClause, MultiFieldsQueryClause


class DistanceFeature(SingleFieldQueryClause):
    FLAT = True
    KEY = 'distance_feature'


class MoreLikeThis(MultiFieldsQueryClause):
    KEY = 'more_like_this'


class Percolate(SingleFieldQueryClause):
    FLAT = True
    KEY = 'percolate'


class RankFeature(SingleFieldQueryClause):
    FLAT = True
    KEY = 'rank_feature'


class Script(LeafQueryClause):
    KEY = 'script'


class Wrapper(LeafQueryClause):
    KEY = 'wrapper'


SPECIALIZED_QUERIES = [
    DistanceFeature,
    MoreLikeThis,
    Percolate,
    RankFeature,
    Script,
    Wrapper
]
