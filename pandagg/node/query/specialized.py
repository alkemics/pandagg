from .abstract import LeafQueryClause, FlatFieldQueryClause, MultiFieldsQueryClause


class DistanceFeature(FlatFieldQueryClause):
    KEY = "distance_feature"


class MoreLikeThis(MultiFieldsQueryClause):
    KEY = "more_like_this"


class Percolate(FlatFieldQueryClause):
    KEY = "percolate"


class RankFeature(FlatFieldQueryClause):
    KEY = "rank_feature"


class Script(LeafQueryClause):
    KEY = "script"


class Wrapper(LeafQueryClause):
    KEY = "wrapper"
