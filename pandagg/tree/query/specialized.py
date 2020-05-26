from .abstract import Leaf


class DistanceFeature(Leaf):
    KEY = "distance_feature"


class MoreLikeThis(Leaf):
    KEY = "more_like_this"


class Percolate(Leaf):
    KEY = "percolate"


class RankFeature(Leaf):
    KEY = "rank_feature"


class Script(Leaf):
    KEY = "script"


class Wrapper(Leaf):
    KEY = "wrapper"
