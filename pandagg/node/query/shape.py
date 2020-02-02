from .abstract import LeafQueryClause


class Shape(LeafQueryClause):
    KEY = 'shape'


SHAPE_QUERIES = [
    Shape
]
