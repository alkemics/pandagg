
from .abstract import CompoundClause


class Nested(CompoundClause):
    Q_TYPE = 'nested'

    def __init__(self):
        super(Nested, self).__init__()
