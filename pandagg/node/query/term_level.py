from typing import Optional, Any, Tuple, List, Union

from .abstract import (
    LeafQueryClause,
    AbstractSingleFieldQueryClause,
    KeyFieldQueryClause,
)
from pandagg.types import QueryName


class Exists(LeafQueryClause):
    KEY = "exists"

    def __init__(self, field: str, _name: Optional[QueryName] = None) -> None:
        self.field: str = field
        super(Exists, self).__init__(_name=_name, field=field)

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return self.KEY, "field=%s" % self.field


class Fuzzy(KeyFieldQueryClause):
    KEY = "fuzzy"
    _implicit_param = "value"


class Ids(LeafQueryClause):
    KEY = "ids"

    def __init__(
        self, values: List[Union[int, str]], _name: Optional[QueryName] = None
    ) -> None:
        self.field: str = "id"
        self.values: List[Union[int, str]] = values

        super(Ids, self).__init__(_name=_name, values=values)

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return self.KEY, "values=%s" % self.values


class Prefix(KeyFieldQueryClause):
    KEY = "prefix"
    _implicit_param = "value"


class Range(KeyFieldQueryClause):
    KEY = "range"


class Regexp(KeyFieldQueryClause):
    KEY = "regexp"
    _implicit_param = "value"


class Term(KeyFieldQueryClause):
    KEY = "term"
    _implicit_param = "value"


class Terms(AbstractSingleFieldQueryClause):
    KEY = "terms"

    def __init__(self, **body: Any) -> None:
        _name: Optional[str] = body.pop("_name", None)
        boost: Optional[float] = body.pop("boost", None)
        if len(body) != 1:
            raise ValueError("Wrong declaration: %s" % body)
        field, terms = self.expand__to_dot(body).popitem()
        b = {field: terms}
        if boost is not None:
            b["boost"] = boost
        super(Terms, self).__init__(_name=_name, field=field, **b)


class TermsSet(KeyFieldQueryClause):
    KEY = "terms_set"
    _implicit_param = "terms"


class Type(KeyFieldQueryClause):
    KEY = "type"
    _implicit_param = "value"


class Wildcard(KeyFieldQueryClause):
    KEY = "wildcard"
    _implicit_param = "value"
