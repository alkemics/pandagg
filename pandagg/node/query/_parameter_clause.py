from pandagg.node.query.abstract import ParentParameterClause


class _Filter(ParentParameterClause):
    KEY = "filter"
    MULTIPLE = True


class _Must(ParentParameterClause):
    KEY = "must"
    MULTIPLE = True


class _MustNot(ParentParameterClause):
    KEY = "must_not"
    MULTIPLE = True


class _Negative(ParentParameterClause):
    KEY = "negative"
    MULTIPLE = False


class _Organic(ParentParameterClause):
    KEY = "organic"
    MULTIPLE = False


class _Positive(ParentParameterClause):
    KEY = "positive"
    MULTIPLE = False


class _Queries(ParentParameterClause):
    KEY = "queries"
    MULTIPLE = True


class _Query(ParentParameterClause):
    KEY = "query"
    MULTIPLE = False


class _Should(ParentParameterClause):
    KEY = "should"
    MULTIPLE = True
