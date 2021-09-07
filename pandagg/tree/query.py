import json
from typing import Optional, Union, Any, List, Dict
from typing_extensions import Literal

from lighttree import Key, Tree
from lighttree.node import NodeId
from pandagg._decorators import Substitution
from pandagg.node.query._parameter_clause import ParentParameterClause
from pandagg.node.query.abstract import (
    QueryClause,
    LeafQueryClause,
    Q,
    QueryClauseDict,
    QueryType,
    TypeOrQuery_,
)
from pandagg.node.query.compound import CompoundClause, Bool
from pandagg.node.query.joining import Nested

from pandagg.tree.mappings import _mappings, MappingsDict, Mappings
from pandagg.types import QueryName, ClauseBody

# because a method `bool` shadows the real bool
bool_ = bool
InsertionModes = Literal["add", "replace", "replace_all"]
ADD: InsertionModes = "add"
REPLACE: InsertionModes = "replace"
REPLACE_ALL: InsertionModes = "replace_all"


SingleQueryClause = Union[QueryClauseDict, QueryClause]
SingleOrMultipleQueryClause = Union[SingleQueryClause, List[SingleQueryClause]]

sub_insertion = Substitution(
    location_kwargs="""
    * *insert_below* (``str``) --
      named query clause under which the inserted clauses should be placed.

    * *compound_param* (``str``) --
      param under which inserted clause will be placed in compound query

    * *on* (``str``) --
      named compound query clause on which the inserted compound clause should be merged.

    * *mode* (``str`` one of 'add', 'replace', 'replace_all') --
      merging strategy when inserting clauses on a existing compound clause.

      - 'add' (default) : adds new clauses keeping initial ones
      - 'replace' : for each parameter (for instance in 'bool' case : 'filter', 'must', 'must_not', 'should'),
        replace existing clauses under this parameter, by new ones only if declared in inserted compound query
      - 'replace_all' : existing compound clause is completely replaced by the new one
"""
)

TypeOrQuery = Union[QueryType, QueryClauseDict, QueryClause, "Query"]


class Query(Tree[QueryClause]):
    def __init__(
        self,
        q: Optional[TypeOrQuery] = None,
        mappings: Optional[Union[MappingsDict, Mappings]] = None,
        nested_autocorrect: bool = False,
    ) -> None:
        """
        Combination of query clauses.

        Mappings declaration is optional, but doing so validates query consistency.

        :param q: optional, query (dict, or Query instance)
        :param mappings: ``dict`` or ``pandagg.tree.mappings.Mappings``
        Mappings of requested indice(s). Providing it will add validation features.
        :param nested_autocorrect: add required nested clauses if missing. Ignored if mappings is not provided.
        """
        self.mappings = _mappings(mappings)
        self.nested_autocorrect = nested_autocorrect
        super(Query, self).__init__()
        if q:
            self._insert_query(q)

    @sub_insertion
    def query(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        compound_param: str = None,
        **body: Any
    ) -> "Query":
        r"""
        Insert provided clause in copy of initial Query.

        >>> from pandagg.query import Query
        >>> Query().query('term', some_field=23)
        {'term': {'some_field': 23}}

        >>> from pandagg.query import Term
        >>> Query()\
        >>> .query({'term': {'some_field': 23})\
        >>> .query(Term(other_field=24))\
        {'bool': {'must': [{'term': {'some_field': 23}}, {'term': {'other_field': 24}}]}}

        :Keyword Arguments:
        %(location_kwargs)s
        """
        q: Query = self.clone(with_nodes=True)
        node: QueryClause = self._q(type_or_query, **body)
        q._insert_query_at(
            node,
            mode=mode,
            on=on,
            insert_below=insert_below,
            compound_param=compound_param,
        )
        return q

    @sub_insertion
    def must(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: Optional[ClauseBody] = None,
        **body: Any
    ) -> "Query":
        r"""
        Create copy of initial Query and insert provided clause under "bool" query "must".

        >>> Query().must('term', some_field=1)
        >>> Query().must({'term': {'some_field': 1}})
        >>> from pandagg.query import Term
        >>> Query().must(Term(some_field=1))

        :Keyword Arguments:
        %(location_kwargs)s
        """
        return self._compound_param_insert(
            "bool", "must", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def should(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: Optional[ClauseBody] = None,
        **body: Any
    ) -> "Query":
        return self._compound_param_insert(
            "bool", "should", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def must_not(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Query":
        return self._compound_param_insert(
            "bool", "must_not", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def filter(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Query":
        return self._compound_param_insert(
            "bool", "filter", mode, type_or_query, insert_below, on, bool_body, **body
        )

    # compound
    def bool(
        self,
        must: Optional[SingleOrMultipleQueryClause] = None,
        should: Optional[SingleOrMultipleQueryClause] = None,
        must_not: Optional[SingleOrMultipleQueryClause] = None,
        filter: Optional[SingleOrMultipleQueryClause] = None,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        """
        >>> Query().bool(must={"term": {"some_field": "yolo"}})
        """
        return self.query(
            "bool",
            must=must,
            should=should,
            must_not=must_not,
            filter=filter,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def boosting(
        self,
        positive: Optional[SingleQueryClause] = None,
        negative: Optional[SingleQueryClause] = None,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        if not positive and not negative:
            raise ValueError('Expect at least one of "positive", "negative"')
        return self.query(
            "boosting",
            positive=positive,
            negative=negative,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def constant_score(
        self,
        filter: Optional[SingleQueryClause] = None,
        boost: Optional[float] = None,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        if not filter and not boost:
            raise ValueError('Expect at least one of "filter", "boost"')
        return self.query(
            "constant_score",
            filter=filter,
            boost=boost,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def dis_max(
        self,
        queries: List[SingleQueryClause],
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "dis_max",
            queries=queries,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def function_score(
        self,
        query: Optional[SingleQueryClause],
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "function_score",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def nested(
        self,
        path: str,
        query: Optional[SingleQueryClause] = None,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "nested",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            path=path,
            **body
        )

    def has_child(
        self,
        query: Optional[SingleQueryClause],
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "has_child",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def has_parent(
        self,
        query: Optional[SingleQueryClause],
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "has_parent",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def script_score(
        self,
        query: Optional[SingleQueryClause],
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "script_score",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def pinned_query(
        self,
        organic: Optional[SingleQueryClause],
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Query":
        return self.query(
            "pinned_query",
            organic=organic,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def show(self, *args: Any, line_max_length: int = 80, **kwargs: Any) -> str:
        """
        Return compact representation of Query.

        >>> Query()\
        >>> .must({"exists": {"field": "some_field"}})\
        >>> .must({"term": {"other_field": {"value": 5}}})\
        >>> .show()
        <Query>
        bool
        └── must
            ├── exists                                                  field=some_field
            └── term                                          field=other_field, value=5

        All *args and **kwargs are propagated to `lighttree.Tree.show` method.
        """
        return "<Query>\n%s" % super(Query, self).show(
            *args, line_max_length=line_max_length, **kwargs
        )  # type: ignore

    def applied_nested_path_at_node(self, nid: NodeId) -> Optional[str]:
        """
        Return nested path applied at a clause.

        :param nid: clause identifier
        :return: None if no nested is applied, else applied path (str)
        """
        # from current node to root
        for id_ in self.ancestors_ids(nid, include_current=True):
            _, node = self.get(id_)
            if isinstance(node, Nested):
                return node.path
        return None

    def to_dict(self, from_: Optional[NodeId] = None) -> Optional[QueryClauseDict]:
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        key, node = self.get(from_)
        if isinstance(node, LeafQueryClause):
            return node.to_dict()

        if not isinstance(node, CompoundClause):
            raise ValueError("Unexpected %s" % node.__class__)

        d: Dict[str, Any] = {}
        is_empty = True
        param_key: str
        # in such case, param key is always a string, thus ignore typing
        for param_key, param_node in self.children(node.identifier):  # type: ignore
            children_serialized = [
                self.to_dict(child_node.identifier)
                for _, child_node in self.children(param_node.identifier)
            ]
            children_serialized = [c for c in children_serialized if c]
            if not children_serialized:
                continue
            is_empty = False
            if not param_node.MULTIPLE:
                d[param_key] = children_serialized[0]
                continue
            d[param_key] = children_serialized
        # a compound query clause can exist, without children clauses, in that case, just ignore it
        if is_empty:
            return None
        q = node.to_dict()
        q[node.KEY].update(d)
        return q

    # compound parameters
    def _compound_param_insert(
        self,
        compound_key: str,
        compound_param_key: str,
        mode: InsertionModes,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        compound_body: ClauseBody = None,
        **body: Any
    ) -> "Query":
        q = self.clone(with_nodes=True)
        node = self._q(type_or_query, **body)
        compound_body = compound_body or {}
        compound_body[compound_param_key] = node
        compound_node = QueryClause.get_dsl_class(compound_key)(**compound_body)
        q._insert_query_at(compound_node, on=on, insert_below=insert_below, mode=mode)
        return q

    def __nonzero__(self) -> bool_:
        return bool(self.to_dict())

    __bool__ = __nonzero__

    def _clone_init(self, deep: bool_, with_nodes: bool_) -> "Query":
        return Query(
            mappings=None
            if self.mappings is None
            else self.mappings.clone(with_nodes=True, deep=deep),
            nested_autocorrect=self.nested_autocorrect,
        )

    def _has_bool_root(self) -> bool_:
        if not self.root:
            return False
        _, r = self.get(self.root)
        return isinstance(r, Bool)

    def _compound_param_id(
        self, nid: NodeId, key: str, create_if_not_exists: bool_ = True
    ) -> str:
        """
        :param nid: id of compound node
        :param key: param key, for instance if compound if bool, can be 'must', 'should', 'must_not' etc
        :return: param node id
        """
        try:
            return self.child_id(nid, key)
        except ValueError:
            if not create_if_not_exists:
                raise
        # add key
        param_node = QueryClause.get_dsl_class(key)()
        self._insert_node_below(param_node, parent_id=nid, key=key)
        return param_node.identifier

    @classmethod
    def _q(cls, type_or_query: TypeOrQuery, **body: Any) -> QueryClause:
        """
        Convert to QueryClause instance.
        """
        type_or_query_: Optional[TypeOrQuery_]
        if isinstance(type_or_query, Query):
            type_or_query_ = type_or_query.to_dict()
        else:
            type_or_query_ = type_or_query
        return Q(type_or_query_, **body)

    def _insert_query_at(
        self,
        node: QueryClause,
        mode: InsertionModes,
        on: Optional[QueryName] = None,
        insert_below: Optional[QueryName] = None,
        compound_param: str = None,
    ) -> None:
        """
        Insert clause (and its children) in Query.

        If compound query with on specified: merge according to mode.
        If insert_below is not specified, place on top (wrapped in bool-must if necessary).
        If insert_below is provided (only under compound query): place under it.
        """
        if mode not in (ADD, REPLACE, REPLACE_ALL):
            raise ValueError("Invalid mode %s" % mode)

        if (
            isinstance(node, Bool)
            and not on
            and not insert_below
            and self._has_bool_root()
        ):
            on = self.root

        if isinstance(node, CompoundClause) and on:
            # ensure we try to merge on same type of clause
            _, existing = self.get(on)
            if existing.KEY != node.KEY:
                raise ValueError(
                    "Cannot merge compound clause %s on %s. Must be the same."
                    % (node.KEY, existing.KEY)
                )
            if mode == REPLACE_ALL:
                pid = None if on == self.root else self.parent_id(on)
                existing_k, _ = self.drop_subtree(on)
                self._insert_query(node, insert_below=pid)
                existing.body = node.body
                return

            # merge
            existing.body.update(node.body)
            for param_key, children in node._children.items():
                if not children:
                    continue
                param_id = self._compound_param_id(on, param_key)
                # here, possible modes are either ADD, or REPLACE
                if mode == REPLACE:
                    existing_clauses_ids = self.children_ids(param_id)
                    for eid in existing_clauses_ids:
                        self.drop_node(eid)
                for child in children:
                    self._insert_query(child, insert_below=param_id)
            return

        if insert_below:
            # below node, with compound_param
            _, pnode = self.get(insert_below)
            if not isinstance(pnode, CompoundClause):
                raise ValueError(
                    "Cannot insert clause below %s clause (only compound clauses can have children clauses)."
                    % pnode.KEY
                )
            compound_param = compound_param or pnode._default_operator
            if compound_param not in pnode._parent_params:
                raise ValueError(
                    "<%s> parameter for <%s> compound clause does not accept children clauses."
                    % (compound_param, pnode.KEY)
                )
            param_id = self._compound_param_id(insert_below, compound_param)
            _, p = self.get(param_id)
            if not p.MULTIPLE:
                # inserting a clause, under a parameter allowing a single clause (for instance below nested query)
                cids = self.children_ids(param_id)
                if cids:
                    # can be at most one
                    cid = cids[0]
                    # must place existing clause, and new one under a bool -> must
                    _, existing_child = self.get(cid)
                    if isinstance(existing_child, Bool):
                        child_param_id = self._compound_param_id(
                            existing_child.identifier, "must"
                        )
                        self._insert_query(node, insert_below=child_param_id)
                        return
                    _, existing_child = self.drop_node(cid)
                    self._insert_query(
                        Bool(must=[existing_child, node]), insert_below=param_id
                    )
                    return
            self._insert_query(node, insert_below=param_id)
            return

        # from now on: position was not provided:
        if self.is_empty():
            # currently empty (-> on top)
            self._insert_query(node)
            return

        # ignore typing, in this case root cannot be None
        root: str = self.root  # type: ignore
        if self._has_bool_root():
            # top query is bool
            must_id = self._compound_param_id(root, "must")
            self._insert_query(node, insert_below=must_id)
            return

        # top query is not bool
        _, initial_query = self.drop_subtree(root)
        if isinstance(node, Bool):
            # if inserted node is bool, placed on top, and previous query is added under "must" parameter
            self._insert_query(node)
            self._insert_query_at(
                initial_query.to_dict(), insert_below=node.identifier, mode=ADD
            )
        else:
            # we place initial + added clauses under a bool>must
            self._insert_query(Bool(must=[node, initial_query.to_dict()]))
        return

    def _insert_query(
        self, query: TypeOrQuery, insert_below: Optional[QueryName] = None
    ) -> None:
        """
        Insert query clause and its children (recursively).
        Does not handle logic about where to insert it, should be handled before (dumb insert below insert_below).
        """
        node = self._q(query)
        self.insert_node(node, parent_id=insert_below)

        if not isinstance(node, CompoundClause):
            return

        _children_clauses = node._children.copy()
        for param_name, child_nodes in _children_clauses.items():
            param_node = QueryClause.get_dsl_class(param_name)()
            if not param_node.MULTIPLE and len(child_nodes) > 1:
                raise ValueError(
                    "Cannot insert multiple query clauses under %s parameter"
                    % param_name
                )
            self.insert_node(param_node, parent_id=node.identifier, key=param_name)
            for child in child_nodes:
                self._insert_query(query=child, insert_below=param_node.identifier)

    def _insert_node_below(
        self, node: QueryClause, parent_id: Optional[NodeId], key: Optional[Key]
    ) -> None:
        """
        Override lighttree.Tree._insert_node_below method to ensure inserted query clause is consistent (for instance
        only compounds queries can have children clauses).
        If mappings are provided, ensure that nested fields are properly handled. If nested_autocorrect is set to True
        at __init__, automatically add it if necessary.

        Note: automatic handling can be ambiguous in case of multiple nested clauses, ie should it operate a must clause
        at root document level, or at nested level. Example: difference between "a car with a rectangular window, and a
        blue window" (can be different windows), and "a car with a rectangular and blue window" (same window must hold
        same characteristics).
        """
        if parent_id is not None:
            _, pnode = self.get(parent_id)
            if isinstance(pnode, LeafQueryClause):
                raise ValueError(
                    "Cannot add clause under leaf query clause <%s>" % pnode.KEY
                )
            if isinstance(pnode, ParentParameterClause):
                if isinstance(node, ParentParameterClause):
                    raise ValueError(
                        "Cannot add parameter clause <%s> under another parameter clause <%s>"
                        % (pnode.KEY, node.KEY)
                    )
            if isinstance(pnode, CompoundClause):
                if key not in pnode._parent_params:
                    raise ValueError(
                        "Expect a parameter clause of type %s under <%s> compound clause, got <%s>"
                        % (pnode._parent_params, pnode.KEY, key)
                    )

        # no mappings validation
        if isinstance(node, Nested) or not self.mappings or not hasattr(node, "field"):
            return super(Query, self)._insert_node_below(
                node=node, parent_id=parent_id, key=key
            )

        # ignore type because cannot be None
        field: str = node.field  # type: ignore

        # automatic handling of nested clauses
        required_nested_level = self.mappings.nested_at_field(field)
        if self.is_empty() or parent_id is None:
            current_nested_level = None
        else:
            current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Query, self)._insert_node_below(
                node=node, parent_id=parent_id, key=key
            )
        if not self.nested_autocorrect:
            raise ValueError(
                "Invalid %s query clause on %s field. Invalid nested: expected %s, current %s."
                % (node.KEY, field, required_nested_level, current_nested_level)
            )
        # requires nested - apply all required nested fields
        to_insert = node
        for nested_lvl in self.mappings.list_nesteds_at_field(field):
            if current_nested_level != nested_lvl:
                to_insert = QueryClause.get_dsl_class("nested")(
                    path=nested_lvl, query=to_insert
                )
        self._insert_query(to_insert, parent_id)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2)
