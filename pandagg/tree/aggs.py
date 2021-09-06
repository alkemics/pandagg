import json
from typing import Optional, Union, Any, Dict, Tuple

from lighttree import Key, Tree
from lighttree.node import NodeId
from pandagg.node.aggs import Composite
from pandagg.tree._tree import TreeReprMixin
from pandagg.tree.mappings import _mappings, Mappings, MappingsDict

from pandagg.node.aggs.abstract import (
    BucketAggClause,
    Root,
    A,
    TypeOrAgg,
    AggClauseDict,
    AggClause,
)
from pandagg.node.aggs.bucket import Nested, ReverseNested
from pandagg.node.aggs.pipeline import BucketSelector, BucketSort
from pandagg.types import AggName, NamedAggsDict, AfterKey

# {"my_agg": {"terms": "some_field"}} or {"my_agg": Terms(field="some_field")}
AggsDictOrNode = Dict[AggName, Union[AggClauseDict, AggClause]]
AggsOrDict = Union[AggsDictOrNode, "Aggs"]


class Aggs(TreeReprMixin, Tree[AggClause]):
    """
    Combination of aggregation clauses. This class provides handful methods to build an aggregation (see
    :func:`~pandagg.tree.aggs.Aggs.aggs` and :func:`~pandagg.tree.aggs.Aggs.groupby`), and is used as well
    to parse aggregations response in easy to manipulate formats.

    Mappings declaration is optional, but doing so validates aggregation validity and automatically handles missing
    nested clauses.

    Accept following syntaxes:

    from a dict:
    >>> Aggs({"per_user": {"terms": {"field": "user"}}})

    from an other Aggs instance:
    >>> Aggs(Aggs({"per_user": {"terms": {"field": "user"}}}))

    dict with AggClause instances as values:
    >>> from pandagg.aggs import Terms, Avg
    >>> Aggs({'per_user': Terms(field='user')})

    :param mappings: ``dict`` or ``pandagg.tree.mappings.Mappings`` Mappings of requested indice(s). If provided, will
    check aggregations validity.
    :param nested_autocorrect: ``bool`` In case of missing nested clauses in aggregation, if True, automatically
    add missing nested clauses, else raise error. Ignored if mappings are not provided.
    :param _groupby_ptr: ``str`` identifier of aggregation clause used as grouping element (used by `clone` method).
    """

    def __init__(
        self,
        aggs: Optional[AggsOrDict] = None,
        mappings: Optional[Union[MappingsDict, "Mappings"]] = None,
        nested_autocorrect: bool = False,
        _groupby_ptr: Optional[NodeId] = None,
    ) -> None:

        self.mappings: Optional[Mappings] = _mappings(mappings)
        self.nested_autocorrect: bool = nested_autocorrect

        super(Aggs, self).__init__()

        # an Aggs always has a root node, which is just the initial empty dict
        self.root: NodeId
        self.insert_node(Root())

        # identifier of clause used for groupby
        self._groupby_ptr: NodeId = self.root if _groupby_ptr is None else _groupby_ptr

        if aggs is not None:
            self._insert_aggs(aggs, at_root=True)

    def grouped_by(
        self, agg_name: Optional[AggName] = None, deepest: bool = False
    ) -> "Aggs":
        """
        Define which aggregation will be used as grouping pointer.

        Either provide an aggregation name, either specify 'deepest=True' to consider deepest linear eligible
        aggregation node as pointer.
        """
        if agg_name and deepest:
            raise ValueError('Should provide only one of "agg_name" or "deepest".')
        new_agg = self.clone()
        if agg_name:
            nid = new_agg.id_from_key(agg_name)
            if not new_agg._is_eligible_grouping_node(nid):
                raise ValueError("Cannot group by <%s> aggregation" % agg_name)
            new_agg._groupby_ptr = nid
            return new_agg
        if deepest:
            new_agg._groupby_ptr = new_agg._deepest_linear_bucket_agg
            return new_agg
        # no argument was provided, reset pointer to root
        new_agg._groupby_ptr = new_agg.root
        return new_agg

    def groupby(
        self,
        name: AggName,
        type_or_agg: Optional[TypeOrAgg] = None,
        insert_below: Optional[AggName] = None,
        at_root: bool = False,
        **body: Any
    ) -> "Aggs":
        """
        Insert provided aggregation clause in copy of initial Aggs.

        Given the initial aggregation::

            A──> B
            └──> C

        If `insert_below` = 'A'::

            A──> new──> B
                   └──> C

        >>> Aggs().groupby('per_user_id', 'terms', field='user_id')
        {"per_user_id":{"terms":{"field":"user_id"}}}

        >>> Aggs().groupby('per_user_id', {'terms': {"field": "user_id"}})
        {"per_user_id":{"terms":{"field":"user_id"}}}

        >>> from pandagg.aggs import Terms
        >>> Aggs().groupby('per_user_id', Terms(field="user_id"))
        {"per_user_id":{"terms":{"field":"user_id"}}}

        :rtype: pandagg.aggs.Aggs
        """
        new_agg = self.clone()
        insert_below_id = (
            None if insert_below is None else new_agg.id_from_key(insert_below)
        )
        node = A(name, type_or_agg, **body)
        new_agg._insert_agg(
            name=name,
            node=node,
            insert_below_id=insert_below_id,
            at_root=at_root,
            groupby=True,
        )
        return new_agg

    def agg(
        self,
        name: AggName,
        type_or_agg: Optional[TypeOrAgg] = None,
        insert_below: Optional[AggName] = None,
        at_root: bool = False,
        **body: Any
    ) -> "Aggs":
        """
        Insert provided agg clause in copy of initial Aggs.

        Accept following syntaxes for type_or_agg argument:

        string, with body provided in kwargs
        >>> Aggs().agg(name='some_agg', type_or_agg='terms', field='some_field')

        python dict format:
        >>> Aggs().agg(name='some_agg', type_or_agg={'terms': {'field': 'some_field'})

        AggClause instance:
        >>> from pandagg.aggs import Terms
        >>> Aggs().agg(name='some_agg', type_or_agg=Terms(field='some_field'))

        :param name: inserted agg clause name
        :param type_or_agg: either agg type (str), or agg clause of dict format, or AggClause instance
        :param insert_below: name of aggregation below which provided aggs should be inserted
        :param at_root: if True, aggregation is inserted at root
        :param body: aggregation clause body when providing string type_of_agg (remaining kwargs)
        :return: copy of initial Aggs with provided agg inserted
        """
        new_agg = self.clone(with_nodes=True)
        if insert_below is not None:
            insert_below = new_agg.id_from_key(insert_below)
        node = A(name, type_or_agg, **body)
        new_agg._insert_agg(
            name=name, node=node, insert_below_id=insert_below, at_root=at_root
        )
        return new_agg

    def aggs(
        self,
        aggs: Union[AggsDictOrNode, "Aggs"],
        insert_below: Optional[AggName] = None,
        at_root: bool = False,
    ) -> "Aggs":
        """
        Insert provided aggs in copy of initial Aggs.

        Accept following syntaxes for provided aggs:

        python dict format:
        >>> Aggs().aggs({'some_agg': {'terms': {'field': 'some_field'}}, 'other_agg': {'avg': {'field': 'age'}}})

        Aggs instance:
        >>> Aggs().aggs(Aggs({'some_agg': {'terms': {'field': 'some_field'}}, 'other_agg': {'avg': {'field': 'age'}}}))

        dict with Agg clauses values:
        >>> from pandagg.aggs import Terms, Avg
        >>> Aggs().aggs({'some_agg': Terms(field='some_field'), 'other_agg': Avg(field='age')})

        :param aggs: aggregations to insert into existing aggregation
        :param insert_below: name of aggregation below which provided aggs should be inserted
        :param at_root: if True, aggregation is inserted at root
        :return: copy of initial Aggs with provided aggs inserted
        """
        new_agg = self.clone(with_nodes=True)
        if insert_below is not None:
            insert_below = new_agg.id_from_key(insert_below)
        new_agg._insert_aggs(aggs=aggs, insert_below_id=insert_below, at_root=at_root)
        return new_agg

    def to_dict(
        self, from_: Optional[NodeId] = None, depth: Optional[int] = None
    ) -> NamedAggsDict:
        """
        Serialize Aggs as dict.

        :param from_: identifier of aggregation clause, if provided, limits serialization to this clause and its
        children (used for recursion, shouldn't be useful)
        :param depth: integer, if provided, limit the serialization to a given depth
        :return: dict
        """
        from_ = self.root if from_ is None else from_
        _, node = self.get(from_)
        children_queries: NamedAggsDict = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            child_name: str
            # agg name is always a string (even though lighttree accepts other types of keys)
            for child_name, child_node in self.children(  # type: ignore
                node.identifier
            ):
                children_queries[child_name] = self.to_dict(
                    from_=child_node.identifier, depth=depth
                )
        if node.identifier == self.root:
            return children_queries
        node_query_dict = node.to_dict()
        if children_queries:
            node_query_dict["aggs"] = children_queries
        return node_query_dict

    def applied_nested_path_at_node(self, nid: NodeId) -> Optional[str]:
        """
        Return nested path applied at a clause.

        :param nid: clause identifier
        :return: None if no nested is applied, else applied path (str)
        """
        # iterate from provided clause to root clause
        for id_ in self.ancestors_ids(nid, include_current=True):
            _, node = self.get(id_)
            if isinstance(node, (Nested, ReverseNested)):
                return node.path
        return None

    def apply_reverse_nested(self, nid: Optional[NodeId] = None) -> None:
        for k, leaf in self.leaves(nid):
            if isinstance(leaf, BucketAggClause) and self.applied_nested_path_at_node(
                leaf.identifier
            ):
                self.insert_node(
                    ReverseNested(),
                    parent_id=leaf.identifier,
                    key="reverse_nested_%s" % leaf.identifier,
                )

    def show(self, *args: Any, line_max_length: int = 80, **kwargs: Any) -> str:
        """
        Return compact representation of Aggs.

        >>> Aggs({
        >>>     "genres": {
        >>>         "terms": {"field": "genres", "size": 3},
        >>>         "aggs": {
        >>>             "movie_decade": {
        >>>                 "date_histogram": {"field": "year", "fixed_interval": "3650d"}
        >>>             }
        >>>         },
        >>>     }
        >>> }).show()
        <Aggregations>
        genres                                           <terms, field="genres", size=3>
        └── movie_decade          <date_histogram, field="year", fixed_interval="3650d">

        All *args and **kwargs are propagated to `lighttree.Tree.show` method.
        :return: str
        """
        root_children = self.children(self.root)
        if len(root_children) == 0:
            return "<Aggregations> empty"
        if len(root_children) == 1:
            child_id = root_children[0][1].identifier
            return "<Aggregations>\n%s" % str(
                super(Aggs, self).show(
                    child_id, *args, line_max_length=line_max_length, **kwargs
                )  # type: ignore
            )

        return "<Aggregations>\n%s" % str(
            super(Aggs, self).show(
                *args, line_max_length=line_max_length, **kwargs
            )  # type: ignore
        )

    def __nonzero__(self) -> bool:
        return bool(self.to_dict())

    __bool__ = __nonzero__

    def _insert_agg(
        self,
        name: AggName,
        node: AggClause,
        insert_below_id: Optional[NodeId] = None,
        at_root: bool = False,
        groupby: bool = False,
    ) -> None:
        """
        Mutate current Aggs instance (no clone), inserting named AggClause instance at asked location.

        :param name: aggregation name
        :param node: AggClause instance that should be inserted
        :param insert_below_id: if provided, inserted clause is placed below this clause
        :param at_root: boolean, if True inserted clause is placed on top of aggregation
        :param groupby: boolean, if True, move all targeted clause children under inserted
        clause and update groupby pointer
        """
        if insert_below_id and at_root:
            raise ValueError('Must define at most one of "insert_below" or "at_root".')
        if at_root:
            insert_below_id = self.root
        # based on last groupby pointer
        if insert_below_id is None:
            insert_below_id = self._groupby_ptr

        if not isinstance(name, str):
            raise ValueError('Agg "name" must be a str.')

        _children_aggs = node._children or {}

        if groupby:
            if _children_aggs:
                raise ValueError("Cannot group by multiple aggs at once.")
            subs = [
                self.drop_subtree(cid) for cid in self.children_ids(insert_below_id)
            ]
            self.insert(node, key=name, parent_id=insert_below_id)
            for sub_key, sub_tree in subs:
                self.insert(sub_tree, key=sub_key, parent_id=node.identifier)
            # moving pointer when using groupby
            self._groupby_ptr = node.identifier
            return

        # in aggs mode, do not move pointer
        self.insert_node(node=node, key=name, parent_id=insert_below_id)
        for child_name, child in _children_aggs.items():
            child_node = A(child_name, child)
            self._insert_agg(
                name=child_name, node=child_node, insert_below_id=node.identifier
            )

    def _clone_init(self, deep: bool, with_nodes: bool) -> "Aggs":
        return Aggs(
            mappings=self.mappings.clone(deep=deep)
            if self.mappings is not None
            else None,
            nested_autocorrect=self.nested_autocorrect,
            _groupby_ptr=self._groupby_ptr if with_nodes else None,
        )

    def _is_eligible_grouping_node(self, nid: NodeId) -> bool:
        """
        Return whether node can be used as grouping node.
        """
        _, node = self.get(nid)
        if not isinstance(node, BucketAggClause):
            return False
        # special aggregations not returning anything
        if isinstance(node, (BucketSelector, BucketSort)):
            return False
        return True

    @property
    def _deepest_linear_bucket_agg(self) -> NodeId:
        """
        Return deepest bucket aggregation node identifier (pandagg.nodes.abstract.BucketAggClause) of that aggregation
        that neither has siblings, nor has an ancestor with siblings.
        """
        if len(self._nodes_map) <= 1:
            return self.root
        last_bucket_agg_id = self.root
        children = [
            c
            for k, c in self.children(last_bucket_agg_id)
            if self._is_eligible_grouping_node(c.identifier)
        ]
        while len(children) == 1:
            last_agg_node = children[0]
            if not self._is_eligible_grouping_node(last_agg_node.identifier):
                break
            last_bucket_agg_id = last_agg_node.identifier
            children = [
                c
                for k, c in self.children(last_bucket_agg_id)
                if self._is_eligible_grouping_node(c.identifier)
            ]
        return last_bucket_agg_id

    def _insert_aggs(
        self,
        aggs: Union["Aggs", AggsDictOrNode],
        insert_below_id: Optional[NodeId] = None,
        at_root: bool = False,
    ) -> None:
        """
        Insert multiple aggregation clauses in current Aggs (mutate current instance).
        By default place them under groupby pointer if none of `insert_below_id` or `at_root` is provided.

        :param aggs: Aggs instance, or dict
        :param insert_below_id: clause identifier under which inserted aggs should be placed
        :param at_root: if True, place inserted aggs at root, instead of placing them under Aggs._groupby_ptr.
        """
        if at_root:
            insert_below_id = self.root
        elif not insert_below_id:
            # parent based on last groupby pointer
            insert_below_id = self._groupby_ptr

        if aggs is None:
            return
        if isinstance(aggs, Aggs):
            self.merge(aggs, nid=insert_below_id)
            self._groupby_ptr = self.root
            return
        if isinstance(aggs, dict):
            for agg_name, agg_body in aggs.items():
                node = A(agg_name, agg_body)
                self._insert_agg(
                    name=agg_name, node=node, insert_below_id=insert_below_id
                )
            return
        raise TypeError("Unsupported aggs type %s for Aggs" % type(aggs))

    def _insert_node_below(
        self, node: AggClause, parent_id: Optional[NodeId], key: Optional[Key]
    ) -> None:
        """
        If mappings is provided, check if aggregation complies with it (nested / reverse nested).

        Note: overrides `lighttree.Tree._insert_node_below` method to handle automatic nested validation while inserting
        a clause.
        """
        # if aggregation node is explicitly nested or reverse nested aggregation, do not override
        if (
            isinstance(node, Nested)
            or isinstance(node, ReverseNested)
            or not self.mappings
            or not hasattr(node, "field")
            # ignore for root insertion
            or parent_id is None
        ):
            return super(Aggs, self)._insert_node_below(node, parent_id, key)

        # ignore typing warning, since hasattr(node, "field") is checked above
        field: str = node.field  # type: ignore
        # parent_id cannot be null if inserting in a non-empty agg
        parent_id_: str = parent_id

        self.mappings.validate_agg_clause(node)

        # from deepest to highest
        required_nested_level = self.mappings.nested_at_field(field)

        current_nested_level = self.applied_nested_path_at_node(parent_id_)
        if current_nested_level == required_nested_level:
            return super(Aggs, self)._insert_node_below(node, parent_id_, key)
        if not self.nested_autocorrect:
            raise ValueError(
                "Invalid %s agg on %s field. Invalid nested: expected %s, current %s."
                % (node.KEY, field, required_nested_level, current_nested_level)
            )
        if current_nested_level and (
            required_nested_level or "" in current_nested_level
        ):
            # requires reverse-nested
            # check if already exists in direct children, else create it
            child_reverse_nested = next(
                (
                    n
                    for k, n in self.children(parent_id_)
                    if isinstance(n, ReverseNested) and n.path == required_nested_level
                ),
                None,
            )
            if child_reverse_nested:
                return super(Aggs, self)._insert_node_below(
                    node, child_reverse_nested.identifier, key
                )
            else:
                rv_node = ReverseNested()
                super(Aggs, self).insert_node(
                    rv_node,
                    parent_id=parent_id_,
                    key="reverse_nested_below_%s"
                    % (self.get_key(parent_id_) or "root"),
                )
                return super(Aggs, self)._insert_node_below(
                    node, rv_node.identifier, key
                )

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(self.mappings.list_nesteds_at_field(field)):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (
                        n
                        for k, n in self.children(parent_id_)
                        if isinstance(n, Nested) and n.path == nested_lvl
                    ),
                    None,
                )
                if child_nested:
                    parent_id_ = child_nested.identifier
                    continue
                nested_node_name = "nested_below_%s" % (
                    self.get_key(parent_id_) or "root"
                )
                nested_node = Nested(path=nested_lvl)
                super(Aggs, self)._insert_node_below(
                    nested_node, parent_id_, nested_node_name
                )
                parent_id_ = nested_node.identifier
        super(Aggs, self)._insert_node_below(node, parent_id_, key)

    def id_from_key(self, key: str) -> NodeId:
        """
        Find node identifier based on key. If multiple nodes have the same key, takes the first one.

        Useful because of how pandagg implements lighttree.Tree.
        A bit of context:

        ElasticSearch allows queries to contain multiple similarly named clauses (for queries and aggregations).
        As a consequence clauses names are not used as clauses identifier in Trees, and internally pandagg (as lighttree
        ) uses auto-generated uuids to distinguish them.

        But for usability reasons, notably when declaring that an aggregation clause must be placed relatively to
        another one, the latter is identified by its name rather than its internal id. Since it is technically
        possible that multiple clauses share the same name (not recommended, but allowed), some pandagg features are
        ambiguous and not recommended in such context.
        """
        for k, n in self.list():
            if k == key:
                return n.identifier
        raise KeyError('No node found with key "%s"' % key)

    def get_composition_supporting_agg(self) -> Tuple[AggName, AggClause]:
        """
        Return first composite-compatible aggregation clause if possible, raise an error otherwise.
        """
        root_children = self.children(self.root)
        if len(root_children) == 0:
            raise ValueError("No aggregation to convert into composite.")
        if len(root_children) > 1:
            raise ValueError(
                "There can be only one root aggregation clause to be able to convert it into a composite "
                "aggregation."
            )
        first_agg_name: AggName
        first_agg_name, first_agg = root_children[0]  # type: ignore
        if isinstance(first_agg, Composite):
            return first_agg_name, first_agg
        if not first_agg.is_convertible_to_composite_source():
            raise ValueError(
                "<%s> agg clause is not convertible into a composite aggregation."
                % first_agg_name
            )
        return first_agg_name, first_agg

    def as_composite(self, size: int, after: Optional[AfterKey] = None) -> "Aggs":
        """
        Convert current aggregation into composite aggregation.
        For now, simply support conversion of the root aggregation clause, and doesn't handle multi-source.
        """
        agg_name: AggName
        agg_to_convert: AggClause
        agg_name, agg_to_convert = self.get_composition_supporting_agg()

        if isinstance(agg_to_convert, Composite):
            c: Aggs = self.clone(with_nodes=True, deep=True)
            new_c: Composite
            _, new_c = c.get(agg_to_convert.identifier)  # type: ignore

            new_c.body.pop("after", None)
            if after is not None:
                new_c.body["after"] = after
            new_c.body.pop("size", None)
            if size is not None:
                new_c.body["size"] = size
            return c

        _, below_aggs = self.subtree(nid=agg_to_convert.identifier)
        initial_grouping_agg: AggName = self.get_key(self._groupby_ptr)  # type: ignore

        return (
            self.clone(with_nodes=False)
            .groupby(
                agg_name,
                Composite(
                    size=size,
                    sources=[{agg_name: agg_to_convert.to_dict()}],
                    after=after,
                ),
            )
            .aggs(below_aggs)
            .grouped_by(agg_name=initial_grouping_agg)
        )

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=2)
