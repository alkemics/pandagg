from collections import OrderedDict, defaultdict
from typing import Optional, List, Set, Dict

from lighttree import Tree
from lighttree.node import NodeId

from pandagg.node.aggs.abstract import AggClause
from pandagg.node.query.joining import Nested
from pandagg.node.response.bucket import Bucket, RootBucket
from pandagg.tree._tree import TreeReprMixin
from pandagg.tree.aggs import Aggs
from pandagg.tree.query import Query
from pandagg.types import AggName, QueryClauseDict, AggregationsResponseDict


class AggsResponseTree(TreeReprMixin, Tree[Bucket]):
    """
    Tree shaped representation of an ElasticSearch aggregations response.

    Note: this class is only used for interactive features, and is currently still experimental. Notably it lacks tests.
    """

    def __init__(
        self, aggs: Aggs, raw_response: Optional[AggregationsResponseDict] = None
    ) -> None:
        super(AggsResponseTree, self).__init__()
        self.__aggs: Aggs = aggs

        self.root: str
        root_node = RootBucket()
        self.insert_node(root_node)
        if raw_response:
            self.parse(raw_response)

    def parse(self, raw_response: AggregationsResponseDict) -> "AggsResponseTree":
        """
        Build response tree from ElasticSearch aggregation response
        """
        _, agg_root_node = self.__aggs.get(self.__aggs.root)
        for child_name, child in self.__aggs.children(agg_root_node.identifier):
            self._parse_node_with_children(
                # ignore warning about child_name not necessarily being a string, in our case it is
                agg_name=child_name,  # type: ignore
                agg_node=child,
                raw_response=raw_response,
                pid=self.root,
            )
        return self

    def bucket_properties(
        self,
        bucket: Bucket,
        properties: Optional[OrderedDict] = None,
        end_level: Optional[AggName] = None,
        depth: Optional[int] = None,
    ) -> OrderedDict:
        """
        Recursive method returning a given bucket's properties in the form of an ordered dictionnary.
        Travel from current bucket through all ancestors until reaching root.

        :param bucket: instance of pandagg.buckets.buckets.Bucket
        :param properties: OrderedDict accumulator of 'level' -> 'key'
        :param end_level: optional parameter to specify until which level properties are fetched
        :param depth: optional parameter to specify a limit number of levels which are fetched
        :return: OrderedDict of structure 'level' -> 'key'
        """
        if properties is None:
            properties = OrderedDict()
        if bucket.level is not None:
            properties[bucket.level] = bucket.key
        if depth is not None:
            depth -= 1
        if bucket.identifier == self.root or bucket.level == end_level or depth == 0:
            return properties
        _, parent = self.parent(bucket.identifier)
        return self.bucket_properties(parent, properties, end_level, depth)

    def get_bucket_filter(self, nid: NodeId) -> Optional[QueryClauseDict]:
        """
        Build query filtering documents belonging to that bucket.
        Suppose the following configuration::

            Base                        <- filter on base
              |── Nested_A                 no filter on A (nested still must be applied for children)
              |     |── SubNested A1
              |     └── SubNested A2    <- filter on A2
              └── Nested_B              <- filter on B

        """
        tree_mapping = self.__aggs.mappings

        b_key, selected_bucket = self.get(nid)
        bucket_properties = self.bucket_properties(selected_bucket)
        agg_node_key_tuples = [
            (self.__aggs.get(self.__aggs.id_from_key(level))[1], key)
            for level, key in bucket_properties.items()
        ]

        filters_per_nested_level: Dict[
            Optional[str], List[QueryClauseDict]
        ] = defaultdict(list)

        for agg_node, key in agg_node_key_tuples:
            level_agg_filter = agg_node.get_filter(key)
            # remove unnecessary match_all filters
            if level_agg_filter is not None and "match_all" not in level_agg_filter:
                current_nested = self.__aggs.applied_nested_path_at_node(
                    agg_node.identifier
                )
                filters_per_nested_level[current_nested].append(level_agg_filter)

        nested_with_conditions = [n for n in filters_per_nested_level.keys() if n]

        if tree_mapping is None:
            return self._build_filter({}, filters_per_nested_level).to_dict()

        all_nesteds = [
            n.identifier
            for _, n in tree_mapping.list(
                filter_=lambda x: (x.KEY == "nested")
                and any((i in x.identifier or "" for i in nested_with_conditions))
            )
        ]

        nid_to_children: Dict[NodeId, Set[str]] = defaultdict(set)
        for nested in all_nesteds:
            nested_with_parents = [
                n for _, n in tree_mapping.ancestors(nid=nested) if n.KEY == "nested"
            ]
            nearest_nested_parent = next(iter(nested_with_parents[1:]), None)
            if nearest_nested_parent is None:
                continue
            nid_to_children[nearest_nested_parent.identifier].add(nested)
        return self._build_filter(nid_to_children, filters_per_nested_level).to_dict()

    def _clone_init(self, deep: bool, with_nodes: bool) -> "AggsResponseTree":
        return AggsResponseTree(aggs=self.__aggs.clone(deep=deep))

    def _parse_node_with_children(
        self,
        agg_name: AggName,
        agg_node: AggClause,
        raw_response: AggregationsResponseDict,
        pid: NodeId,
    ) -> None:
        """
        Recursive method to parse ES raw response.

        :param agg_node: current aggregation
        :param raw_response: ES response at current level
        :param pid: parent node identifier
        """
        agg_raw_response = raw_response.get(agg_name)
        if not agg_raw_response:
            return None
        for key, raw_value in agg_node.extract_buckets(agg_raw_response):
            bucket = Bucket(
                level=agg_name, key=key, value=agg_node.extract_bucket_value(raw_value)
            )
            self.insert_node(bucket, parent_id=pid)
            for child_name, child in self.__aggs.children(agg_node.identifier):
                self._parse_node_with_children(
                    # ignore typing error about child_name being possibly not a str, it is
                    agg_name=child_name,  # type: ignore
                    agg_node=child,
                    raw_response=raw_value,
                    pid=bucket.identifier,
                )

    @classmethod
    def _build_filter(
        cls,
        nid_to_children: Dict[NodeId, Set[str]],
        filters_per_nested_level: Dict[Optional[str], List[QueryClauseDict]],
        current_nested_path: Optional[str] = None,
    ) -> Query:
        """
        Recursive function to build bucket filters from highest to deepest nested conditions."""
        current_conditions = filters_per_nested_level.get(current_nested_path, [])
        # TODO - test this method
        nested_children = nid_to_children[current_nested_path]  # type: ignore
        for nested_child in nested_children:
            nested_child_conditions = cls._build_filter(
                nid_to_children=nid_to_children,
                filters_per_nested_level=filters_per_nested_level,
                current_nested_path=nested_child,
            )
            if nested_child_conditions:
                current_conditions.append(
                    Nested(path=nested_child, query=nested_child_conditions).to_dict()
                )
        q = Query()
        for clause in current_conditions:
            q = q.query(clause)
        return q
