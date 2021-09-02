from __future__ import annotations

import copy
import dataclasses
from typing_extensions import Literal, TypedDict
from typing import (
    Iterator,
    Optional,
    List,
    TYPE_CHECKING,
    Dict,
    Tuple,
    Union,
    overload,
    Any,
)

from elasticsearch import Elasticsearch
from lighttree.node import NodeId

from pandagg.query import Query
from pandagg.aggs import Aggs, Composite
from pandagg.interactive.response import IResponse
from pandagg.node.aggs.abstract import UniqueBucketAgg, MetricAgg, Root, AggClause
from pandagg.node.aggs.bucket import Nested, ReverseNested
from pandagg.tree.response import AggsResponseTree
from pandagg.types import (
    HitDict,
    HitsDict,
    DocSource,
    TotalDict,
    SearchResponseDict,
    ShardsDict,
    AggregationsResponseDict,
    AggName,
    ProfileDict,
    BucketDict,
    BucketKey,
    CompositeBucketKey,
    BucketKeyAtom,
)

if TYPE_CHECKING:
    import pandas as pd
    from pandagg.search import Search


GroupingKeysDict = Dict[AggName, BucketKeyAtom]
GroupingKeysTuple = Tuple[BucketKeyAtom, ...]

RowValues = Dict[str, Any]
# dictionary containing both grouping keys and values
Row = Dict[str, Any]


class NormalizedBucketDict(TypedDict, total=False):
    level: AggName
    key: BucketKey
    value: Any
    # children are themselves NormalizedBucketDict, but mypy doesn't support recursive types
    children: List[Any]


@dataclasses.dataclass
class Hit:
    data: HitDict

    @property
    def _source(self) -> Optional[DocSource]:
        return self.data.get("_source")

    @property
    def _score(self) -> Optional[float]:
        return self.data.get("_score")

    @property
    def _id(self) -> Optional[str]:
        return self.data.get("_id")

    @property
    def _index(self) -> Optional[str]:
        return self.data.get("_index")

    def __repr__(self) -> str:
        if self._score is None:
            return "<Hit %s>" % self._id
        return "<Hit %s> score=%.2f" % (self._id, self._score)


@dataclasses.dataclass
class Hits:
    data: Optional[HitsDict]

    @property
    def total(self) -> Optional[TotalDict]:
        return self.data.get("total") if self.data else None

    @property
    def hits(self) -> List[Hit]:
        return [Hit(hit) for hit in self.data.get("hits", [])] if self.data else []

    @property
    def max_score(self) -> Optional[float]:
        return self.data.get("max_score") if self.data else None

    def __len__(self) -> int:
        return len(self.hits)

    def __iter__(self) -> Iterator[Hit]:
        return iter(self.hits)

    def _total_repr(self) -> str:
        if self.total is None:
            return 'Unknown total (probably filtered by "filter_path")'
        if self.total.get("relation") == "eq":
            return str(self.total["value"])
        if self.total.get("relation") == "gte":
            return ">=%d" % self.total["value"]
        raise ValueError("Invalid total %s" % self.total)

    def to_dataframe(
        self, expand_source: bool = True, source_only: bool = True
    ) -> pd.DataFrame:
        """
        Return hits as pandas dataframe.
        Requires pandas dependency.
        :param expand_source: if True, `_source` sub-fields are expanded as columns
        :param source_only: if True, doesn't include hit metadata (except id which is used as dataframe index)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'Using dataframe output format requires to install pandas. Please install "pandas" or '
                "use another output format."
            )
        hits: List[HitDict] = self.data.get("hits", []) if self.data else []
        if not hits:
            return pd.DataFrame()
        if not expand_source:
            return pd.DataFrame(hits).set_index("_id")

        flattened_hits: List[DocSource] = []

        hit: HitDict
        for hit in hits:
            hit_metadata: HitDict = hit.copy()
            hit_source: DocSource = hit_metadata.pop("_source")
            if source_only:
                hit_source["_id"] = hit_metadata["_id"]
            else:
                hit_source.update(hit_metadata)
            flattened_hits.append(hit_source)
        return pd.DataFrame(flattened_hits).set_index("_id")

    def __repr__(self) -> str:
        if not isinstance(self.total, dict):
            total_repr = str(self.total)
        elif self.total.get("relation") == "eq":
            total_repr = str(self.total["value"])
        elif self.total.get("relation") == "gte":
            total_repr = ">%d" % self.total["value"]
        else:
            raise ValueError("Invalid total %s" % self.total)
        return "<Hits> total: %s, contains %d hits" % (total_repr, len(self.hits))


@dataclasses.dataclass
class SearchResponse:

    data: SearchResponseDict
    _search: Search

    @property
    def took(self) -> Optional[int]:
        return self.data.get("took")

    @property
    def timed_out(self) -> Optional[int]:
        return self.data.get("timed_out")

    @property
    def _shards(self) -> Optional[ShardsDict]:
        return self.data.get("_shards")

    @property
    def hits(self) -> Hits:
        return Hits(self.data.get("hits"))

    @property
    def aggregations(self) -> Aggregations:
        return Aggregations(self.data.get("aggregations", {}), _search=self._search)

    @property
    def profile(self) -> Optional[ProfileDict]:
        return self.data.get("profile")

    def __iter__(self) -> Iterator[Hit]:
        return iter(self.hits)

    @property
    def success(self) -> bool:
        if (
            self._shards is None
            or self._shards.get("total") is None
            or self._shards.get("successful") is None
        ):
            # if total result is filtered by 'filter_path', ignore
            return False
        return (
            self._shards["total"] == self._shards["successful"] and not self.timed_out
        )

    def __len__(self) -> int:
        return len(self.hits)

    def __repr__(self) -> str:
        return (
            "<Response> took %sms, success: %s, total result %s, contains %s hits"
            % (self.took, self.success, self.hits._total_repr(), len(self.hits))
        )


@dataclasses.dataclass
class Aggregations:
    data: AggregationsResponseDict
    _search: Search

    @property
    def _aggs(self) -> Aggs:
        return self._search._aggs

    @property
    def _query(self) -> Query:
        return self._search._query

    @property
    def _client(self) -> Optional[Elasticsearch]:
        return self._search._using

    @property
    def _index(self) -> Optional[List[str]]:
        return self._search._index

    def keys(self) -> List[AggName]:
        return list(self.data.keys())

    @overload
    def parse_group_by(
        self,
        *,
        response: AggregationsResponseDict,
        until: Optional[AggName],
        with_single_bucket_groups: bool = False,
        row_as_tuple: Literal[True],
    ) -> Tuple[List[AggName], List[Tuple[GroupingKeysTuple, BucketDict]]]:
        ...

    @overload
    def parse_group_by(
        self,
        *,
        response: AggregationsResponseDict,
        until: Optional[AggName],
        with_single_bucket_groups: bool = False,
        row_as_tuple: Literal[False],
    ) -> Tuple[List[AggName], List[Tuple[GroupingKeysDict, BucketDict]]]:
        ...

    def parse_group_by(
        self,
        *,
        response: AggregationsResponseDict,
        until: Optional[AggName],
        with_single_bucket_groups: bool = False,
        row_as_tuple: bool = False,
    ) -> Tuple[
        List[AggName],
        Union[
            List[Tuple[GroupingKeysTuple, BucketDict]],
            List[Tuple[GroupingKeysDict, BucketDict]],
        ],
    ]:

        if not until:
            index_names_: List[AggName] = []
            if row_as_tuple:
                r_: List[Tuple[GroupingKeysTuple, BucketDict]] = [(tuple(), response)]
                return index_names_, r_
            r__: GroupingKeysDict = {}
            return index_names_, [(r__, response)]

        # initialization: cache ancestors once for faster computation
        until_id: NodeId = self._aggs.id_from_key(until)
        # remove root (not an aggregation clause), ignore type warning about key None (since only root
        # can have a None key)
        ancestors: List[Tuple[AggName, AggClause]] = [
            (k, n)  # type: ignore
            for k, n in self._aggs.ancestors(
                until_id, include_current=True, from_root=True
            )[1:]
        ]

        if not ancestors:
            index_names__: List[AggName] = []
            if row_as_tuple:
                r___: List[Tuple[GroupingKeysTuple, BucketDict]] = [(tuple(), response)]
                return index_names__, r___
            r____: GroupingKeysDict = {}
            return index_names__, [(r____, response)]

        # from root aggregation to deepest aggregation clause
        index_names: List[AggName] = []

        for name, a in ancestors:
            if isinstance(a, UniqueBucketAgg) and not with_single_bucket_groups:
                continue
            if isinstance(a, Composite):
                # a composite aggregation can generate multiple grouping columns
                index_names.extend(a.source_names)
            else:
                index_names.append(name)

        first_agg_name: AggName
        first_agg_name, _ = ancestors[0]

        index_values: List[Tuple[GroupingKeysDict, BucketDict]] = list(
            self._parse_group_by(
                response=response,
                until=until,
                agg_clauses_per_name={k: a for k, a in ancestors},
                agg_name=first_agg_name,
                row={},
                with_single_bucket_groups=with_single_bucket_groups,
            )
        )
        if not row_as_tuple:
            return index_names, index_values
        values_: List[Tuple[GroupingKeysTuple, BucketDict]] = [
            (tuple(grouping_row[index_name] for index_name in index_names), raw_bucket)
            for grouping_row, raw_bucket in index_values
        ]
        return index_names, values_

    def _parse_group_by(
        self,
        response: AggregationsResponseDict,
        until: AggName,
        row: GroupingKeysDict,
        agg_name: AggName,
        agg_clauses_per_name: Dict[AggName, AggClause],
        with_single_bucket_groups: bool,
    ) -> Iterator[Tuple[GroupingKeysDict, BucketDict]]:
        """
        Recursive parsing of succession of grouping aggregation clauses.

        Yields each row for which last bucket aggregation generated buckets.
        """
        if agg_name not in response:
            return None

        agg_node = agg_clauses_per_name[agg_name]

        key: BucketKey
        raw_bucket: BucketDict

        for key, raw_bucket in agg_node.extract_buckets(response[agg_name]):
            sub_row: GroupingKeysDict = copy.copy(row)
            if not isinstance(agg_node, UniqueBucketAgg) or with_single_bucket_groups:
                if isinstance(agg_node, Composite):
                    key_: CompositeBucketKey = key  # type: ignore
                    for source_name in agg_node.source_names:
                        sub_row[source_name] = key_[source_name]
                else:
                    key__: BucketKeyAtom = key  # type: ignore
                    sub_row[agg_name] = key__
            if agg_name == until:
                # end real yield
                yield sub_row, raw_bucket
            elif agg_name in agg_clauses_per_name.keys():
                # yield children
                child_name: AggName
                for child_name, _ in self._aggs.children(  # type: ignore
                    agg_node.identifier
                ):
                    for nrow, nraw_bucket in self._parse_group_by(
                        row=sub_row,
                        response=raw_bucket,
                        agg_name=child_name,
                        until=until,
                        agg_clauses_per_name=agg_clauses_per_name,
                        with_single_bucket_groups=with_single_bucket_groups,
                    ):
                        yield nrow, nraw_bucket

    def _normalize_buckets(
        self, agg_response: AggregationsResponseDict, agg_name: AggName
    ) -> Iterator[NormalizedBucketDict]:
        """
        Recursive function to parse aggregation response as a normalized entities.
        Each response bucket is represented as a dict with keys (key, level, value, children)::

            {
                "level": "owner.id",
                "key": 35,
                "value": 235,
                "children": [
                ]
            }
        """
        id_: NodeId = self._aggs.id_from_key(agg_name)
        _, agg_node = self._aggs.get(id_)

        agg_children = self._aggs.children(id_)

        key: BucketKey
        raw_bucket: BucketDict
        for key, raw_bucket in agg_node.extract_buckets(agg_response[agg_name]):
            result: NormalizedBucketDict = {
                "level": agg_name,
                "key": key,
                "value": agg_node.extract_bucket_value(raw_bucket),
            }
            child_key: AggName
            normalized_children: List[NormalizedBucketDict] = [
                normalized_child
                for child_key, child in agg_children
                for normalized_child in self._normalize_buckets(
                    # ignore warning about child_key not being necessarily a AggName (str), it is
                    agg_name=child_key,  # type: ignore
                    agg_response=raw_bucket,
                )
            ]
            if normalized_children:
                result["children"] = normalized_children
            yield result

    def _grouping_agg(
        self, name: Optional[AggName] = None
    ) -> Tuple[Optional[AggName], AggClause]:
        """
        Return aggregation node that used as grouping node.
        Note: in case there is only a nested aggregation below that node, group-by nested clause.
        """
        key: str
        if name is not None:
            # override existing groupby_ptr
            id_ = self._aggs.id_from_key(name)
            if not self._aggs._is_eligible_grouping_node(id_):
                raise ValueError(
                    "Cannot group by <%s>, not a valid grouping aggregation" % name
                )
            key, node = self._aggs.get(id_)  # type: ignore
        else:
            key, node = self._aggs.get(self._aggs._groupby_ptr)  # type: ignore

        # if parent of single nested clause and nested_autocorrect
        if self._aggs.nested_autocorrect:
            children = self._aggs.children(node.identifier)
            if len(children) == 1:
                child_key: str
                child_key, child_node = children[0]  # type: ignore
                if isinstance(child_node, (Nested, ReverseNested)):
                    return child_key, child_node
        return key, node

    @overload
    def to_tabular(
        self,
        *,
        index_orient: Literal[True] = True,
        grouped_by: Optional[AggName] = None,
        expand_columns: bool = True,
        expand_sep: str = "|",
        normalize: bool = True,
        with_single_bucket_groups: bool = False,
    ) -> Tuple[List[AggName], Dict[GroupingKeysTuple, RowValues]]:
        ...

    @overload
    def to_tabular(
        self,
        *,
        index_orient: Literal[False],
        grouped_by: Optional[AggName] = None,
        expand_columns: bool = True,
        expand_sep: str = "|",
        normalize: bool = True,
        with_single_bucket_groups: bool = False,
    ) -> Tuple[List[AggName], List[Row]]:
        ...

    def to_tabular(
        self,
        *,
        index_orient: bool = True,
        grouped_by: Optional[AggName] = None,
        expand_columns: bool = True,
        expand_sep: str = "|",
        normalize: bool = True,
        with_single_bucket_groups: bool = False,
    ) -> Tuple[List[AggName], Union[Dict[GroupingKeysTuple, RowValues], List[Row]]]:
        """
        Build tabular view of ES response grouping levels (rows) until 'grouped_by' aggregation node included is
        reached, and using children aggregations of grouping level as values for each of generated groups (columns).

        Suppose an aggregation of this shape (A & B bucket aggregations)::

            A──> B──> C1
                 ├──> C2
                 └──> C3

        With grouped_by='B', breakdown ElasticSearch response (tree structure), into a tabular structure of this shape::

                                  C1     C2    C3
            A           B
            wood        blue      10     4     0
                        red       7      5     2
            steel       blue      1      9     0
                        red       23     4     2

        :param index_orient: if True, level-key samples are returned as tuples, else in a dictionary
        :param grouped_by: name of the aggregation node used as last grouping level
        :param normalize: if True, normalize columns buckets
        :return: index_names, values
        """
        grouping_agg_name, grouping_agg = self._grouping_agg(grouped_by)

        index_names: List[AggName]

        if index_orient:
            index_values: List[Tuple[GroupingKeysTuple, BucketDict]]
            index_names, index_values = self.parse_group_by(
                response=self.data,
                until=grouping_agg_name,
                with_single_bucket_groups=with_single_bucket_groups,
                row_as_tuple=True,
            )
            rows: Dict[GroupingKeysTuple, Row] = {
                row_index: self._serialize_columns(
                    row_raw_data=row_raw_data,
                    normalize=normalize,
                    total_agg=grouping_agg,
                    expand_columns=expand_columns,
                    expand_sep=expand_sep,
                )
                for row_index, row_raw_data in index_values
            }
            return index_names, rows

        index_values_: List[Tuple[GroupingKeysDict, BucketDict]]
        index_names, index_values_ = self.parse_group_by(
            response=self.data,
            until=grouping_agg_name,
            with_single_bucket_groups=with_single_bucket_groups,
            row_as_tuple=False,
        )
        rows_ = [
            dict(
                row_index,
                **self._serialize_columns(
                    row_raw_data=row_raw_data,
                    normalize=normalize,
                    total_agg=grouping_agg,
                    expand_columns=expand_columns,
                    expand_sep=expand_sep,
                ),
            )
            for row_index, row_raw_data in index_values_
        ]
        return index_names, rows_

    def _serialize_columns(
        self,
        row_raw_data: BucketDict,
        normalize: bool,
        expand_columns: bool,
        expand_sep: str,
        total_agg: AggClause,
    ) -> RowValues:
        # extract value (usually 'doc_count') of grouping agg node
        result: RowValues = {}
        if not isinstance(total_agg, Root):
            result[total_agg.VALUE_ATTRS[0]] = total_agg.extract_bucket_value(
                row_raw_data
            )

        # extract values of children, one columns per child
        child_key: AggName
        child: AggClause
        for child_key, child in self._aggs.children(  # type: ignore
            total_agg.identifier
        ):
            if isinstance(child, (UniqueBucketAgg, MetricAgg)):
                result[child_key] = child.extract_bucket_value(row_raw_data[child_key])
            elif expand_columns:
                for key, bucket in child.extract_buckets(row_raw_data[child_key]):
                    result[
                        "%s%s%s" % (child_key, expand_sep, key)
                    ] = child.extract_bucket_value(bucket)
            elif normalize:
                result[child_key] = next(
                    self._normalize_buckets(row_raw_data, child_key), None
                )
            else:
                result[child_key] = row_raw_data[child_key]
        return result

    def to_dataframe(
        self,
        grouped_by: Optional[str] = None,
        normalize_children: bool = True,
        with_single_bucket_groups: bool = False,
    ) -> pd.DataFrame:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'Using dataframe output format requires to install pandas. Please install "pandas" or '
                "use another output format."
            )
        index_names: List[AggName]
        rows: Dict[GroupingKeysTuple, RowValues]

        index_names, rows = self.to_tabular(
            index_orient=True,
            grouped_by=grouped_by,
            normalize=normalize_children,
            with_single_bucket_groups=with_single_bucket_groups,
        )

        if not rows:
            return pd.DataFrame()

        index: Tuple[GroupingKeysTuple, ...]
        values: Tuple[RowValues, ...]
        index, values = zip(*rows.items())

        # empty index
        if len(index[0]) == 0:
            return pd.DataFrame(index=(None,) * len(values), data=list(values))
        # single or multi-index
        return pd.DataFrame(
            index=pd.MultiIndex.from_tuples(index, names=index_names), data=list(values)
        ).sort_index()

    def to_normalized(self) -> NormalizedBucketDict:
        children: List[NormalizedBucketDict] = []
        for k in self.data.keys():
            for child in self._normalize_buckets(self.data, k):
                children.append(child)
        return {"level": "root", "key": None, "value": None, "children": children}

    def to_tree(self) -> AggsResponseTree:
        return AggsResponseTree(aggs=self._aggs).parse(self.data)

    def to_interactive_tree(self) -> IResponse:
        return IResponse(tree=self.to_tree(), search=self._search, depth=1)

    def __repr__(self) -> str:
        if not self.keys():
            return "<Aggregations> empty"
        return "<Aggregations> %s" % list(map(str, self.keys()))
