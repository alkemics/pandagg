from __future__ import annotations

import copy
import dataclasses
from typing import Iterator, Optional, List, TYPE_CHECKING, Dict, Tuple

from elasticsearch import Elasticsearch
from lighttree.node import NodeId

from pandagg.query import Query
from pandagg.aggs import Aggs
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
)

if TYPE_CHECKING:
    import pandas as pd
    from pandagg.search import Search


class Hit:
    def __init__(self, data: HitDict) -> None:
        self.data: HitDict = data
        self._source: Optional[DocSource] = data.get("_source")
        self._score: Optional[float] = data.get("_score")
        self._id: Optional[str] = data.get("_id")
        self._index: Optional[str] = data.get("_index")

    def __repr__(self) -> str:
        if self._score is None:
            return "<Hit %s>" % self._id
        return "<Hit %s> score=%.2f" % (self._id, self._score)


class Hits:
    def __init__(self, hits: Optional[HitsDict]) -> None:
        self.data: Optional[HitsDict] = hits
        self.total: Optional[TotalDict] = hits.get("total") if hits else None
        self.hits: List[Hit] = (
            [Hit(hit) for hit in hits.get("hits", [])] if hits else []
        )
        self.max_score: Optional[float] = hits.get("max_score") if hits else None

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
        hits = self.data.get("hits", []) if self.data else []
        if not hits:
            return pd.DataFrame()
        if not expand_source:
            return pd.DataFrame(hits).set_index("_id")
        flattened_hits = []
        for hit in hits:
            hit_metadata = hit.copy()
            hit_source = hit_metadata.pop("_source")
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


class SearchResponse:
    def __init__(self, data: SearchResponseDict, search: Search) -> None:
        self.data = data
        self.__search = search

        self.took: Optional[int] = data.get("took")
        self.timed_out: Optional[int] = data.get("timed_out")
        self._shards: Optional[ShardsDict] = data.get("_shards")
        self.hits: Hits = Hits(data.get("hits"))
        self.aggregations: Aggregations = Aggregations(
            data.get("aggregations", {}), _search=self.__search
        )
        self.profile: Optional[ProfileDict] = data.get("profile")

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

    def _parse_group_by(
        self,
        response: AggregationsResponseDict,
        until: AggName,
        row=None,
        agg_name: Optional[AggName] = None,
        ancestors_: Optional[List[Tuple[str, AggClause]]] = None,
        row_as_tuple: bool = False,
        with_single_bucket_groups: bool = False,
    ):
        """
        Recursive parsing of succession of grouping aggregation clauses.

        Yields each row for which last bucket aggregation generated buckets.
        """
        # initialization: cache ancestors once for faster computation, that will be passed as arguments to downside
        # recursive calls
        ancestors: List[Tuple[str, AggClause]]
        if ancestors_ is None:
            until_id: NodeId = self._aggs.id_from_key(until)
            # remove root (not an aggregation clause), ignore type warning about key None (since only root
            # can have a None key)
            ancestors = [
                (k, n)  # type: ignore
                for k, n in self._aggs.ancestors(until_id, include_current=True)[:-1]
            ]
            agg_name, agg_node = ancestors[-1]
        else:
            ancestors = ancestors_

        if agg_name not in response:
            return None

        if not row:
            row = [] if row_as_tuple else {}

        agg_node = [n for k, n in ancestors if k == agg_name][0]
        for key, raw_bucket in agg_node.extract_buckets(response[agg_name]):
            sub_row = copy.copy(row)
            if not isinstance(agg_node, UniqueBucketAgg) or with_single_bucket_groups:
                if row_as_tuple:
                    sub_row.append(key)
                else:
                    sub_row[agg_name] = key
            if agg_name == until:
                # end real yield
                if row_as_tuple:
                    yield tuple(sub_row), raw_bucket
                else:
                    yield sub_row, raw_bucket
            elif agg_name in {k for k, _ in ancestors}:
                # yield children
                child_key: str
                for child_key, _ in self._aggs.children(  # type: ignore
                    agg_node.identifier
                ):
                    for nrow, nraw_bucket in self._parse_group_by(
                        row=sub_row,
                        response=raw_bucket,
                        agg_name=child_key,
                        until=until,
                        row_as_tuple=row_as_tuple,
                        ancestors_=ancestors,
                    ):
                        yield nrow, nraw_bucket

    def _normalize_buckets(self, agg_response, agg_name=None):
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
        agg_name = agg_name or self._aggs.root
        id_ = self._aggs.id_from_key(agg_name)
        agg_key, agg_node = self._aggs.get(id_)
        agg_children = self._aggs.children(agg_node.identifier)
        for key, raw_bucket in agg_node.extract_buckets(agg_response[agg_name]):
            result = {
                "level": agg_name,
                "key": key,
                "value": agg_node.extract_bucket_value(raw_bucket),
            }
            normalized_children = [
                normalized_child
                for child_key, child in agg_children
                for normalized_child in self._normalize_buckets(
                    agg_name=child_key, agg_response=raw_bucket
                )
            ]
            if normalized_children:
                result["children"] = normalized_children
            yield result

    def _grouping_agg(
        self, name: Optional[str] = None
    ) -> Tuple[Optional[str], AggClause]:
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

    def to_tabular(
        self,
        index_orient=True,
        grouped_by=None,
        expand_columns=True,
        expand_sep="|",
        normalize=True,
        with_single_bucket_groups=False,
    ):
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

        :param index_orient: if True, level-key samples are returned as tuples, else in a dictionnary
        :param grouped_by: name of the aggregation node used as last grouping level
        :param normalize: if True, normalize columns buckets
        :return: index_names, values
        """
        grouping_key, grouping_agg = self._grouping_agg(grouped_by)
        if grouping_key is None:
            index_values = [(tuple() if index_orient else dict(), self.data)]
            index_names = []
        else:
            index_names = [
                k
                for k, a in self._aggs.ancestors(
                    grouping_agg.identifier, from_root=True, include_current=True
                )
                if (not isinstance(a, UniqueBucketAgg) or with_single_bucket_groups)
                and k is not None
            ]
            index_values = list(
                self._parse_group_by(
                    response=self.data,
                    row_as_tuple=index_orient,
                    until=grouping_key,
                    with_single_bucket_groups=with_single_bucket_groups,
                )
            )
            if not index_values:
                return [], []

        if index_orient:
            rows = {
                row_index: self._serialize_columns(
                    row_values,
                    normalize=normalize,
                    total_agg=grouping_agg,
                    expand_columns=expand_columns,
                    expand_sep=expand_sep,
                )
                for row_index, row_values in index_values
            }
        else:
            rows = [
                dict(
                    row_index,
                    **self._serialize_columns(
                        row_values,
                        normalize=normalize,
                        total_agg=grouping_agg,
                        expand_columns=expand_columns,
                        expand_sep=expand_sep,
                    )
                )
                for row_index, row_values in index_values
            ]
        return index_names, rows

    def _serialize_columns(
        self, row_data, normalize, expand_columns, expand_sep, total_agg=None
    ):
        # extract value (usually 'doc_count') of grouping agg node
        result = {}
        if total_agg is not None and not isinstance(total_agg, Root):
            result[total_agg.VALUE_ATTRS[0]] = total_agg.extract_bucket_value(row_data)
            grouping_agg_children = self._aggs.children(total_agg.identifier)
        else:
            grouping_agg_children = self._aggs.children(self._aggs.root)

        # extract values of children, one columns per child
        for child_key, child in grouping_agg_children:
            if isinstance(child, (UniqueBucketAgg, MetricAgg)):
                result[child_key] = child.extract_bucket_value(row_data[child_key])
            elif expand_columns:
                for key, bucket in child.extract_buckets(row_data[child_key]):
                    result[
                        "%s%s%s" % (child_key, expand_sep, key)
                    ] = child.extract_bucket_value(bucket)
            elif normalize:
                result[child_key] = next(
                    self._normalize_buckets(row_data, child_key), None
                )
            else:
                result[child_key] = row_data[child_key]
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
        index_names, rows = self.to_tabular(
            index_orient=True,
            grouped_by=grouped_by,
            normalize=normalize_children,
            with_single_bucket_groups=with_single_bucket_groups,
        )
        index, values = zip(*rows.items())
        if not index:
            return pd.DataFrame()
        if len(index[0]) == 0:
            index = (None,) * len(index)
        else:
            index = pd.MultiIndex.from_tuples(index, names=index_names)
        return pd.DataFrame(index=index, data=list(values)).sort_index()

    def to_normalized(self) -> Dict:
        children = []
        for k in sorted(list((self.data.keys()))):
            for child in self._normalize_buckets(self.data, k):
                children.append(child)
        return {"level": "root", "key": None, "value": None, "children": children}

    def to_tree(self) -> AggsResponseTree:
        return AggsResponseTree(aggs=self._aggs).parse(self.data)

    def to_interactive_tree(self) -> IResponse:
        return IResponse(tree=self.to_tree(), search=self._search, depth=1)

    def serialize(self, output="tabular", **kwargs):
        """
        :param output: output format, one of "raw", "tree", "interactive_tree", "normalized", "tabular", "dataframe"
        :param kwargs: tabular serialization kwargs
        :return:
        """
        if output == "raw":
            return self.data
        elif output == "tree":
            return self.to_tree()
        elif output == "interactive_tree":
            return self.to_interactive_tree()
        elif output == "normalized":
            return self.to_normalized()
        elif output == "tabular":
            return self.to_tabular(**kwargs)
        elif output == "dataframe":
            return self.to_dataframe(**kwargs)
        else:
            raise NotImplementedError("Unknown %s output format." % output)

    def __repr__(self) -> str:
        if not self.keys():
            return "<Aggregations> empty"
        return "<Aggregations> %s" % list(map(str, self.keys()))
