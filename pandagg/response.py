#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import copy

from builtins import str as text

from future.utils import iterkeys, iteritems

from pandagg.interactive.response import IResponse
from pandagg.node.aggs.abstract import UniqueBucketAgg, MetricAgg, ShadowRoot
from pandagg.node.aggs.bucket import Nested
from pandagg.tree.response import AggsResponseTree


class Response:
    def __init__(self, data, search):
        self.data = data
        self.__search = search

        self.took = data["took"]
        self.timed_out = data["timed_out"]
        self._shards = data["_shards"]
        self.hits = Hits(data["hits"])
        self.aggregations = Aggregations(
            data.get("aggregations", {}),
            aggs=self.__search._aggs,
            index=self.__search._index,
            query=self.__search._query,
            client=self.__search._using,
        )
        self.profile = data.get("profile")

    def __iter__(self):
        return iter(self.hits)

    @property
    def success(self):
        return (
            self._shards["total"] == self._shards["successful"] and not self.timed_out
        )

    def __len__(self):
        return len(self.hits)

    def __repr__(self):
        return (
            "<Response> took %dms, success: %s, total result %s, contains %s hits"
            % (self.took, self.success, self.hits._total_repr(), len(self.hits))
        )


class Hits:
    def __init__(self, hits):
        self.data = hits
        self.total = hits["total"]
        self.hits = [Hit(hit) for hit in hits.get("hits", [])]
        self.max_score = hits["max_score"]

    def __len__(self):
        return len(self.hits)

    def __iter__(self):
        return iter(self.hits)

    def _total_repr(self):
        if not isinstance(self.total, dict):
            return str(self.total)
        if self.total.get("relation") == "eq":
            return str(self.total["value"])
        if self.total.get("relation") == "gte":
            return ">=%d" % self.total["value"]
        raise ValueError("Invalid total %s" % self.total)

    def __repr__(self):
        if not isinstance(self.total, dict):
            total_repr = text(self.total)
        elif self.total.get("relation") == "eq":
            total_repr = text(self.total["value"])
        elif self.total.get("relation") == "gte":
            total_repr = ">%d" % self.total["value"]
        else:
            raise ValueError("Invalid total %s" % self.total)
        return "<Hits> total: %s, contains %d hits" % (total_repr, len(self.hits))


class Hit:
    def __init__(self, data):
        self.data = data
        self._source = data.get("_source")
        self._score = data.get("_score")
        self._id = data.get("_id")
        self._type = data.get("_type")
        self._index = data.get("_index")

    def __repr__(self):
        return "<Hit %s> score=%.2f" % (self._id, self._score)


class Aggregations:
    def __init__(self, data, aggs, query, index, client):
        self.data = data
        self.__aggs = aggs
        self.__index = index
        self.__query = query
        self.__client = client

    def keys(self):
        return self.data.keys()

    def get(self, key):
        return self.data[key]

    def _parse_group_by(
        self,
        response,
        row=None,
        agg_name=None,
        until=None,
        ancestors=None,
        row_as_tuple=False,
        with_single_bucket_groups=False,
    ):
        """Recursive parsing of succession of unique child bucket aggregations.

        Yields each row for which last bucket aggregation generated buckets.
        """
        if ancestors is None:
            ancestors = self.__aggs.ancestors(until, id_only=True)
        if not row:
            row = [] if row_as_tuple else {}
        agg_name = self.__aggs.root if agg_name is None else agg_name
        if agg_name in response:
            agg_node = self.__aggs.get(agg_name)
            for key, raw_bucket in agg_node.extract_buckets(response[agg_name]):
                sub_row = copy.copy(row)
                if (
                    not isinstance(agg_node, UniqueBucketAgg)
                    or with_single_bucket_groups
                ):
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
                elif agg_name in ancestors:
                    # yield children
                    for child in self.__aggs.children(agg_name, id_only=False):
                        for nrow, nraw_bucket in self._parse_group_by(
                            row=sub_row,
                            response=raw_bucket,
                            agg_name=child.name,
                            until=until,
                            row_as_tuple=row_as_tuple,
                            ancestors=ancestors,
                        ):
                            yield nrow, nraw_bucket

    def _normalize_buckets(self, agg_response, agg_name=None):
        """Recursive function to parse aggregation response as a normalized entities.
        Each response bucket is represented as a dict with keys (key, level, value, children)::

            {
                "level": "owner.id",
                "key": 35,
                "value": 235,
                "children": [
                ]
            }
        """
        agg_name = agg_name or self.__aggs.root
        agg_node = self.__aggs.get(agg_name)
        agg_children = self.__aggs.children(agg_node.name, id_only=False)
        for key, raw_bucket in agg_node.extract_buckets(agg_response[agg_name]):
            result = {
                "level": agg_name,
                "key": key,
                "value": agg_node.extract_bucket_value(raw_bucket),
            }
            normalized_children = [
                normalized_child
                for child in agg_children
                for normalized_child in self._normalize_buckets(
                    agg_name=child.name, agg_response=raw_bucket
                )
            ]
            if normalized_children:
                result["children"] = normalized_children
            yield result

    def _grouping_agg(self, name=None):
        """Return aggregation node that used as grouping node.
        Note: in case there is only a nested aggregation below that node, groupby nested clause.
        """
        # if provided
        if name is not None:
            if name not in self.__aggs:
                raise ValueError("Cannot group by <%s>, agg node does not exist" % name)
            if not self.__aggs._is_eligible_grouping_node(name):
                raise ValueError(
                    "Cannot group by <%s>, not a valid grouping aggregation" % name
                )
            # if parent of single nested clause and nested_autocorrect
            node = self.__aggs.get(name)
            if self.__aggs.nested_autocorrect:
                children = self.__aggs.children(node.identifier, id_only=False)
                if len(children) == 1 and isinstance(children[0], Nested):
                    return children[0]
            return node

        if isinstance(self.__aggs.get(self.__aggs.root), ShadowRoot):
            return None
        name = self.__aggs.deepest_linear_bucket_agg
        if name is None:
            return None
        return self.__aggs.get(name)

    def to_tabular(
        self,
        index_orient=True,
        grouped_by=None,
        expand_columns=True,
        expand_sep="|",
        normalize=True,
        with_single_bucket_groups=False,
    ):
        """Build tabular view of ES response grouping levels (rows) until 'grouped_by' aggregation node included is
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
        :return: index, index_names, values
        """
        grouping_agg = self._grouping_agg(grouped_by)
        if grouping_agg is None:
            index_values = [(tuple() if index_orient else dict(), self.data)]
            index_names = []
        else:
            index_names = [
                a.name
                for a in self.__aggs.ancestors(
                    grouping_agg.name, id_only=False, from_root=True
                )
                + [grouping_agg]
                if not isinstance(a, UniqueBucketAgg) or with_single_bucket_groups
            ]
            index_values = list(
                self._parse_group_by(
                    response=self.data,
                    row_as_tuple=index_orient,
                    until=grouping_agg.name,
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
        if total_agg is not None and not isinstance(total_agg, ShadowRoot):
            result[total_agg.VALUE_ATTRS[0]] = total_agg.extract_bucket_value(row_data)
            grouping_agg_children = self.__aggs.children(
                total_agg.identifier, id_only=False
            )
        else:
            grouping_agg_children = self.__aggs.children(
                self.__aggs.root, id_only=False
            )

        # extract values of children, one columns per child
        for child in grouping_agg_children:
            if isinstance(child, (UniqueBucketAgg, MetricAgg)):
                result[child.name] = child.extract_bucket_value(row_data[child.name])
            elif expand_columns:
                for key, bucket in child.extract_buckets(row_data[child.name]):
                    result[
                        "%s%s%s" % (child.name, expand_sep, key)
                    ] = child.extract_bucket_value(bucket)
            elif normalize:
                result[child.name] = next(
                    self._normalize_buckets(row_data, child.name), None
                )
            else:
                result[child.name] = row_data[child.name]
        return result

    def to_dataframe(
        self, grouped_by=None, normalize_children=True, with_single_bucket_groups=False
    ):
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
        index, values = zip(*iteritems(rows))
        if not index:
            return pd.DataFrame()
        if len(index[0]) == 0:
            index = (None,) * len(index)
        else:
            index = pd.MultiIndex.from_tuples(index, names=index_names)
        return pd.DataFrame(index=index, data=values)

    def to_normalized(self):
        children = []
        for k in sorted(iterkeys(self.data)):
            for child in self._normalize_buckets(self.data, k):
                children.append(child)
        return {"level": "root", "key": None, "value": None, "children": children}

    def to_tree(self):
        return AggsResponseTree(aggs=self.__aggs, index=self.__index).parse(self.data)

    def to_interactive_tree(self):
        return IResponse(
            tree=self.to_tree(),
            index_name=self.__index,
            query=self.__query,
            client=self.__client,
            depth=1,
        )

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
            raise NotImplementedError("Unkown %s output format." % output)

    def __repr__(self):
        if not self.keys():
            return "<Aggregations> empty"
        return "<Aggregations> %s" % list(map(text, self.keys()))
