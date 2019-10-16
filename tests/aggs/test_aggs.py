#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
#                                   IMPORTS
# =============================================================================

from unittest import TestCase

from pandagg.aggs import Agg
from pandagg.mapping import MappingTree, Mapping
from pandagg.nodes import Avg, Max, Min, DateHistogram

from tests.mapping.mapping_example import MAPPING_NAME, MAPPING_DETAIL


EXPECTED_DICT_AGG = {
    "week": {
        "date_histogram": {
            "field": "date",
            "format": "yyyy-MM-dd",
            "interval": "1w"
        },
        "aggs": {
            "nested_below_week": {
                "nested": {
                    "path": "local_metrics"
                },
                "aggs": {
                    "local_metrics.field_class.name": {
                        "terms": {
                            "field": "local_metrics.field_class.name",
                            "size": 10
                        },
                        "aggs": {
                            "avg_f1_score": {
                                "avg": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            },
                            "max_f1_score": {
                                "max": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            },
                            "min_f1_score": {
                                "min": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


class AggTestCase(TestCase):

    def test_mapping_declaration(self):
        """Should accept:
        - dict
        - mapping tree
        - mapping object
        """
        agg_dict_mapping = Agg(mapping={MAPPING_NAME: MAPPING_DETAIL})
        agg_tree_mapping = Agg(mapping=MappingTree(mapping_name=MAPPING_NAME, mapping_detail=MAPPING_DETAIL))
        agg_obj_mapping = Agg(mapping=Mapping(tree=MappingTree(mapping_name=MAPPING_NAME, mapping_detail=MAPPING_DETAIL)))
        self.assertEqual(
            agg_dict_mapping.tree_mapping.to_dict(),
            agg_tree_mapping.tree_mapping.to_dict()
        )
        self.assertEqual(
            agg_dict_mapping.tree_mapping.to_dict(),
            agg_obj_mapping.tree_mapping.to_dict()
        )

    def test_init_from_dict(self):

        agg = Agg(from_=EXPECTED_DICT_AGG, mapping={MAPPING_NAME: MAPPING_DETAIL})
        self.assertEqual(agg.query_dict(), EXPECTED_DICT_AGG)
        self.assertEqual(
            agg.__repr__().decode('utf-8'),
            u"""<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
        ├── avg_f1_score
        ├── max_f1_score
        └── min_f1_score
""")

    def test_init_from_node_hierarchy(self):
        week = DateHistogram(
            agg_name='week',
            field='date',
            interval='1w'
        )

        # default size defines size of terms aggregations, (impacts "local_metrics.field_class.name" terms agg)
        agg = Agg(mapping={MAPPING_NAME: MAPPING_DETAIL}) \
            .groupby([week, "local_metrics.field_class.name"], default_size=10) \
            .agg([
                Min(agg_name='min_f1_score', field='local_metrics.performance.test.f1_score'),
                Max(agg_name='max_f1_score', field='local_metrics.performance.test.f1_score'),
                Avg(agg_name='avg_f1_score', field='local_metrics.performance.test.f1_score')
            ])
        self.assertEqual(agg.query_dict(), EXPECTED_DICT_AGG)
        self.assertEqual(
            agg.__repr__().decode('utf-8'),
            u"""<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
        ├── avg_f1_score
        ├── max_f1_score
        └── min_f1_score
""")
