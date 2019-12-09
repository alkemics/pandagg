#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
#                                   IMPORTS
# =============================================================================

from unittest import TestCase
import pandas as pd
from treelib.exceptions import MultipleRootError
from pandagg.aggs import Agg, ClientBoundAgg
from pandagg.buckets.response import Response
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError
from pandagg.mapping import MappingTree, Mapping
from pandagg.nodes import Avg, Min, DateHistogram, Terms, Filter

import tests.aggs.data_sample as sample

from tests.mapping.mapping_example import MAPPING


class AggTestCase(TestCase):

    def test_add_node_with_mapping(self):
        with_mapping = Agg(mapping=MAPPING)
        self.assertEqual(len(with_mapping.nodes.keys()), 0)

        # add regular node
        with_mapping.add_node(Terms('workflow', field='workflow'))
        self.assertEqual(len(with_mapping.nodes.keys()), 1)

        # try to add second root fill fail
        with self.assertRaises(MultipleRootError):
            with_mapping.add_node(Terms('classification_type', field='classification_type'))

        # try to add field aggregation on non-existing field will fail
        with self.assertRaises(AbsentMappingFieldError):
            with_mapping.add_node(
                node=Terms('imaginary_agg', field='imaginary_field'),
                pid='workflow'
            )
        self.assertEqual(len(with_mapping.nodes.keys()), 1)

        # try to add aggregation on a non-compatible field will fail
        with self.assertRaises(InvalidOperationMappingFieldError):
            with_mapping.add_node(
                node=Avg('average_of_string', field='classification_type'),
                pid='workflow'
            )
        self.assertEqual(len(with_mapping.nodes.keys()), 1)

        # add field aggregation on field passing through nested will automatically add nested
        with_mapping.add_node(
            node=Avg('local_f1_score', field='local_metrics.performance.test.f1_score'),
            pid='workflow'
        )
        self.assertEqual(len(with_mapping.nodes.keys()), 3)
        self.assertEqual(
            with_mapping.__str__(),
            """<Aggregation>
workflow
└── nested_below_workflow
    └── local_f1_score
"""
        )
        self.assertIn('nested_below_workflow', with_mapping)
        nested_node = with_mapping['nested_below_workflow']
        self.assertEqual(nested_node.AGG_TYPE, 'nested')
        self.assertEqual(nested_node.path, 'local_metrics')

        # add other agg requiring nested will reuse nested agg as parent
        with_mapping.add_node(
            node=Avg('local_precision', field='local_metrics.performance.test.precision'),
            pid='workflow'
        )
        self.assertEqual(
            with_mapping.__str__(),
            """<Aggregation>
workflow
└── nested_below_workflow
    ├── local_f1_score
    └── local_precision
"""
        )
        self.assertEqual(len(with_mapping.nodes.keys()), 4)

        # add under a nested parent a field aggregation that requires to be located under root will automatically
        # add reverse-nested
        with_mapping.add_node(
            node=Terms('language_terms', field='language'),
            pid='nested_below_workflow'
        )
        self.assertEqual(len(with_mapping.nodes.keys()), 6)
        self.assertEqual(
            with_mapping.__str__(),
            """<Aggregation>
workflow
└── nested_below_workflow
    ├── local_f1_score
    ├── local_precision
    └── reverse_nested_below_nested_below_workflow
        └── language_terms
"""
        )

    def test_add_node_without_mapping(self):
        without_mapping = Agg()
        self.assertEqual(len(without_mapping.nodes.keys()), 0)

        # add regular node
        without_mapping.add_node(Terms('workflow_not_existing', field='field_not_existing'))
        self.assertEqual(len(without_mapping.nodes.keys()), 1)

    # TODO - finish these tests (reverse nested)
    def test_paste_tree_with_mapping(self):
        # with explicit nested
        initial_agg_1 = Agg(
            mapping=MAPPING,
            from_={
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w"
                    }
                }
            }
        )
        self.assertEqual(set(initial_agg_1.nodes.keys()), {'week'})
        pasted_agg_1 = Agg(
            from_={
                "nested_below_week": {
                    "nested": {
                        "path": "local_metrics"
                    },
                    "aggs": {
                        "local_metrics.field_class.name": {
                            "terms": {
                                "field": "local_metrics.field_class.name",
                                "size": 10
                            }
                        }
                    }
                }
            }
        )
        self.assertEqual(set(pasted_agg_1.nodes.keys()), {'nested_below_week', 'local_metrics.field_class.name'})

        initial_agg_1.paste('week', pasted_agg_1)
        self.assertEqual(set(initial_agg_1.nodes.keys()), {'week', 'nested_below_week', 'local_metrics.field_class.name'})
        self.assertEqual(
            initial_agg_1.__str__(),
            """<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
"""
        )

        # without explicit nested
        initial_agg_2 = Agg(
            mapping=MAPPING,
            from_={
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w"
                    }
                }
            }
        )
        self.assertEqual(set(initial_agg_2.nodes.keys()), {'week'})
        pasted_agg_2 = Agg(
            from_={
                "local_metrics.field_class.name": {
                    "terms": {
                        "field": "local_metrics.field_class.name",
                        "size": 10
                    }
                }
            }
        )
        self.assertEqual(set(pasted_agg_2.nodes.keys()), {'local_metrics.field_class.name'})

        initial_agg_2.paste("week", pasted_agg_2)
        self.assertEqual(set(initial_agg_2.nodes.keys()), {'week', 'nested_below_week', 'local_metrics.field_class.name'})
        self.assertEqual(
            initial_agg_2.__str__(),
            """<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
"""
        )

    def test_paste_tree_without_mapping(self):
        # with explicit nested
        initial_agg_1 = Agg(
            mapping=None,
            from_={
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w"
                    }
                }
            }
        )
        self.assertEqual(set(initial_agg_1.nodes.keys()), {'week'})

        pasted_agg_1 = Agg(
            from_={
                "nested_below_week": {
                    "nested": {
                        "path": "local_metrics"
                    },
                    "aggs": {
                        "local_metrics.field_class.name": {
                            "terms": {
                                "field": "local_metrics.field_class.name",
                                "size": 10
                            }
                        }
                    }
                }
            }
        )
        self.assertEqual(set(pasted_agg_1.nodes.keys()), {'nested_below_week', "local_metrics.field_class.name"})

        initial_agg_1.paste('week', pasted_agg_1)
        self.assertEqual(set(initial_agg_1.nodes.keys()), {'week', 'nested_below_week', "local_metrics.field_class.name"})
        self.assertEqual(
            initial_agg_1.__str__(),
            """<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
"""
        )

        # without explicit nested (will NOT add nested)
        initial_agg_2 = Agg(
            mapping=None,
            from_={
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w"
                    }
                }
            }
        )
        self.assertEqual(set(initial_agg_2.nodes.keys()), {"week"})

        pasted_agg_2 = Agg(
            from_={
                "local_metrics.field_class.name": {
                    "terms": {
                        "field": "local_metrics.field_class.name",
                        "size": 10
                    }
                }
            }
        )
        self.assertEqual(set(pasted_agg_2.nodes.keys()), {"local_metrics.field_class.name"})

        initial_agg_2.paste("week", pasted_agg_2)
        self.assertEqual(set(initial_agg_2.nodes.keys()), {"week", "local_metrics.field_class.name"})
        self.assertEqual(
            initial_agg_2.__str__(),
            """<Aggregation>
week
└── local_metrics.field_class.name
"""
        )

    def test_interpret_agg_string(self):
        empty_agg = Agg()
        empty_agg._interpret_agg(insert_below=None, element='some_field')
        self.assertEqual(
            empty_agg.query_dict(),
            {'some_field': {'terms': {'field': 'some_field'}}}
        )

        # with default size
        empty_agg = Agg()
        empty_agg._interpret_agg(insert_below=None, element='some_field', size=10)
        self.assertEqual(
            empty_agg.query_dict(),
            {'some_field': {'terms': {'field': 'some_field', 'size': 10}}}
        )

        # with parent
        agg = Agg(from_={'root_agg_name': {'terms': {'field': 'some_field', 'size': 5}}})
        agg._interpret_agg(insert_below='root_agg_name', element='child_field')
        self.assertEqual(
            agg.query_dict(),
            {
                "root_agg_name": {
                    "aggs": {
                        "child_field": {
                            "terms": {
                                "field": "child_field"
                            }
                        }
                    },
                    "terms": {
                        "field": "some_field",
                        "size": 5
                    }
                }
            }
        )

        # with required nested
        agg = Agg(
            from_={'term_workflow': {'terms': {'field': 'workflow', 'size': 5}}},
            mapping=MAPPING
        )
        agg._interpret_agg(insert_below='term_workflow', element='local_metrics.field_class.name')
        self.assertEqual(
            agg.query_dict(),
            {
                "term_workflow": {
                    "aggs": {
                        "nested_below_term_workflow": {
                            "aggs": {
                                "local_metrics.field_class.name": {
                                    "terms": {
                                        "field": "local_metrics.field_class.name"
                                    }
                                }
                            },
                            "nested": {
                                "path": "local_metrics"
                            }
                        }
                    },
                    "terms": {
                        "field": "workflow",
                        "size": 5
                    }
                }
            }
        )

    def test_interpret_node(self):
        empty_agg = Agg()
        node = Terms(
            name='some_name',
            field='some_field',
            size=10
        )
        empty_agg._interpret_agg(insert_below=None, element=node)
        self.assertEqual(
            empty_agg.query_dict(),
            {
                "some_name": {
                    "terms": {
                        "field": "some_field",
                        "size": 10
                    }
                }
            }
        )
        # with parent with required nested
        agg = Agg(
            from_={'term_workflow': {'terms': {'field': 'workflow', 'size': 5}}},
            mapping=MAPPING
        )
        node = Avg(
            name='min_local_f1',
            field='local_metrics.performance.test.f1_score'
        )
        agg._interpret_agg(insert_below='term_workflow', element=node)
        self.assertEqual(
            agg.query_dict(),
            {
                "term_workflow": {
                    "aggs": {
                        "nested_below_term_workflow": {
                            "aggs": {
                                "min_local_f1": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.f1_score"
                                    }
                                }
                            },
                            "nested": {
                                "path": "local_metrics"
                            }
                        }
                    },
                    "terms": {
                        "field": "workflow",
                        "size": 5
                    }
                }
            }
        )

    def test_query_dict(self):
        # empty
        self.assertEqual(Agg().query_dict(), {})

        # single node
        agg = Agg()
        node = Terms(
            name='root_agg',
            field='some_field',
            size=10
        )
        agg.add_node(node)
        self.assertEqual(
            agg.query_dict(),
            {
                "root_agg": {
                    "terms": {
                        "field": "some_field",
                        "size": 10
                    }
                }
            }
        )

        # hierarchy
        agg.add_node(
            Terms(
                name='other_name',
                field='other_field',
                size=30
            ),
            'root_agg'
        )
        agg.add_node(
            Avg(
                name='avg_some_other_field',
                field='some_other_field'
            ),
            'root_agg'
        )
        self.assertEqual(
            agg.__str__(),
            """<Aggregation>
root_agg
├── avg_some_other_field
└── other_name
"""
        )
        self.assertEqual(
            agg.query_dict(),
            {
                "root_agg": {
                    "aggs": {
                        "avg_some_other_field": {
                            "avg": {
                                "field": "some_other_field"
                            }
                        },
                        "other_name": {
                            "terms": {
                                "field": "other_field",
                                "size": 30
                            }
                        }
                    },
                    "terms": {
                        "field": "some_field",
                        "size": 10
                    }
                }
            }
        )

    def test_parse_as_tree(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        response = my_agg._serialize_as_tree(sample.ES_AGG_RESPONSE)
        self.assertIsInstance(response, Response)
        self.assertEqual(
            response.__str__(),
            sample.EXPECTED_RESPONSE_REPR
        )
        # check that tree attributes are accessible

    def test_normalize_buckets(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        self.assertEqual(
            my_agg._serialize_as_normalized(sample.ES_AGG_RESPONSE),
            sample.EXPECTED_NORMALIZED_RESPONSE
        )

    def test_parse_as_tabular(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        index, index_names, values = my_agg._serialize_as_tabular(sample.ES_AGG_RESPONSE)
        self.assertEqual(index_names, ['classification_type', 'global_metrics.field.name'])
        self.assertEqual(len(index), len(values))
        self.assertEqual(len(index), 10)
        self.assertEqual(index, sample.EXPECTED_TABULAR_INDEX)
        self.assertEqual(values, sample.EXPECTED_TABULAR_VALUES)

    def test_parse_as_dataframe(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        df = my_agg._serialize_as_dataframe(sample.ES_AGG_RESPONSE)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(set(df.index.names), {'classification_type', 'global_metrics.field.name'})
        self.assertEqual(set(df.columns), {'avg_f1_micro', 'avg_nb_classes', 'doc_count'})
        self.assertEqual(df.shape, (len(sample.EXPECTED_TABULAR_INDEX), 3))

    def test_validate_aggs_parent_id(self):
        """
        <Aggregation>
        classification_type
        └── global_metrics.field.name
            ├── avg_f1_micro
            └── avg_nb_classes
        """
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)

        with self.assertRaises(ValueError) as e:
            my_agg._validate_aggs_parent_id(pid=None)
        self.assertEqual(e.exception.args, ("Declaration is ambiguous, you must declare the node id under which these "
                                            "aggregations should be placed.",))

        with self.assertRaises(ValueError) as e:
            my_agg._validate_aggs_parent_id("avg_f1_micro")
        self.assertEqual(e.exception.args, ("Node id <avg_f1_micro> is not a bucket aggregation.",))

        self.assertEqual(
            my_agg._validate_aggs_parent_id("global_metrics.field.name"),
            "global_metrics.field.name"
        )

        with self.assertRaises(ValueError) as e:
            my_agg._validate_aggs_parent_id("non-existing-node")
        self.assertEqual(e.exception.args, ("Node id <non-existing-node> is not present in aggregation.",))

        # linear agg
        my_agg.remove_node('avg_f1_micro')
        my_agg.remove_node('avg_nb_classes')
        """
        <Aggregation>
        classification_type
        └── global_metrics.field.name
        """
        self.assertEqual(
            my_agg._validate_aggs_parent_id(None),
            'global_metrics.field.name'
        )

        # empty agg
        agg = Agg()
        self.assertEqual(agg._validate_aggs_parent_id(None), None)

    def test_agg_method(self):
        pass

    def test_groupby_method(self):
        pass

    def test_mapping_from_init(self):
        agg_from_dict_mapping = Agg(mapping=MAPPING)
        agg_from_tree_mapping = Agg(mapping=MappingTree(mapping_detail=MAPPING))
        agg_from_obj_mapping = Agg(mapping=Mapping(tree=MappingTree(mapping_detail=MAPPING)))
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.to_dict(),
            agg_from_tree_mapping.tree_mapping.to_dict()
        )
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.to_dict(),
            agg_from_obj_mapping.tree_mapping.to_dict()
        )
        self.assertIsInstance(agg_from_dict_mapping, Agg)
        self.assertIsInstance(agg_from_tree_mapping, Agg)
        self.assertIsInstance(agg_from_obj_mapping, Agg)

    def test_set_mapping(self):
        agg_from_dict_mapping = Agg() \
            .set_mapping(mapping=MAPPING)
        agg_from_tree_mapping = Agg() \
            .set_mapping(mapping=MappingTree(mapping_detail=MAPPING))
        agg_from_obj_mapping = Agg() \
            .set_mapping(mapping=Mapping(tree=MappingTree(mapping_detail=MAPPING)))
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.to_dict(),
            agg_from_tree_mapping.tree_mapping.to_dict()
        )
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.to_dict(),
            agg_from_obj_mapping.tree_mapping.to_dict()
        )
        # set mapping returns self
        self.assertIsInstance(agg_from_dict_mapping, Agg)
        self.assertIsInstance(agg_from_tree_mapping, Agg)
        self.assertIsInstance(agg_from_obj_mapping, Agg)

    def test_init_from_dict(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        self.assertEqual(my_agg.query_dict(), sample.EXPECTED_AGG_QUERY)
        self.assertEqual(my_agg.__str__(), sample.EXPECTED_REPR)

    def test_init_from_node_hierarchy(self):
        node_hierarchy = sample.get_node_hierarchy()

        agg = Agg(from_=node_hierarchy, mapping=MAPPING)
        self.assertEqual(agg.query_dict(), sample.EXPECTED_AGG_QUERY)
        self.assertEqual(agg.__str__(), sample.EXPECTED_REPR)

        # with nested
        node_hierarchy = DateHistogram(
            name='week',
            field='date',
            interval='1w',
            aggs=[
                Terms(
                    name="local_metrics.field_class.name",
                    field="local_metrics.field_class.name",
                    size=10,
                    aggs=[
                        Min(
                            name='min_f1_score',
                            field='local_metrics.performance.test.f1_score'
                        )
                    ]
                )
            ]
        )
        agg = Agg(from_=node_hierarchy, mapping=MAPPING)
        self.assertEqual(
            agg.query_dict(),
            {
                "week": {
                    "aggs": {
                        "nested_below_week": {
                            "aggs": {
                                "local_metrics.field_class.name": {
                                    "aggs": {
                                        "min_f1_score": {
                                            "min": {
                                                "field": "local_metrics.performance.test.f1_score"
                                            }
                                        }
                                    },
                                    "terms": {
                                        "field": "local_metrics.field_class.name",
                                        "size": 10
                                    }
                                }
                            },
                            "nested": {
                                "path": "local_metrics"
                            }
                        }
                    },
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w"
                    }
                }
            }
        )
        self.assertEqual(
            agg.__str__(),
            """<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
        └── min_f1_score
"""
        )

    def test_groupby_and_agg(self):
        agg = sample.get_wrapper_declared_agg()
        self.assertEqual(agg.query_dict(), sample.EXPECTED_AGG_QUERY)
        self.assertEqual(agg.__str__(), sample.EXPECTED_REPR)

    def test_applied_nested_path_at_node(self):
        """ Check that correct nested path is detected at node levels:
        week
        └── nested_below_week
            └── local_metrics.field_class.name
                ├── avg_f1_score
                ├── max_f1_score
                └── min_f1_score
        """
        node_hierarchy = DateHistogram(
            name='week',
            field='date',
            interval='1w',
            aggs=[
                Terms(
                    name="local_metrics.field_class.name",
                    field="local_metrics.field_class.name",
                    size=10,
                    aggs=[
                        Min(
                            name='min_f1_score',
                            field='local_metrics.performance.test.f1_score'
                        )
                    ]
                )
            ]
        )
        agg = Agg(from_=node_hierarchy, mapping=MAPPING)

        self.assertEqual(agg.applied_nested_path_at_node('week'), None)
        for nid in ('nested_below_week', 'local_metrics.field_class.name', 'min_f1_score'):
            self.assertEqual(agg.applied_nested_path_at_node(nid), 'local_metrics')

    def test_deepest_linear_agg(self):
        # deepest_linear_bucket_agg
        """
        week
        └── nested_below_week
            └── local_metrics.field_class.name   <----- HERE because then metric aggregation
                └── avg_f1_score
        """
        node_hierarchy = DateHistogram(
            name='week',
            field='date',
            interval='1w',
            aggs=[
                Terms(
                    name="local_metrics.field_class.name",
                    field="local_metrics.field_class.name",
                    size=10,
                    aggs=[
                        Min(
                            name='min_f1_score',
                            field='local_metrics.performance.test.f1_score'
                        )
                    ]
                )
            ]
        )
        agg = Agg(from_=node_hierarchy, mapping=MAPPING)
        self.assertEqual(agg.deepest_linear_bucket_agg, 'local_metrics.field_class.name')

        # week is last bucket linear bucket
        node_hierarchy_2 = DateHistogram(
            name='week',
            field='date',
            interval='1w',
            aggs=[
                Terms(
                    name="local_metrics.field_class.name",
                    field="local_metrics.field_class.name",
                    size=10
                ),
                Filter(
                    name="f1_score_above_threshold",
                    filter_={
                        "range": {
                            "local_metrics.performance.test.f1_score": {
                                "gte": 0.5
                            }
                        }
                    }
                )
            ]
        )
        agg2 = Agg(from_=node_hierarchy_2, mapping=MAPPING)
        self.assertEqual(agg2.deepest_linear_bucket_agg, 'week')


class ClientBoundAggTestCase(TestCase):

    def test_query(self):
        agg = ClientBoundAgg(client=None, index_name='some_index')

        new_agg = agg\
            .query({'term': {'some_field': 1}})\
            .query({'bool': {'must': [
                {'range': {'other_field': {'gt': 2}}},
                {'term': {'another_field': 'hi'}}
            ]}})

        self.assertIs(agg._query, None)
        self.assertEqual(
            new_agg._query,
            {
                'bool': {
                    'must': [
                        {'range': {'other_field': {'gt': 2}}},
                        {'term': {'another_field': 'hi'}},
                        {'term': {'some_field': 1}}
                    ]
                }
            }
        )
