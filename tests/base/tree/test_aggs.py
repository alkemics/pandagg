#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
#                                   IMPORTS
# =============================================================================

from unittest import TestCase
import pandas as pd
from treelib.exceptions import MultipleRootError, NodeIDAbsentError
from pandagg.tree.agg import Agg
from pandagg.interactive.response import IResponse
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError
from pandagg.tree.mapping import Mapping
from pandagg.interactive.mapping import IMapping
from pandagg.node.agg.bucket import DateHistogram, Terms, Filter
from pandagg.node.agg.metric import Avg, Min

import tests.base.data_sample as sample
from pandagg.utils import equal_queries

from tests.base.mapping_example import MAPPING


class AggTestCase(TestCase):

    def test_deserialize_nodes_with_subaggs(self):
        expected = {
            'genres': {
                'terms': {'field': 'genres', 'size': 3},
                'aggs': {
                    'movie_decade': {
                        'date_histogram': {
                            'field': 'year',
                            'fixed_interval': '3650d'
                        }
                    }
                }
            }
        }
        agg1 = Agg(expected)
        agg2 = Agg(
            Terms(
                'genres', field='genres', size=3,
                aggs=DateHistogram(name='movie_decade', field='year', fixed_interval='3650d')
            )
        )
        agg3 = Agg(
            Terms(
                'genres', field='genres', size=3,
                aggs=[
                    DateHistogram(name='movie_decade', field='year', fixed_interval='3650d')
                ]
            )
        )
        agg4 = Agg(
            Terms(
                'genres', field='genres', size=3,
                aggs={
                    'movie_decade': {
                        'date_histogram': {
                            'field': 'year',
                            'fixed_interval': '3650d'
                        }
                    }
                }
            )
        )
        agg5 = Agg({
            'genres': {
                'terms': {'field': 'genres', 'size': 3},
                'aggs': DateHistogram(name='movie_decade', field='year', fixed_interval='3650d')
            }
        })
        for a in (agg1, agg2, agg3, agg4, agg5):
            self.assertEqual(a.query_dict(), expected)

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
[workflow] terms
└── [nested_below_workflow] nested
    └── [local_f1_score] avg
"""
        )
        self.assertIn('nested_below_workflow', with_mapping)
        nested_node = with_mapping['nested_below_workflow']
        self.assertEqual(nested_node.KEY, 'nested')
        self.assertEqual(nested_node.path, 'local_metrics')

        # add other agg requiring nested will reuse nested agg as parent
        with_mapping.add_node(
            node=Avg('local_precision', field='local_metrics.performance.test.precision'),
            pid='workflow'
        )
        self.assertEqual(
            with_mapping.__str__(),
            """<Aggregation>
[workflow] terms
└── [nested_below_workflow] nested
    ├── [local_f1_score] avg
    └── [local_precision] avg
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
[workflow] terms
└── [nested_below_workflow] nested
    ├── [local_f1_score] avg
    ├── [local_precision] avg
    └── [reverse_nested_below_nested_below_workflow] reverse_nested
        └── [language_terms] terms
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
[week] date_histogram
└── [nested_below_week] nested
    └── [local_metrics.field_class.name] terms
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
[week] date_histogram
└── [nested_below_week] nested
    └── [local_metrics.field_class.name] terms
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
[week] date_histogram
└── [nested_below_week] nested
    └── [local_metrics.field_class.name] terms
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
[week] date_histogram
└── [local_metrics.field_class.name] terms
"""
        )

    def test_interpret_agg_string(self):
        some_agg = Agg()
        some_agg = some_agg.agg('some_field', insert_below=None)
        self.assertEqual(
            some_agg.query_dict(),
            {'some_field': {'terms': {'field': 'some_field'}}}
        )

        # with default size
        some_agg = Agg()
        some_agg = some_agg.agg('some_field', insert_below=None, size=10)
        self.assertEqual(
            some_agg.query_dict(),
            {'some_field': {'terms': {'field': 'some_field', 'size': 10}}}
        )

        # with parent
        some_agg = Agg(from_={'root_agg_name': {'terms': {'field': 'some_field', 'size': 5}}})
        some_agg = some_agg.agg('child_field', insert_below='root_agg_name')
        self.assertEqual(
            some_agg.query_dict(),
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
        some_agg = Agg(
            from_={'term_workflow': {'terms': {'field': 'workflow', 'size': 5}}},
            mapping=MAPPING
        )
        some_agg = some_agg.agg('local_metrics.field_class.name', insert_below='term_workflow')
        self.assertEqual(
            some_agg.query_dict(),
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
        node = Terms(
            name='some_name',
            field='some_field',
            size=10
        )
        some_agg = Agg().agg(node, insert_below=None)
        self.assertEqual(
            some_agg.query_dict(),
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
        some_agg = Agg(
            from_={'term_workflow': {'terms': {'field': 'workflow', 'size': 5}}},
            mapping=MAPPING
        )
        node = Avg(
            name='min_local_f1',
            field='local_metrics.performance.test.f1_score'
        )
        some_agg = some_agg.agg(node, insert_below='term_workflow')
        self.assertEqual(
            some_agg.query_dict(),
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
[root_agg] terms
├── [avg_some_other_field] avg
└── [other_name] terms
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
        response = my_agg._serialize_response_as_tree(sample.ES_AGG_RESPONSE)
        self.assertIsInstance(response, IResponse)
        self.assertEqual(
            response.__str__(),
            sample.EXPECTED_RESPONSE_REPR
        )
        # check that tree attributes are accessible

    def test_normalize_buckets(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        self.assertTrue(equal_queries(
            my_agg._serialize_response_as_normalized(sample.ES_AGG_RESPONSE),
            sample.EXPECTED_NORMALIZED_RESPONSE
        ))

    def test_parse_as_tabular(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        index, index_names, values = my_agg._serialize_response_as_tabular(sample.ES_AGG_RESPONSE)
        self.assertEqual(index_names, ['classification_type', 'global_metrics.field.name'])
        self.assertEqual(len(index), len(values))
        self.assertEqual(len(index), 10)
        self.assertEqual(index, sample.EXPECTED_TABULAR_INDEX)
        self.assertEqual(values, sample.EXPECTED_TABULAR_VALUES)

    def test_parse_as_dataframe(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        df = my_agg._serialize_response_as_dataframe(sample.ES_AGG_RESPONSE)
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

        with self.assertRaises(NodeIDAbsentError) as e:
            my_agg._validate_aggs_parent_id("non-existing-node")
        self.assertEqual(e.exception.args, ("Node 'non-existing-node' is not in the tree",))

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

        # TODO - pipeline aggregation under metric agg

    def test_agg_method(self):
        pass

    def test_groupby_method(self):
        pass

    def test_mapping_from_init(self):
        agg_from_dict_mapping = Agg(mapping=MAPPING)
        agg_from_tree_mapping = Agg(mapping=Mapping(body=MAPPING))
        agg_from_obj_mapping = Agg(mapping=IMapping(tree=Mapping(body=MAPPING)))
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.__repr__(),
            agg_from_tree_mapping.tree_mapping.__repr__()
        )
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.__repr__(),
            agg_from_obj_mapping.tree_mapping.__repr__()
        )
        self.assertIsInstance(agg_from_dict_mapping, Agg)
        self.assertIsInstance(agg_from_tree_mapping, Agg)
        self.assertIsInstance(agg_from_obj_mapping, Agg)

    def test_set_mapping(self):
        agg_from_dict_mapping = Agg() \
            .set_mapping(mapping=MAPPING)
        agg_from_tree_mapping = Agg() \
            .set_mapping(mapping=Mapping(body=MAPPING))
        agg_from_obj_mapping = Agg() \
            .set_mapping(mapping=IMapping(tree=Mapping(body=MAPPING), client=None))
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.__repr__(),
            agg_from_tree_mapping.tree_mapping.__repr__()
        )
        self.assertEqual(
            agg_from_dict_mapping.tree_mapping.__repr__(),
            agg_from_obj_mapping.tree_mapping.__repr__()
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
                        "interval": "1w"
                    }
                }
            }
        )
        self.assertEqual(
            agg.__str__(),
            """<Aggregation>
[week] date_histogram
└── [nested_below_week] nested
    └── [local_metrics.field_class.name] terms
        └── [min_f1_score] min
"""
        )

    def test_groupby_and_agg(self):
        agg = sample.get_wrapper_declared_agg()
        self.assertEqual(agg.query_dict(), sample.EXPECTED_AGG_QUERY)
        self.assertEqual(agg.__str__(), sample.EXPECTED_REPR)

    def test_groupby_insert_below(self):
        a1 = Agg(
            Terms('A', field='A', aggs=[
                Terms('B', field='B'),
                Terms('C', field='C')
            ]))
        self.assertEqual(a1.__repr__(), '''<Aggregation>
[A] terms
├── [B] terms
└── [C] terms
''')

        self.assertEqual(a1.groupby(
            by=Terms('D', field='D'),
            insert_below='A'
        ).__repr__(), '''<Aggregation>
[A] terms
└── [D] terms
    ├── [B] terms
    └── [C] terms
''')
        self.assertEqual(a1.groupby(
            by=[Terms('D', field='D'), Terms('E', field='E')],
            insert_below='A'
        ).__repr__(), '''<Aggregation>
[A] terms
└── [D] terms
    └── [E] terms
        ├── [B] terms
        └── [C] terms
''')
        self.assertEqual(a1.groupby(
            by=Terms('D', field='D', aggs=Terms('E', field='E')),
            insert_below='A'
        ).__repr__(), '''<Aggregation>
[A] terms
└── [D] terms
    └── [E] terms
        ├── [B] terms
        └── [C] terms
''')

    def test_groupby_insert_above(self):
        a1 = Agg(
            Terms('A', field='A', aggs=[
                Terms('B', field='B'),
                Terms('C', field='C')
            ]))
        self.assertEqual(a1.__repr__(), '''<Aggregation>
[A] terms
├── [B] terms
└── [C] terms
''')

        self.assertEqual(a1.groupby(
            by=Terms('D', field='D'),
            insert_above='B'
        ).__repr__(), '''<Aggregation>
[A] terms
├── [C] terms
└── [D] terms
    └── [B] terms
''')
        self.assertEqual(a1.groupby(
            by=[Terms('D', field='D'), Terms('E', field='E')],
            insert_above='B'
        ).__repr__(), '''<Aggregation>
[A] terms
├── [C] terms
└── [D] terms
    └── [E] terms
        └── [B] terms
''')
        self.assertEqual(a1.groupby(
            by=Terms('D', field='D', aggs=Terms('E', field='E')),
            insert_above='B'
        ).__repr__(), '''<Aggregation>
[A] terms
├── [C] terms
└── [D] terms
    └── [E] terms
        └── [B] terms
''')
        # above root
        self.assertEqual(a1.groupby(
            by=Terms('D', field='D', aggs=Terms('E', field='E')),
            insert_above='A'
        ).__repr__(), '''<Aggregation>
[D] terms
└── [E] terms
    └── [A] terms
        ├── [B] terms
        └── [C] terms
''')

    def test_agg_insert_below(self):
        a1 = Agg(
            Terms('A', field='A', aggs=[
                Terms('B', field='B'),
                Terms('C', field='C')
            ]))
        self.assertEqual(a1.__repr__(), '''<Aggregation>
[A] terms
├── [B] terms
└── [C] terms
''')

        self.assertEqual(a1.agg(
            arg=Terms('D', field='D'),
            insert_below='A'
        ).__repr__(), '''<Aggregation>
[A] terms
├── [B] terms
├── [C] terms
└── [D] terms
''')
        self.assertEqual(a1.agg(
            arg=[Terms('D', field='D'), Terms('E', field='E')],
            insert_below='A'
        ).__repr__(), '''<Aggregation>
[A] terms
├── [B] terms
├── [C] terms
├── [D] terms
└── [E] terms
''')

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
                    filter={
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

    def test_query(self):
        agg = Agg(client=None, index_name='some_index')

        new_agg = agg\
            .query({'term': {'user': {'value': 1}}})\
            .query({'bool': {'must': [
                {'range': {'other_field': {'gt': 2}}},
                {'term': {'another_field': {'value': 'hi'}}}
            ]}})

        self.assertEqual(agg._query.query_dict(), None)
        self.assertEqual(
            new_agg._query.query_dict(),
            {
                'bool': {
                    'must': [
                        {'range': {'other_field': {'gt': 2}}},
                        {'term': {'another_field': {'value': 'hi'}}},
                        {'term': {'user': {'value': 1}}}
                    ]
                }
            }
        )
