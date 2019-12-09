#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from mock import Mock

from pandagg.exceptions import AbsentMappingFieldError
from pandagg.mapping.field_agg_factory import field_classes_per_name
from pandagg.mapping.mapping import Mapping, MappingTree, MappingNode, ClientBoundMapping
from tests.mapping.mapping_example import MAPPING, EXPECTED_MAPPING_TREE_REPR


class MappingTreeTestCase(TestCase):
    """All tree logic is tested in utils.
    Here, check that:
     - a dict mapping is correctly parsed into a tree,
     - it has the right representation.
    """

    def test_node_repr(self):
        node = MappingNode(
            path='path.to.field',
            depth=3,
            body={'type': 'boolean'}
        )
        self.assertEqual(
            node.__str__(),
            u"""<Mapping Field path.to.field> of type boolean:
{
    "type": "boolean"
}"""
        )

    def test_parse_tree_from_dict(self):
        mapping_tree = MappingTree(mapping_detail=MAPPING)

        self.assertEqual(mapping_tree.__str__(), EXPECTED_MAPPING_TREE_REPR)

    def test_nesteds_applied_at_field(self):
        mapping_tree = MappingTree(mapping_detail=MAPPING)

        self.assertEqual(mapping_tree.nested_at_field('classification_type'), None)
        self.assertEqual(mapping_tree.list_nesteds_at_field('classification_type'), [])
        self.assertEqual(mapping_tree.nested_at_field('date'), None)
        self.assertEqual(mapping_tree.list_nesteds_at_field('date'), [])
        self.assertEqual(mapping_tree.nested_at_field('global_metrics'), None)
        self.assertEqual(mapping_tree.list_nesteds_at_field('global_metrics'), [])

        self.assertEqual(mapping_tree.nested_at_field('local_metrics'), 'local_metrics')
        self.assertEqual(mapping_tree.list_nesteds_at_field('local_metrics'), ['local_metrics'])
        self.assertEqual(mapping_tree.nested_at_field('local_metrics.dataset.support_test'), 'local_metrics')
        self.assertEqual(mapping_tree.list_nesteds_at_field('local_metrics.dataset.support_test'), ['local_metrics'])

    def test_mapping_type_of_field(self):
        mapping_tree = MappingTree(mapping_detail=MAPPING)
        with self.assertRaises(AbsentMappingFieldError):
            self.assertEqual(mapping_tree.mapping_type_of_field('yolo'), False)

        self.assertEqual(mapping_tree.mapping_type_of_field('global_metrics'), 'object')
        self.assertEqual(mapping_tree.mapping_type_of_field('local_metrics'), 'nested')
        self.assertEqual(mapping_tree.mapping_type_of_field('global_metrics.field.name.raw'), 'keyword')
        self.assertEqual(mapping_tree.mapping_type_of_field('local_metrics.dataset.support_test'), 'integer')


class MappingTestCase(TestCase):
    """Check that calling a tree will return its root node.
    """

    def test_mapping_aggregations(self):
        mapping_tree = MappingTree(mapping_detail=MAPPING)
        # check that leaves are expanded, based on 'field_name' attribute of nodes
        mapping = Mapping(tree=mapping_tree, depth=1)
        for field_name in ('classification_type', 'date', 'global_metrics', 'id', 'language', 'local_metrics', 'workflow'):
            self.assertTrue(hasattr(mapping, field_name))

        workflow = mapping.workflow
        workflow_node = workflow()
        self.assertTrue(isinstance(workflow_node, MappingNode))


class ClientBoundMappingTestCase(TestCase):
    """Check that when reaching leaves (fields without children) leaves have the "a" attribute that can generate
    aggregations on that field type.
    """
    def test_client_bound(self):
        client_mock = Mock(spec=['search'])
        es_response_mock = {
            "_shards": {
                "failed": 0,
                "successful": 135,
                "total": 135
            },
            "aggregations": {
                "terms_agg": {
                    "buckets": [
                        {
                            "doc_count": 25,
                            "key": 1
                        },
                        {
                            "doc_count": 50,
                            "key": 2
                        }
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 4
                }
            },
            "hits": {
                "hits": [],
                "max_score": 0.0,
                "total": 300
            },
            "timed_out": False,
            "took": 30
        }
        client_mock.search = Mock(return_value=es_response_mock)

        mapping_tree = MappingTree(mapping_detail=MAPPING)
        client_bound_mapping = ClientBoundMapping(
            client=client_mock,
            tree=mapping_tree,
            depth=1,
            index_name='classification_report_index_name'
        )

        workflow_field = client_bound_mapping.workflow
        self.assertTrue(hasattr(workflow_field, 'a'))
        # workflow type is String
        self.assertIsInstance(workflow_field.a, field_classes_per_name['keyword'])

        response = workflow_field.a.terms(
            size=20,
            output=None,
            query={'term': {'classification_type': 'multiclass'}}
        )
        self.assertEqual(response, [
            (1, {"doc_count": 25, "key": 1}),
            (2, {"doc_count": 50, "key": 2}),
        ])
        client_mock.search.assert_called_once()
        client_mock.search.assert_called_with(
            body={
                'aggs': {'terms_agg': {'terms': {'field': 'workflow', 'size': 20}}},
                'size': 0,
                'query': {'term': {'classification_type': 'multiclass'}}
            },
            index='classification_report_index_name'
        )
