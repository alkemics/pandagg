#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase

from pandagg.mapping.types import field_classes_per_name
from pandagg.mapping.mapping import Mapping, MappingTree, MappingNode, ClientBoundMapping
from tests.mapping.mapping_example import MAPPING_NAME, MAPPING_DETAIL


class MappingTreeTestCase(TestCase):
    """All tree logic is tested in utils.
    Here, check that:
     - a dict mapping is correctly parsed into a tree,
     - it has the right representation.
    """

    def test_node_repr(self):
        node = MappingNode(field_path='path.to.field', field_name='field_name', depth=3, detail={'type': 'boolean'})
        self.assertEqual(
            node.__repr__(),
            u"""<Mapping Field path.to.field> of type boolean:
{
    "type": "boolean"
}"""
        )

    def test_parse_tree_from_dict(self):
        mapping_tree = MappingTree(mapping_name=MAPPING_NAME, mapping_detail=MAPPING_DETAIL)

        self.assertEqual(
            mapping_tree.__repr__().decode('utf-8'),
            u"""<MappingTree>
classification_report                                       
├── classification_type                                     String
├── date                                                    Date
├── global_metrics                                         {Object}
│   ├── field                                              {Object}
│   │   ├── id                                              Integer
│   │   ├── name                                            String
│   │   └── type                                            String
│   └── performance                                        {Object}
│       └── test                                           {Object}
│           ├── macro                                      {Object}
│           │   ├── f1_score                                Float
│           │   ├── precision                               Float
│           │   └── recall                                  Float
│           └── micro                                      {Object}
│               ├── f1_score                                Float
│               ├── precision                               Float
│               └── recall                                  Float
├── id                                                      String
├── language                                                String
├── local_metrics                                          [Nested]
│   ├── dataset                                            {Object}
│   │   ├── support_test                                    Integer
│   │   └── support_train                                   Integer
│   ├── field_class                                        {Object}
│   │   ├── id                                              Integer
│   │   └── name                                            String
│   └── performance                                        {Object}
│       └── test                                           {Object}
│           ├── f1_score                                    Float
│           ├── precision                                   Float
│           └── recall                                      Float
└── workflow                                                String
"""
        )


class MappingTestCase(TestCase):
    """Check that calling a tree will return its root node.
    """

    def test_mapping_aggregations(self):
        mapping_tree = MappingTree(mapping_name=MAPPING_NAME, mapping_detail=MAPPING_DETAIL)
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
        mapping_tree = MappingTree(mapping_name=MAPPING_NAME, mapping_detail=MAPPING_DETAIL)
        client_bound_mapping = ClientBoundMapping(client=None, tree=mapping_tree, depth=1)
        workflow_field = client_bound_mapping.workflow
        self.assertTrue(hasattr(workflow_field, 'a'))
        # workflow type is String
        self.assertIsInstance(workflow_field.a, field_classes_per_name['string'])
