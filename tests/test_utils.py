#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from pandagg.tree import Tree
from pandagg.utils import Obj, TreeBasedObj
from unittest import TestCase


class ObjTestCase(TestCase):

    def test_set_obj_attribute(self):
        obj = Obj()

        # valid key
        # set by __setitem__
        obj['some_key'] = 'some_value'
        self.assertEqual(obj.some_key, 'some_value')
        self.assertEqual(obj['some_key'], 'some_value')
        self.assertIn('some_key', dir(obj))

        # set by __setattr__
        obj.some_key_2 = 'some_value_2'
        self.assertEqual(obj.some_key_2, 'some_value_2')
        self.assertEqual(obj['some_key_2'], 'some_value_2')
        self.assertIn('some_key_2', dir(obj))

        # key containing '-' character can not be set as attribute
        obj['some-key'] = 'some-value'
        self.assertEqual(obj['some-key'], 'some-value')
        # internally stored in mangled '__d' attribute -> '_Obj__d'
        self.assertIn('some-key', obj['_Obj__d'])
        self.assertEqual(obj['_Obj__d']['some-key'], 'some-value')

        self.assertEqual(obj.__str__(), "<Obj> ['some-key', 'some_key', 'some_key_2']")

    def test_obj_init(self):
        obj = Obj(yolo="yolo value", toto="toto value")
        self.assertEqual(obj.yolo, "yolo value")
        self.assertEqual(obj.toto, "toto value")

        # with underscores
        obj2 = Obj(_yolo="yolo value", _toto="toto value")
        self.assertEqual(obj2._yolo, "yolo value")
        self.assertEqual(obj2._toto, "toto value")

        # unauthorized attributes/keys
        with self.assertRaises(AssertionError):
            Obj(__d="trying to mess around")
        with self.assertRaises(AssertionError):
            obj = Obj()
            obj['__d'] = 'yolo'
        with self.assertRaises(AssertionError):
            obj = Obj()
            obj[23] = 'yolo'
        with self.assertRaises(AssertionError):
            obj = Obj()
            obj[None] = 'yolo'

        # if other that string is accepted
        class FlexObj(Obj):
            _STRING_KEY_CONSTRAINT = False

        # unauthorized attributes/keys
        with self.assertRaises(AssertionError):
            FlexObj(__d="trying to mess around")
        with self.assertRaises(AssertionError):
            obj = FlexObj()
            obj['__d'] = 'yolo'
        # authorized:
        obj = FlexObj()
        obj[23] = 'yolo'
        self.assertEqual(obj[23], 'yolo')

        obj = FlexObj()
        obj[None] = 'yolo'
        self.assertEqual(obj[None], 'yolo')

    def test_obj_inherit(self):
        class MyCustomObj(Obj):
            pass

        obj = MyCustomObj()

        obj['some-key'] = 'some-value'
        self.assertEqual(obj['some-key'], 'some-value')
        # still stored in mangled '__d' attribute of initial Obj class -> '_Obj__d'
        self.assertIn('some-key', obj['_Obj__d'])
        self.assertEqual(obj['_Obj__d']['some-key'], 'some-value')

        self.assertEqual(obj.__str__(), "<MyCustomObj> ['some-key']")

        class MyOtherCustomObj(Obj):
            _REPR_NAME = 'CallMe'

        other_obj = MyOtherCustomObj(maybe='...')
        self.assertEqual(other_obj.__str__(), "<CallMe> ['maybe']")


class TreeBasedObjTestCase(TestCase):

    def setUp(self):
        """Builds following tree:
        Harry
        ├── Bill
        │   └── George
        └── Jane
            └── Diane
        """
        tree = Tree(identifier="family tree")
        tree.create_node(identifier="Harry", tag="harry")
        tree.create_node(identifier="Jane", tag="jane", parent="Harry")
        tree.create_node(identifier="Bill", tag="bill", parent="Harry")
        tree.create_node(identifier="Diane", tag="diane", parent="Jane")
        tree.create_node(identifier="George", tag="george", parent="Bill")
        self.tree = tree

    def tearDown(self):
        self.tree = None

    def test_tree(self):
        """Check expand and shallow copy.
        - check that an object expand its tree's chilren in its attributes
        - check that no deep copy is made when expanding trees. More precisely, since we might manipulate
        lots of nodes, we want to check that nodes are never copied. Instead their reference is passed to different
        trees.
        """
        # if no depth is passed, the tree does not expand
        no_expand_obj = TreeBasedObj(tree=self.tree)
        for child in ('bill', 'jane'):
            self.assertFalse(hasattr(no_expand_obj, child))

        # if depth is passed, the tree expand
        obj = TreeBasedObj(tree=self.tree, depth=1)
        for child in ('bill', 'jane'):
            self.assertTrue(hasattr(obj, child))

        # when accessing child, check that it auto-expands children
        obj.jane.diane
        bill_obj = obj.bill
        """
        Bill
        └── George
        """
        self.assertTrue(hasattr(bill_obj, 'george'))
        # check that initial tree, and child tree reference the same nodes
        self.assertIs(bill_obj._tree["Bill"], obj._tree["Bill"])
        self.assertIs(bill_obj._tree["George"], obj._tree["George"])

        # test representations
        self.assertEqual(
            obj.__str__(),
            text(u"""
<TreeBasedObj>
harry
├── bill
│   └── george
└── jane
    └── diane
""")
        )
        self.assertEqual(
            bill_obj.__str__(),
            text(u"""
<TreeBasedObj subpart: bill>
bill
└── george
""")
        )
        self.assertEqual(
            bill_obj.george.__str__(),
            text(u"""
<TreeBasedObj subpart: bill.george>
george
""")
        )
