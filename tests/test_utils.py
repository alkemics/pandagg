#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.utils import Obj
from unittest import TestCase


class UtilsTestCase(TestCase):

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

        self.assertEqual(obj.__repr__(), "<Obj> ['some-key', 'some_key_2', 'some_key']")

    def test_obj_init(self):
        obj = Obj(yolo="yolo value", toto="toto value")
        self.assertEqual(obj.yolo, "yolo value")
        self.assertEqual(obj.toto, "toto value")

        # with underscores
        obj2 = Obj(_yolo="yolo value", _toto="toto value")
        self.assertEqual(obj2._yolo, "yolo value")
        self.assertEqual(obj2._toto, "toto value")

        # with double underscore
        with self.assertRaises(AssertionError):
            Obj(__d="trying to mess around")

    def test_obj_inherit(self):
        class MyCustomObj(Obj):
            pass

        obj = MyCustomObj()

        obj['some-key'] = 'some-value'
        self.assertEqual(obj['some-key'], 'some-value')
        # still stored in mangled '__d' attribute of initial Obj class -> '_Obj__d'
        self.assertIn('some-key', obj['_Obj__d'])
        self.assertEqual(obj['_Obj__d']['some-key'], 'some-value')

        self.assertEqual(obj.__repr__(), "<MyCustomObj> ['some-key']")

        class MyOtherCustomObj(Obj):
            _REPR_NAME = 'CallMe'

        other_obj = MyOtherCustomObj(maybe='...')
        self.assertEqual(other_obj.__repr__(), "<CallMe> ['maybe']")
