from unittest import TestCase

from pandagg.utils import ordered


class PandaggTestCase(TestCase):
    def assertQueryEqual(self, first, second, msg=None):
        self.assertIsInstance(first, dict, msg)
        self.assertIsInstance(second, dict, msg)
        fo = ordered(first)
        fs = ordered(second)
        # preserve regular formatting
        if not fo == fs:
            self.assertEqual(first, second, msg)
