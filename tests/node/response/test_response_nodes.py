from pandagg.node.response.bucket import Bucket
from unittest import TestCase


class ResponseBucketTestCase(TestCase):
    def test_bucket(self):
        self.assertEqual(
            Bucket(level="windows.color", key="green", value=32).line_repr(depth=5),
            "windows.color=green                   32",
        )
