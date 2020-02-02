
from pandagg.node.response.bucket import Bucket
from unittest import TestCase


class ResponseBucketTestCase(TestCase):

    def test_bucket(self):
        bucket = Bucket(depth=0, level='windows.color', key='green', value=32)

        self.assertEqual(
            bucket.display_name,
            'windows.color=green'
        )

        self.assertEqual(
            bucket.display_name_with_value,
            'windows.color=green                                       32'
        )

        self.assertEqual(
            Bucket(depth=5, level='windows.color', key='green', value=32).display_name_with_value,
            'windows.color=green                   32'
        )
