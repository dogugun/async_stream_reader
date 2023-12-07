import unittest

from utils.utils import parse_id, create_timestamp


class TestUtilsFunctions(unittest.TestCase):

    def test_flatten_json(self):
        byte_object = b'id:test_id\nevent:tesp_event'
        self.assertEqual(parse_id(byte_object), "test_id")
        self.assertEqual(parse_id(b""), "")

    def test_create_timestamp(self):

        self.assertIsNotNone(create_timestamp())
        self.assertEqual(len(create_timestamp()), 15)


