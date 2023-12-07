import unittest


import numpy as np
import pandas as pd

from api_module.calculations import get_first_actor
from api_module.response_generation import generate_resp_for_int, generate_resp_successful_streams


class TestCalculationsFunctions(unittest.TestCase):

    def test_generate_resp_for_int(self):
        self.assertEqual(generate_resp_for_int("key", 1), {"key": "1"})

    def test_generate_resp_successful_streams(self):
        data = {
            'user.id': [10, 42],
            'count': [1, 2],
        }

        df = pd.DataFrame(data)
        self.assertIsNotNone(generate_resp_successful_streams(df))
        self.assertEqual(len(generate_resp_successful_streams(df)), 2)
        self.assertIsInstance(generate_resp_successful_streams(df)[0], dict)


