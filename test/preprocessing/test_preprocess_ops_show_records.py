import unittest


import numpy as np
import pandas as pd

from api_module.calculations import get_first_actor
from api_module.response_generation import generate_resp_for_int, generate_resp_successful_streams
from datetime import datetime
import pytz

from api_module.time_operations import adjust_datetime_to_cet, get_timezone_from_country, calculate_age_from_birthdate
from preprocessing.preprocess_show_records import flatten_json


class TestPreprocessOpsShowRecordsFunctions(unittest.TestCase):

    def test_flatten_json(self):
        json_str = """{"user": "dogugun", "age": 36}"""
        self.assertIsInstance(flatten_json(json_str), pd.DataFrame)
        self.assertEquals(flatten_json(json_str).shape, (1,2))
        self.assertTrue(bool(flatten_json(None)[0].isna()[0]))
