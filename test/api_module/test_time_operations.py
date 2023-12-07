import unittest


import numpy as np
import pandas as pd

from api_module.calculations import get_first_actor
from api_module.response_generation import generate_resp_for_int, generate_resp_successful_streams
from datetime import datetime
import pytz

from api_module.time_operations import adjust_datetime_to_cet, get_timezone_from_country, calculate_age_from_birthdate


class TestTimeOperationsFunctions(unittest.TestCase):

    def test_adjust_datetime_to_cet(self):
        event_date = datetime(year=2023, month=12, day=7, hour = 14, minute=0, second=0)
        tz_code = "Asia/Istanbul"
        self.assertNotEqual(adjust_datetime_to_cet(event_date, tz_code), event_date)
        self.assertEqual(adjust_datetime_to_cet(event_date, tz_code), pytz.timezone(tz_code).localize(event_date))

    def test_get_timezone_from_country(self):
        c_list = ["US", "TR", "SI"]
        tzones = ["America/Los_Angeles", "Europe/Istanbul", "Europe/Ljubljana"]
        for i,c in enumerate(c_list):
            self.assertEqual(get_timezone_from_country(c), tzones[i])

    def test_calculate_age_from_birthdate(self):
        self.assertIsNotNone(calculate_age_from_birthdate("30/06/1987"))
        self.assertGreater(calculate_age_from_birthdate("30/06/1987"), 35)