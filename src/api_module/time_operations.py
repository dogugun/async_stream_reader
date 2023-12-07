import numpy as np
from datetime import datetime
from variables.variables import COUNTRY_TZ_DICT, BIRTHDATE_FORMAT
from pytz import country_timezones
import pytz

def adjust_datetime_to_cet(event_date, tz_code):
    loc_dt_obj = pytz.timezone(tz_code).localize(event_date)
    adj_dt_obj = loc_dt_obj.astimezone(pytz.timezone('Europe/Paris'))
    return adj_dt_obj


def get_timezone_from_country(country):
    if country is np.nan:
        return np.nan
    if country in COUNTRY_TZ_DICT:
        tz_code = COUNTRY_TZ_DICT[country]
    else:
        tz_code = country_timezones(country)[0]
    return tz_code


def calculate_age_from_birthdate(birthdate):
    birth_date = datetime.strptime(birthdate, BIRTHDATE_FORMAT)
    current_date = datetime.now()
    age = current_date.year - birth_date.year - (
                (current_date.month, current_date.day) < (birth_date.month, birth_date.day))

    return age