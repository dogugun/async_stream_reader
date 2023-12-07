import numpy as np
import pandas as pd

from api_module.response_generation import generate_resp_for_int, generate_resp_successful_streams, generate_resp_object
from api_module.time_operations import calculate_age_from_birthdate, get_timezone_from_country, adjust_datetime_to_cet

from persistence.persistence_ops import get_preprocessed_records

from utils.utils import generate_http_404_error
from variables.variables import PLATFORM_SYTFLIX, RECENT_YEAR, DATETIME_FORMAT


def get_processed_data():
    df = get_preprocessed_records()
    if df is not None:
        df = process_required_columns(df.copy(deep=False))
        return df


def process_required_columns(df):
    df["user_name"] = df["user.first_name"] + " " + df["user.last_name"]
    df["age"] = df["user.date_of_birth"].apply(calculate_age_from_birthdate)
    df["first_actor"] = df["show.cast"].apply(get_first_actor)

    df["event_date"] = pd.to_datetime(df['event_date'], format=DATETIME_FORMAT)
    df["timezone"] = df["user.country"].apply(get_timezone_from_country).fillna('Europe/Paris')
    df["event_date"] = df.apply(lambda row: adjust_datetime_to_cet(row['event_date'], row['timezone']),
                                          axis=1)
    return df


def get_first_actor(actor_list):
    if actor_list is np.nan:
        return np.nan
    else:
        return actor_list.split(",")[0]


def get_user_aggregations():
    df = get_processed_data()
    if df is None:
        raise generate_http_404_error()

    person_list = []

    user_ids = df["user.id"].unique()
    for uid in user_ids:
        df_user = df[df["user.id"] == uid]
        df_info = df_user[["user.id", "user_name", "age", "event", "show.platform", "show.title", "first_actor", "show.show_id", "event_date"]]
        person = generate_resp_object(df_info)
        person_list.append(person)

    return person_list


def get_recent_shows(year=RECENT_YEAR):
    df = get_processed_data()
    if df is None:
        raise generate_http_404_error()
    df_recent = df[df["show.release_year"]>=year]
    recent_show_count = df_recent["show.title"].nunique()
    resp_obj = generate_resp_for_int("recent_shows", recent_show_count)
    return resp_obj


def get_successful_streams():
    df = get_processed_data()
    if df is None:
        raise generate_http_404_error()
    df = df.sort_values(["user.id", "event_date"])
    df_cons = df[["event", "event_date", "show.show_id", "user.id", "show.platform"]]

    df_cons["event_next"] = df_cons["event"].shift(-1)
    df_cons["event_date_next"] = df_cons["event_date"].shift(-1)
    df_cons["show.show_id_next"] = df_cons["show.show_id"].shift(-1)
    df_cons["user.id_next"] = df_cons["user.id"].shift(-1)
    df_cons["show.platform_next"] = df_cons["show.platform"].shift(-1)
    df_cons = df_cons[(df_cons["event"] == "stream-started") & (df_cons["event_next"] == "stream-finished")]
    df_cons = df_cons[(df_cons["event"] == "stream-started")
                      & (df_cons["event_next"] == "stream-finished")
                      & (df_cons["user.id"] == df_cons["user.id_next"])
                      & (df_cons["show.show_id"] == df_cons["show.show_id_next"])
                      & (df_cons["show.platform"] == df_cons["show.platform_next"])]

    resp_obj = generate_resp_successful_streams(df_cons.groupby("user.id").size().reset_index().rename(columns={0: "count"}))
    return resp_obj


def get_start_event_rate(platform= PLATFORM_SYTFLIX):
    df = get_processed_data()
    if df is None:
        raise generate_http_404_error()
    df = df[df["show.platform"] == platform]
    start_percentage = df[df["event"]== "stream-started"].shape[0]/df.shape[0]*100
    resp_obj = generate_resp_for_int("start_event_rate(%)", start_percentage)
    return resp_obj


#print(get_start_event_rate())

#get_user_aggregations()