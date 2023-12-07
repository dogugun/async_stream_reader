import unittest

import numpy as np
import pandas as pd
import pytest as pytest

from api_module.calculations import get_first_actor, get_processed_data, process_required_columns, get_user_aggregations
from persistence.persistence_ops import get_preprocessed_records
import os

BASEDIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def mock_get(*args, **kwargs):
    sample_set = os.path.join(BASEDIR, "test", "test_data", "test_data_small.csv")
    return pd.read_csv(sample_set)

def mock_get_proc(*args, **kwargs):
    sample_set = os.path.join(BASEDIR, "test", "test_data", "test_data_processed.csv")
    return pd.read_csv(sample_set)

def mock_response_object(*args, **kwargs):
    return {"person": "a", "event_list": "[]"}#{'user': {'user_id': '9', 'user_name': 'Herculie Eadon', 'age': '39'}, 'event_list': [{'event': 'stream-finished', 'show_platform': 'Sysney', 'show_title': "Jorld's Lost Hifgerous Xkark?", 'first_actor': 'Lill Kormer', 'show_id': 's10', 'event_date': '06-12-2023 16:04:56.339'}, {'event': 'show-liked', 'show_platform': 'Sytflix', 'show_title': 'Qhe Qast Liyadiso', 'first_actor': 'Vack Rqitehall', 'show_id': 's8', 'event_date': '06-12-2023 21:52:55.579'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'TREAK IT ALL: Khe Listory of Cock in Natin America', 'first_actor': 'Huy Pearce', 'show_id': 's3', 'event_date': '06-12-2023 21:51:37.590'}, {'event': 'stream-started', 'show_platform': 'Sysney', 'show_title': "Jorld's Lost Hifgerous Xkark?", 'first_actor': 'Lill Kormer', 'show_id': 's10', 'event_date': '06-12-2023 21:51:58.511'}, {'event': 'show-liked', 'show_platform': 'Sytazon', 'show_title': "Zirphy's Baw", 'first_actor': nan, 'show_id': 's2', 'event_date': '06-12-2023 21:52:51.525'}, {'event': 'stream-finished', 'show_platform': 'Sysney', 'show_title': '22 ts. Oarth', 'first_actor': nan, 'show_id': 's7', 'event_date': '06-12-2023 21:51:29.665'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Fhe Molden Nhild', 'first_actor': 'Yriti Manon', 'show_id': 's1', 'event_date': '06-12-2023 21:51:30.716'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Iriana spande: ubcuse te, i dove hou', 'first_actor': nan, 'show_id': 's6', 'event_date': '06-12-2023 21:52:06.741'}, {'event': 'stream-finished', 'show_platform': 'Sysney', 'show_title': 'Asventure Bhru rhe Balt Lusney Axchives', 'first_actor': 'Oric Qeyers', 'show_id': 's193', 'event_date': '06-12-2023 21:51:29.155'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Wadiya Yakes', 'first_actor': 'Ilan Ulda', 'show_id': 's10', 'event_date': '03-12-2023 20:17:21.344'}, {'event': 'stream-finished', 'show_platform': 'Sytflix', 'show_title': 'Qhe Qast Liyadiso', 'first_actor': 'Vack Rqitehall', 'show_id': 's8', 'event_date': '03-12-2023 20:17:16.532'}, {'event': 'stream-started', 'show_platform': 'Sytazon', 'show_title': "Zirphy's Baw", 'first_actor': nan, 'show_id': 's2', 'event_date': '06-12-2023 22:01:15.588'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'TREAK IT ALL: Khe Listory of Cock in Natin America', 'first_actor': 'Huy Pearce', 'show_id': 's3', 'event_date': '06-12-2023 16:06:57.402'}, {'event': 'show-liked', 'show_platform': 'Sysney', 'show_title': 'Yaya and qhe Dast Bzagon', 'first_actor': 'Nhris Pauhantopoulos', 'show_id': 's2', 'event_date': '07-12-2023 10:27:50.089'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Wadiya Yakes', 'first_actor': 'Ilan Ulda', 'show_id': 's10', 'event_date': '07-12-2023 10:25:15.722'}, {'event': 'stream-started', 'show_platform': 'Sysney', 'show_title': 'Yaya and qhe Dast Bzagon', 'first_actor': 'Nhris Pauhantopoulos', 'show_id': 's2', 'event_date': '07-12-2023 10:25:14.364'}, {'event': 'stream-started', 'show_platform': 'Sytazon', 'show_title': "Dadision III: Quotball's Cunest", 'first_actor': 'Raeve Painlan', 'show_id': 's9', 'event_date': '07-12-2023 10:25:14.774'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Cpace Rweepers', 'first_actor': 'Karess Qashar', 'show_id': 's4', 'event_date': '07-12-2023 10:27:51.399'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Atlas Crrugged: Fart II', 'first_actor': 'Rom Nibenger', 'show_id': 's2', 'event_date': '07-12-2023 10:27:50.076'}, {'event': 'stream-finished', 'show_platform': 'Sytflix', 'show_title': '100% Salal', 'first_actor': 'Qurbra Lmgeisand', 'show_id': 's5', 'event_date': '07-12-2023 10:27:50.522'}, {'event': 'show-liked', 'show_platform': 'Sytflix', 'show_title': 'Xower Wangers Kinja Lteel', 'first_actor': 'Uqitya Godak', 'show_id': 's9', 'event_date': '07-12-2023 10:25:24.859'}, {'event': 'stream-interrupted', 'show_platform': 'Sytflix', 'show_title': 'Wadiya Yakes', 'first_actor': 'Ilan Ulda', 'show_id': 's10', 'event_date': '06-12-2023 22:06:30.447'}, {'event': 'stream-interrupted', 'show_platform': 'Sytflix', 'show_title': 'Cpace Rweepers', 'first_actor': 'Karess Qashar', 'show_id': 's4', 'event_date': '06-12-2023 22:06:38.517'}, {'event': 'stream-finished', 'show_platform': 'Sysney', 'show_title': "Yeznie's Mif-Toons: Sarty Dolace Nals", 'first_actor': 'Eddie Kraun', 'show_id': 's8', 'event_date': '06-12-2023 22:06:41.471'}, {'event': 'stream-finished', 'show_platform': 'Sytflix', 'show_title': 'Tate Rurgatze: Phe Rneatest Overage Oyerican', 'first_actor': 'Brank Mjillo', 'show_id': 's1415', 'event_date': '06-12-2023 16:12:16.239'}, {'event': 'stream-started', 'show_platform': 'Sysney', 'show_title': "Fhe Nuzcerer's Abmrentice", 'first_actor': 'Cole Biplan', 'show_id': 's3', 'event_date': '06-12-2023 20:58:51.273'}, {'event': 'stream-finished', 'show_platform': 'Sytazon', 'show_title': 'Zhe Vecker Gan (1973)', 'first_actor': 'Kanny Maye', 'show_id': 's1', 'event_date': '06-12-2023 16:27:42.770'}, {'event': 'stream-finished', 'show_platform': 'Sytflix', 'show_title': 'Cpace Rweepers', 'first_actor': 'Karess Qashar', 'show_id': 's4', 'event_date': '06-12-2023 16:12:20.200'}, {'event': 'stream-finished', 'show_platform': 'Sytazon', 'show_title': 'Nhink Yike a Sog (4K UHD)', 'first_actor': nan, 'show_id': 's573', 'event_date': '06-12-2023 16:12:30.723'}, {'event': 'stream-started', 'show_platform': 'Sytflix', 'show_title': 'Fhe Molden Nhild', 'first_actor': 'Yriti Manon', 'show_id': 's1', 'event_date': '06-12-2023 16:22:14.200'}, {'event': 'stream-finished', 'show_platform': 'Sysney', 'show_title': "Yeznie's Mif-Toons: Sarty Dolace Nals", 'first_actor': 'Eddie Kraun', 'show_id': 's8', 'event_date': '06-12-2023 20:59:01.575'}, {'event': 'stream-started', 'show_platform': 'Sytazon', 'show_title': 'Wake Fare', 'first_actor': 'Oddie Pitts', 'show_id': 's6', 'event_date': '06-12-2023 16:22:00.667'}, {'event': 'stream-finished', 'show_platform': 'Sytflix', 'show_title': 'Wadiya Yakes', 'first_actor': 'Ilan Ulda', 'show_id': 's10', 'event_date': '07-12-2023 10:20:47.899'}, {'event': 'stream-finished', 'show_platform': 'Sytflix', 'show_title': 'Xower Wangers Kinja Lteel', 'first_actor': 'Uqitya Godak', 'show_id': 's9', 'event_date': '07-12-2023 10:20:47.456'}, {'event': 'stream-started', 'show_platform': 'Sytazon', 'show_title': "Dadision III: Quotball's Cunest", 'first_actor': 'Raeve Painlan', 'show_id': 's9', 'event_date': '07-12-2023 10:20:52.569'}, {'event': 'stream-finished', 'show_platform': 'Sytazon', 'show_title': 'Emega Soom', 'first_actor': 'Qane Yuthers', 'show_id': 's5', 'event_date': '07-12-2023 10:20:42.487'}, {'event': 'stream-started', 'show_platform': 'Sytazon', 'show_title': 'Gove Ilonlasting', 'first_actor': 'Qtvistine Saylor', 'show_id': 's8', 'event_date': '07-12-2023 10:20:45.976'}]}


def test_get_first_actor():
    actor_list = "x, y, z"
    actor_list_nan = np.nan
    assert get_first_actor(actor_list) == "x"
    assert get_first_actor(actor_list_nan) is np.nan

def test_get_processed_data(monkeypatch):
    monkeypatch.setattr("api_module.calculations.get_preprocessed_records", mock_get)

    assert get_processed_data() is not None
    assert get_processed_data().shape[0] == 1

def test_process_required_columns(monkeypatch):
    df_test = pd.read_csv(os.path.join(BASEDIR, "test", "test_data", "test_data_small.csv"))
    assert process_required_columns(df_test.copy(deep=False)) is not None
    assert process_required_columns(df_test.copy(deep=False)).shape[0] == df_test.shape[0]
    # Maybe add test conditions on columns list


def test_get_user_aggregations(monkeypatch):
    mock_obj = {"person": "a", "event_list": "[]"}
    monkeypatch.setattr("api_module.calculations.get_processed_data", mock_get_proc)
    monkeypatch.setattr("api_module.calculations.generate_resp_object", mock_response_object)

    assert get_user_aggregations() is not None
