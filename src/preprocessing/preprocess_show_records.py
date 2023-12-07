import shutil

import numpy as np

from logger.logger import get_module_logger
from persistence.persistence_ops import get_all_show_records, save_to_processed
import pandas as pd
from pandas import json_normalize
import json
import os

BASEDIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
LOGGER = get_module_logger(__name__)

def flatten_json(json_str):
    try:
        return json_normalize(json.loads(json_str))
    except (json.JSONDecodeError, TypeError):
        return pd.DataFrame([np.nan])

def generate_denormalized_data(timestamp):
    records = get_all_show_records()
    if len(records)>0:
        df = pd.DataFrame([(rec.split(b"\n")[0].decode("utf-8"), rec.split(b"\n")[1].decode("utf-8"), rec.split(b"\n")[2].decode("utf-8")) for rec in records])
        df.columns = ["id", "event", "data"]
        df["id"] = df["id"].str.replace("id:", "")
        df["event"] = df["event"].str.replace("event:", "")
        df["data"] = df["data"].str.replace("data:", "")
        df["data"] = df["data"].apply(lambda x: x.replace('null', '""') if pd.notnull(x) else x)

        df_data = pd.concat([flatten_json(row) for row in df["data"].values], axis=0, ignore_index=True)
        if 0 in df_data.columns:
            df_data = df_data.drop([0], axis=1)

        df = pd.concat([df[["id", "event"]], df_data], axis=1)

        df = df[~df["event_date"].isna()]

        save_to_processed(df, timestamp)
        return True
    else:
        return False


def archive_raw_data():
    raw_path = os.path.join(BASEDIR, "data", "raw")
    arc_path = os.path.join(BASEDIR, "data", "archive")

    files_to_move = os.listdir(raw_path)

    os.makedirs(arc_path, exist_ok=True)

    for file_name in files_to_move:
        source_path = os.path.join(raw_path, file_name)
        destination_path = os.path.join(arc_path, file_name)
        shutil.move(source_path, destination_path)
    LOGGER.info(f"Archived raw data")