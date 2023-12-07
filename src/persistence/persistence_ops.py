import pandas as pd
from utils.utils import parse_id
import os
BASEDIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


# CREATE
async def async_save_show_data(show_obj):
    id = parse_id(show_obj)
    save_path = os.path.join(BASEDIR, "data", "raw", id)
    with open(save_path, 'wb') as f:
        f.write(show_obj)

def save_show_data(show_obj):
    id = parse_id(show_obj)
    save_path = os.path.join(BASEDIR, "data", id)
    with open(save_path, 'wb') as f:
        f.write(show_obj)

def save_to_processed(df, timestamp):
    processed_path = os.path.join(BASEDIR, "data", "processed", f"processed_{timestamp}.csv")
    df.to_csv(processed_path, index = False)
    return True

# READ
def get_all_show_records():
    data_path = os.path.join(BASEDIR, "data", "raw")
    files = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f)) if not f.startswith(".")]

    all_contents = []

    for file_path in files:
        with open(os.path.join(data_path, file_path), 'rb') as file:
            file_content = file.read()
            all_contents.append(file_content)
    return all_contents


def get_preprocessed_records():
    data_path = os.path.join(BASEDIR, "data", "processed")
    files = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
    all_contents = []

    df_list = []
    for file_path in files:
        df = pd.read_csv(os.path.join(data_path, file_path))
        df_list.append(df)
    if len(df_list)>0:
        return pd.concat(df_list, axis=0)




