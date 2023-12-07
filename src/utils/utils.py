import json
import logging
import re
import sys
import time
from datetime import datetime
from werkzeug.exceptions import HTTPException
import pytz
import os

from logger.logger import get_module_logger
from variables.variables import ERROR_MESSAGE_404, TIMESTAMP_FORMAT

BASEDIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
LOGGER = get_module_logger(__name__)

def create_data_folder():
    if not os.path.isdir(os.path.join(BASEDIR, "data")):
        data_path = os.path.join(BASEDIR, "data")
        raw_path = os.path.join(BASEDIR, "data", "raw")
        archive_path = os.path.join(BASEDIR, "data", "archive")
        processed_path = os.path.join(BASEDIR, "data", "processed")
        report_path = os.path.join(BASEDIR, "data", "report")
        os.makedirs(data_path, exist_ok=True)
        os.makedirs(raw_path, exist_ok=True)
        os.makedirs(archive_path, exist_ok=True)
        os.makedirs(processed_path, exist_ok=True)
        os.makedirs(report_path, exist_ok=True)
    else:
        LOGGER.info("Data folders exist, continuing")


# def format_response_body(chunk):
#     show_event=None
#     lines = chunk.decode('utf-8').split("\n")
#     if len(lines)>=3:
#         id = lines[0].split(":")[1]
#         name = lines[1].split(":")[1]
#         data = json.loads(lines[2].replace("data:", ""))
#
#         #show_event = ShowEvent(id, name, data)
#     return show_event

# def get_first_name_from_show_event(chunk):
#     st = time.time()
#     lines = chunk.decode('utf-8').split("\n")
#     res = json.loads(lines[2].replace("data:", ""))["user"]["first_name"]
#     print("Calculated first name in: ", time.time() - st)
#     return res



def parse_id(byte_object):
    id = byte_object.split(b"\n")[0].replace(b"id:", b"").decode("utf-8")
    return id


def create_timestamp():
    current_time = datetime.now()
    time_string = current_time.strftime(TIMESTAMP_FORMAT)
    return time_string


def display_countdown(seconds):
    for i in range(seconds, 0, -1):
        sys.stdout.write(f"\rCountdown: {i} seconds")
        sys.stdout.flush()
        time.sleep(1)

    sys.stdout.write("\rCountdown: 0 seconds\n")
    sys.stdout.flush()


def generate_http_404_error():
    e = HTTPException(description=ERROR_MESSAGE_404)
    e.code = 404
    return e

