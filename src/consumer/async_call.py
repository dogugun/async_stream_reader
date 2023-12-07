import asyncio
import json
import datetime
import logging
import queue
import time
from threading import Event
import os
import aiohttp
from aiohttp import BasicAuth
from requests.auth import HTTPBasicAuth

from config._config import get_config
from logger.logger import get_module_logger
from persistence.persistence_ops import async_save_show_data
from utils.utils import create_timestamp
from variables.variables import TIMEOUT_MESSAGE, PLATFORM_LIST, TARGET_REACHED_MESSAGE, TARGET_NAME, \
    TARGET_COUNT_THRESHOLD, ACHIEVED_TARGET_COUNT_MESSAGE, PARSE_ERROR_MESSAGE, EXEC_REPORT_TITLE, EXEC_REPORT_LINE, \
    TIMEOUT_THRESHOLD

# Shared variable to count occurrences
target_field_count = 0

config = get_config()
LOGGER = get_module_logger(__name__)
BASEDIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# global sync_time_counter
# global async_time_counter

async_time_counter = {"sytflix": [], "sytazon": [], "sysney": []}


def report_exec_time(async_counter, timestamp):
    file_path = os.path.join(BASEDIR, "data", "report", f"exec_report_{timestamp}.txt")
    with open(file_path, 'w') as f:
        f.write(EXEC_REPORT_TITLE.format(timestamp = timestamp))
        for k in async_counter:
            f.write(EXEC_REPORT_LINE.format(source = k, second = async_counter[k]))

async def process_chunk(chunk, event):
    global target_field_count
    LOGGER.debug("entered process_chunk")
    try:
        first_name = json.loads(chunk.split(b"\n")[2].replace(b"data:", b""))["user"]["first_name"]
    except ValueError:
        LOGGER.error(PARSE_ERROR_MESSAGE)
    else:
        if first_name == TARGET_NAME:
            target_field_count += 1
            LOGGER.info(f"Found {TARGET_NAME} {target_field_count} times" )
            if target_field_count>=TARGET_COUNT_THRESHOLD:
                LOGGER.info(ACHIEVED_TARGET_COUNT_MESSAGE.format(target_name = TARGET_NAME,
                                                           target_count = TARGET_COUNT_THRESHOLD))
                event.set()
                return

async def read_stream(session, instance_id, source, result_queue, event):
    st_async_calls = time.time()

    global target_field_count

    stream_url = f'{config.stream_endpoint}{source}'
    auth = BasicAuth(config.stream_user, config.stream_password)

    async with session.get(stream_url, timeout=None, auth = auth) as response:

        if response.status != 200:
            LOGGER.error(f"Error: {response.status}")
        i = 0

        async for chunk in response.content.iter_any():
            if event.is_set():
                return result_queue
            result_queue.put(chunk)

            # ASSUMPTION: We are calculating the times consumers are running. Therefore async sub tasks are excluded
            asyncio.create_task(process_chunk(chunk, event))
            asyncio.create_task(async_save_show_data(chunk))


            LOGGER.debug(f"READING CHUNK {i} from {source} at {datetime.datetime.now()}")
            i +=1
            et_async_calls = time.time()
            async_time_counter[source] = et_async_calls - st_async_calls
            if target_field_count >= TARGET_COUNT_THRESHOLD:
                LOGGER.info(TARGET_REACHED_MESSAGE.format(source=source))
                et_async_calls = time.time()
                async_time_counter[source] = et_async_calls - st_async_calls
                return result_queue

async def call_parallel_consumers(run_timestamp):
    num_instances = 3
    result_queue = queue.Queue()
    event = Event()
    parameters_list = PLATFORM_LIST


    async with aiohttp.ClientSession() as session:

        tasks = [read_stream(session, instance_id, parameters_list[instance_id], result_queue, event) for instance_id in range(num_instances)]
        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=TIMEOUT_THRESHOLD)
        except asyncio.TimeoutError:
            LOGGER.info(TIMEOUT_MESSAGE)

    report_exec_time(async_time_counter, run_timestamp)

    merged_results = []
    while not result_queue.empty():
        result = result_queue.get()
        merged_results.append(result)

    LOGGER.info(f"Captured {len(merged_results)} objects")
    print(f"Captured {len(merged_results)} objects")

# if __name__ == "__main__":
#     run_timestamp = create_timestamp()
#     asyncio.run(call_parallel_consumers(run_timestamp))
