import asyncio

import click
from aiohttp.client_exceptions import ClientConnectorError

from app import start_api_from_ep
#from api_module.api import start_api_from_call
from consumer.async_call import call_parallel_consumers
from logger.logger import get_module_logger
from preprocessing.preprocess_show_records import generate_denormalized_data, archive_raw_data
from utils.utils import create_timestamp, create_data_folder, display_countdown
from variables.variables import EXIT_MESSAGE

LOGGER = get_module_logger(__name__)

#cli = click.Group()
@click.group()
def cli():
    pass

@click.group()
def main():
    pass

@main.command()
def start_consumer():
    create_data_folder()
    run_timestamp = create_timestamp()
    max_retries = 3
    retry_delay = 5
    successfully_ran = False
    for retry_count in range(max_retries):
        try:
            asyncio.run(call_parallel_consumers(run_timestamp))

            #Preprocess data
            preproc_res = generate_denormalized_data(run_timestamp)
            print("preprocessed the data")
            if not preproc_res:
                LOGGER.error("Could not preprocess data")
                return
            archive_raw_data()
            successfully_ran = True
        except ClientConnectorError as e:
            LOGGER.error(f"Error: {e}")
            LOGGER.error(f"Retry attempt {retry_count + 1}/{max_retries}")
            display_countdown(retry_delay)
        else:
            return

    if not successfully_ran:
        LOGGER.error(EXIT_MESSAGE)
@main.command()
def start_api():
    start_api_from_ep()#start_api_from_call()


cli.add_command(main)

if __name__ == '__main__':
    cli()

