PLATFORM_LIST = ["sytflix", "sytazon", "sysney"]
TARGET_NAME = "Sytac"
TARGET_COUNT_THRESHOLD = 3
TIMEOUT_THRESHOLD = 20
COUNTRY_TZ_DICT = {
    "PT": "UTC",
    "CA": "America/Toronto",
    "US": "America/Los_Angeles",
    "RU": "Europe/Moscow",
    "ID": "Asia/Jakarta",
    "CN": "Asia/Shanghai",
    "BR": "America/Sao_Paulo",
    "AR": "America/Argentina/Buenos_Aires",
}

RECENT_YEAR = 2020

DATETIME_FORMAT = '%d-%m-%Y %H:%M:%S.%f'

BIRTHDATE_FORMAT = "%d/%m/%Y"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

RESPONSE_TIME_FORMAT = "%d-%m-%Y %H:%M:%S.%f"

ERROR_MESSAGE_404 = """Data not found! Please run data consumer than access API!"""
TIMEOUT_MESSAGE = """Timeout: Execution exceeded 20 seconds."""
TARGET_REACHED_MESSAGE= """Instance for {source}: Stopping due to reaching the target count."""
ACHIEVED_TARGET_COUNT_MESSAGE = """Found {target_name} {target_count} times, exiting!"""
PARSE_ERROR_MESSAGE= """Cannot parse data object"""
EXIT_MESSAGE = """Failed to access data stream. Exiting"""

EXEC_REPORT_TITLE = """EXECUTION TIME FOR RUN {timestamp} \n"""
EXEC_REPORT_LINE = """{source} was listened for {second} seconds. \n"""


PLATFORM_SYTFLIX = "Sytflix"