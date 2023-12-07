from dotenv import load_dotenv
import os


class Config:
    def __init__(self):
        load_dotenv()
        self.stream_endpoint = os.getenv('STREAM_ENDPOINT')
        self.stream_user = os.getenv('STREAM_USER')
        self.stream_password = os.getenv('STREAM_PASSWORD')

def get_config():
    config_instance = Config()
    return config_instance