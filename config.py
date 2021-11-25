import os
from os.path import dirname, join
from pydantic import BaseSettings
from dotenv import load_dotenv

path = os.environ.get('DOTENV_PATH', '.env')
load_dotenv(dotenv_path=join(dirname(__file__), path))


class Config(BaseSettings):
    port: int
    postgres_server: str
    postgres_main_db_name: str
    postgres_user: str
    postgres_password: str
    postgres_log_db_name: str
    redis_server: str
    firebase_api_key: str
    # jwt_key: str
    rock_url: str
    rock_api_key: str


config = Config()
