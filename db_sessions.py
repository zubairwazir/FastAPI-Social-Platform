from db_connection import Database
from config import config

main_db_instance = Database(
    database=config.postgres_main_db_name,
    host=config.postgres_server,
    user=config.postgres_user,
    password=config.postgres_password
)

log_db_instance = Database(
    database=config.postgres_log_db_name,
    host=config.postgres_server,
    user=config.postgres_user,
    password=config.postgres_password
)
