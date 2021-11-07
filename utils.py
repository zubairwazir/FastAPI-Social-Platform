import os
from config import config
import jwt
from fastapi.responses import JSONResponse
from config import config as app_config
from jwt import InvalidSignatureError, InvalidTokenError
from datetime import datetime
from db_sessions import main_db_instance
from typing import Callable, List, Union
from concurrent.futures import Executor


def random_lowercase(n):
    min_lc = ord(b'a')
    len_lc = 26
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc # convert 0..255 to 97..122
    
    return ba.decode('utf-8')


def jwt_token(email):
    return jwt.encode({"email": email}, config.jwt_key, algorithm="HS256")


def validate_token(token: str):
    try:
        data = jwt.decode(token, key=app_config.jwt_key, algorithms="HS256")
        return True, data

    except (InvalidSignatureError, InvalidTokenError, Exception):
        return False, None


async def increment_usage_counter(email: str):
    month = datetime.now().strftime("%b")
    year = int(datetime.now().strftime("%Y"))

    try:
        rows = await main_db_instance.fetch_rows("select count from company.api_usage where month=$1 and year=$2 and "
                                                 "email= $3", month, year, email)

        if rows and len(rows) != 0:
            await main_db_instance.execute(f"update company.api_usage set count = count + 1 where month='{month}' and year={year} and email= '{email}'")
        else:
            await main_db_instance.execute(f"insert into company.api_usage (email, month, year, count) values ('{email}', '{month}', {year}, 1)")

    except Exception:
        print("error - Failed to update counter")
        return JSONResponse({"success": False, "message": "Internal server error."}, status_code=500)


async def schedule_task(executor: Executor, fn: Callable, *args):
    import logging
    import asyncio

    logging.info(f'Scheduling {fn.__name__} background task with args {args}')
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, fn, *args)


def convert_tickers(tickers: Union[str, List[str]]):
    if not isinstance(tickers, list):
        tickers = [tickers, '']

    return tuple([x.upper() for x in tickers])
