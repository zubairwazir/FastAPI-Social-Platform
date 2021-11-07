import jwt
import logging
from datetime import datetime
from fastapi import Header, HTTPException
from fastapi.responses import JSONResponse
from config import config as app_config
from db_sessions import main_db_instance


logger = logging.getLogger('DEPENDENCY_ROUTES')


async def api_counter(authorization: str = Header(None)):
    try:
        data = jwt.decode(
            authorization, key=app_config.jwt_key, algorithms="HS256")
    except Exception:
        raise HTTPException(status_code=401, detail=str("Invalid token"))

    email = data["email"]

    month = datetime.now().strftime("%b")
    year = int(datetime.now().strftime("%Y"))

    try:
        rows = await main_db_instance.fetch_rows("select count from company.api_usage where month=$1 and year=$2 and email= $3", month, year, email)

        if rows and len(rows) != 0:
            await main_db_instance.execute(f"update company.api_usage set count = count + 1 where month='{month}' and year={year} and email= '{email}'")
        else:
            await main_db_instance.execute(f"insert into company.api_usage (email, month, year, count) values ('{email}', '{month}', {year}, 1)")
    except Exception as exp:
        logger.error(f"Failed to update the api usage counter: {exp}")
        return JSONResponse({"success": False, "message": "Internal server error."}, status_code=500)

