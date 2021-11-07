from db_sessions import main_db_instance
from fastapi.encoders import jsonable_encoder
import asyncio
from batch_jobs import proddatadownload_v2 as downloadMarketData

async def hello_world_task():
    await main_db_instance.connect()

    # query_statement = 'select * from "company"."General"'

    # result_data = await main_db_instance.execute(query_statement)
    # result_data_json = jsonable_encoder(result_data)

    # print(result_data_json)

    downloadMarketData()

    await main_db_instance.disconnect()

loop = asyncio.get_event_loop()
loop.run_until_complete(hello_world_task())
loop.close()

