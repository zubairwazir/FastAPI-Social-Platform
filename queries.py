from config import hasura_api_url, database_url, hasura_token
import aiohttp
from fastapi import Depends
import databases
import sqlalchemy

async def get_result_by_query(query: str, http_client):
    body_data: dict = {
        'type': 'run_sql',
        'args': {
            'sql': query
        }
    }

    headers: dict = {
        'x-hasura-admin-secret': hasura_token,
        'X-Hasura-Role': 'admin',
        'content-type': 'application/json'
    }

    url = '{api_url}/v1/query'.format(api_url=hasura_api_url)
    response = await http_client.post(url=url, json=body_data, headers=headers)
    return await response.json()
