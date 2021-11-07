import asyncio
import logging
import hashlib
from aioredis import RedisError
from fastapi.routing import APIRoute
from cache.redis import RedisRequestResponseCache
from fastapi import Response, Request, HTTPException
from utils import validate_token, increment_usage_counter

from config import config
from typing import Callable, Coroutine, Any

# TODO: read the values from config
_pass_through_list = {
    '/updateinfo',
    '/cache/delete',
    '/getuserinfo',
    '/updateinfo',
    '/newToken',
    '/validate_token',
    '/login',
    '/verify-email'
}
_exclusion_list = {
    '/exclude'
    # '/updateinfo',
    # '/redis/delete',
    # '/getuserinfo',
    # '/updateinfo',
    # '/newToken',
    # '/validate_token',
    # '/login',
    # '/verify-email'
}
_expiry_config = {
}  # the time here is in seconds
_redis = RedisRequestResponseCache(
    url=config.redis_server,
)

logging.basicConfig(level=logging.INFO)


class CachingLayerRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_route_handler = super().get_route_handler()

        async def cache_route_handler(request: Request) -> Response:
            # if the entry is in pass through list just simply forward the request
            # without even validating the token
            if request.url.path in _pass_through_list:
                return await original_route_handler(request)

            forwarded = False
            is_valid, data = validate_token(request.headers.get('Authorization', ''))

            if not is_valid:
                raise HTTPException(status_code=401, detail="Invalid token")

            try:
                key = request.url.path
                if key in _exclusion_list:
                    return await original_route_handler(request)

                if request.method == 'POST':
                    body = await request.body()
                    body_hash = hashlib.md5(body).hexdigest()
                    key += f'_{body_hash}'

                exists, content = await _redis.get(key)

                # cache miss, forward the request to key operation function
                if not exists:
                    forwarded = True
                    logging.info(f'Cache miss: forwarding the request to path operation function - {request.url.path}')
                    response: Response = await original_route_handler(request)

                    # in seconds, one day = 24 * 60 * 60
                    expire_after = _expiry_config.get(
                        request.url.path, 1 * 60 * 60)
                    logging.info(
                        f"Caching the response with an expiry time of {expire_after} seconds")
                    await _redis.set(key, response.body, expire_after=expire_after)

                    content = response.body
                else:
                    logging.info("Cache hit: returning the response")

                return Response(content=content, media_type="application/json")

            # catch any kind of exception in the caching layer
            except (RedisError, Exception) as exp:
                logging.error(f'Exception in the caching layer: {exp}')

                if not forwarded:  # only forward the request if the exception did not arise from the controller
                    logging.info(f'Forwarding the request to key operation function: {request.url.path}')
                    return await original_route_handler(request)

            finally:
                # increment the counter in the background task
                asyncio.get_event_loop().create_task(
                    increment_usage_counter(data['email']))

        return cache_route_handler
