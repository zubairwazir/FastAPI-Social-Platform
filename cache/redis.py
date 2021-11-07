import aioredis
import msgpack
import asyncio
from typing import List


class RedisRequestResponseCache:
    def __init__(self, url: str):
        self._redis = aioredis.from_url(url,
                                        max_connections=200,
                                        socket_timeout=2,
                                        socket_connect_timeout=2,
                                        retry_on_timeout=False
                                        )

    # checks if the key is in cache
    async def contains(self, key):
        return await self._redis.exists(key)

    # get the item from cache
    async def get(self, item):
        try:
            cached_response = await asyncio.wait_for(self._redis.get(item), timeout=2.0)
        except asyncio.TimeoutError:
            return False, None

        if cached_response is None:
            return False, None

        return True, msgpack.unpackb(cached_response, raw=False)

    # set the item in cache
    async def set(self, key, value, **kwargs):
        expire_after = kwargs.get('expire_after', 24 * 60 * 60)  # default of 1 day expiry
        serialized_value = msgpack.packb(value, use_bin_type=True)

        try:
            await asyncio.wait_for(self._redis.setex(key, expire_after, serialized_value), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    async def close(self):
        await self._redis.close()

    # bulk delete keys
    async def delete(self, pattern: str) -> List[str]:
        keys = await self._redis.keys(pattern)

        if len(keys) > 0:
            await self._redis.delete(*keys)

        return keys
