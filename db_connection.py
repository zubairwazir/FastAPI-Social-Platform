import asyncpg


class Database:
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 5432):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self._cursor = None

        self._connection_pool = None
        self.con = None

    async def connect(self):
        if not self._connection_pool:
            try:
                self._connection_pool = await asyncpg.create_pool(
                    min_size=1,
                    max_size=30,
                    command_timeout=300,
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                )

            except Exception as e:
                print(e)
    
    async def disconnect(self):
        if self._connection_pool:
            try:
                await self._connection_pool.close()
                self._connection_pool = None
                self.con = None
            except Exception as e:
                print(e)

    async def fetch_rows(self, query: str, *args):
        if not self._connection_pool:
            await self.connect()
        else:
            con = await self._connection_pool.acquire()
            try:
                result = await con.fetch(query, *args)
                return result
            except Exception as e:
                print(e)
            finally:
                await self._connection_pool.release(con)

    async def execute(self, query: str):
        if not self._connection_pool:
            await self.connect()
        else:
            con = await self._connection_pool.acquire()
            try:
                result = await con.execute(query)
                return result
            except Exception as e:
                print(e)
            finally:
                await self._connection_pool.release(con)
