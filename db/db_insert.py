# import asyncpg
from collections import namedtuple
from asyncpg.pool import Pool
# from datetime import timedelta

async def insert2db(msg: namedtuple, *, pool: Pool, schema: str=None, table: str) -> None:
    fields = msg._fields
    placeholders = [f'${i}' for i, _ in enumerate(fields, 1)]
    query_insert = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
    # print(datetime.datetime.utcnow(), ': Insert statement:', query_insert)
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(query_insert, *msg)
