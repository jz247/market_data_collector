import asyncpg
from collections import namedtuple
from asyncpg.pool import Pool
from datetime import timedelta

async def insert2db(msg: namedtuple, *, pool: Pool, schema: str=None, table: str) -> None:
    fields = msg._fields
    placeholders = [f'${i}' for i, _ in enumerate(fields, 1)]
#     query_create = f"CREATE TABLE IF NOT EXISTS {table}\
# (ts timestamp, base text, quote text, event_type text, price numeric, quantity numeric, market_maker boolean)"
    query_insert = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
    print('Insert statement:', query_insert)
    async with pool.acquire() as connection:
        async with connection.transaction():
            # await connection.execute(query_create)
            try:
                await connection.execute(query_insert, *msg)
            except asyncpg.exceptions.UniqueViolationError as e:
                print("Catch error:", e)
                print('original msg', msg)
                msg = msg._replace(datetime = msg.datetime + timedelta(microseconds=1))
                print('updated msg', msg)
                await connection.execute(query_insert, *msg)
                # raise Exception('Catch duplicated events.')
                # pass
