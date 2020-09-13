import time
import asyncio
import asyncpg
from asyncpg.pool import Pool
import logging

logger = logging.getLogger(__name__)

async def create_pool(*, dsn: str, min_conn: int = 2, max_conn: int = 10, **kwargs) -> Pool:
    pool = await asyncpg.create_pool(dsn=dsn,
                                     min_size=min_conn,
                                     max_size=max_conn,
                                     **kwargs)
    logger.info("Pool created")
    return pool


def get_pool(dsn: str, loop: asyncio.AbstractEventLoop) -> Pool:
    attempts = 10
    sleep_between_attempts = 3
    for _ in range(attempts):
        try:
            pool = loop.run_until_complete(create_pool(dsn=dsn))
        except Exception as e:
            logger.exception(e)
            time.sleep(sleep_between_attempts)
        else:
            return pool
    raise Exception(f"Connected to the database using {dsn} "
                    f"after {attempts * sleep_between_attempts} seconds")
