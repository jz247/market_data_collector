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



