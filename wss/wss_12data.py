import json
import websockets
from datetime import datetime, timedelta, timezone
from collections import namedtuple
from db.db_pool import logger
import asyncpg
from retrying import retry

twelvedata_namedtuple = namedtuple(
    "twelvedata_msg",
    ['datetime', 'symbol', 'price']
)


def create_twelvedata_msg(*, response: dict) -> namedtuple:
    return twelvedata_namedtuple(
        datetime=datetime.fromtimestamp(response.get('timestamp'), timezone.utc),
        symbol=response.get('symbol'),
        price=response.get('price')
    )

def retry_on_errors(exc):
    print(datetime.datetime.utcnow(), ': Exception type is', type(exc))
    if 'IncompleteReadError' in str(type(exc)):
        return True
    elif 'ConnectionClosedError' in str(type(exc)):
        return True
    elif 'UniqueViolationError' in str(type(exc)):
        return True
    else:
        return False

@retry(wait_fixed=5, retry_on_exception=retry_on_errors, stop_max_attempt_number=50)
async def insert2db_with_retry(insert2db, msg):
    try:
        await insert2db(msg=msg)

    except asyncpg.exceptions.UniqueViolationError as e:
        print(datetime.datetime.utcnow(), ': Catch error:', e)
        print(datetime.datetime.utcnow(), ': Original msg', msg)
        msg = msg._replace(datetime = msg.datetime + timedelta(microseconds=1))
        print(datetime.datetime.utcnow(), ': Updated msg', msg)
        await insert2db(msg=msg)
        pass

@retry(wait_fixed=5, retry_on_exception=retry_on_errors, stop_max_attempt_number=50)
async def twelvedata_async(*, socket, subscribe, insert2db):
    async with websockets.connect(socket) as conn:
        await conn.send(subscribe)
        while True:
            try:
                message = json.loads(await conn.recv())

            except websockets.exceptions.ConnectionClosedError as e:
                print(datetime.datetime.utcnow(), ': Reconnecting due to catching error:', e)
                conn = websockets.connect(socket)
                message = json.loads(await conn.recv())
                pass

            finally:
                # print(datetime.datetime.utcnow(), ': Received message:', message)
                if 'status' not in message:
                    msg = create_twelvedata_msg(response=message)
                    # print(datetime.datetime.utcnow(), ': Received values:', msg)
                    # await insert2db(msg=msg)
                    await insert2db_with_retry(insert2db, msg=msg)
                else:
                    logger.warning('Subscribe-status message')
