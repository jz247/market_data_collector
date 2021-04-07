import json
import websockets
from datetime import datetime, timedelta
from collections import namedtuple
from db.db_pool import logger
import asyncpg

twelvedata_namedtuple = namedtuple(
    "twelvedata_msg",
    ['datetime', 'symbol', 'price']
)


def create_twelvedata_msg(*, response: dict) -> namedtuple:
    return twelvedata_namedtuple(
        datetime=datetime.utcfromtimestamp(response.get('timestamp')),
        symbol=response.get('symbol'),
        price=response.get('price')
    )


async def twelvedata_async(*, socket, subscribe, insert2db):
    async with websockets.connect(socket) as websocket:
        await websocket.send(subscribe)
        while True:
            message = json.loads(await websocket.recv())
            print('Received message:', message)
            if 'status' not in message:
                msg = create_twelvedata_msg(response=message)
                print('Received values:', msg)
                # await insert2db(msg=msg)
                try:
                    await insert2db(msg=msg)
                except asyncpg.exceptions.UniqueViolationError as e:
                    print("Catch error:", e)
                    print('original msg', msg)
                    msg = msg._replace(datetime = msg.datetime + timedelta(microseconds=1))
                    print('updated msg', msg)
                    await insert2db(msg=msg)
                    pass
            else:
                logger.warning('Subscribe-status message')
