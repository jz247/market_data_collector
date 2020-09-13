import json
import websockets
from datetime import datetime
from collections import namedtuple
from db.db_pool import logger

binance_namedtuple = namedtuple(
    "binance_msg", 
    ['ts', 'base', 'quote', 'event_type', 'price', 'quantity', 'market_maker']
)

def create_binance_msg(*, response: dict) -> namedtuple:
    return binance_namedtuple(
                ts=datetime.utcfromtimestamp(response.get('T')/1000),
                base=response.get('s')[:3], 
                quote=response.get('s')[3:],
                event_type=response.get('e'),
                price=response.get('p'),
                quantity=response.get('q'),
                market_maker=response.get('m')
                )




