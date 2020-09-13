import json
import websockets
from datetime import datetime
from collections import namedtuple
from db.db_pool import logger

binance_namedtuple = namedtuple(
    "binance_msg", 
    ['ts', 'base', 'quote', 'event_type', 'price', 'quantity', 'market_maker']
)


