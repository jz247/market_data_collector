
import os
import sys
import asyncio
import nest_asyncio
import json
from dotenv import load_dotenv

from wss.wss_binance import binance_async
from db.db_pool import get_pool
from db.db_insert import insert2db


def main() -> None:
    ###
    pair = "bnbbtc"
    ###
    load_dotenv()
    subscribe = json.dumps({
    "method": "SUBSCRIBE",
    "params": [
        f"{pair}@aggTrade",
        # f"{pair}@depth"
    ],
    'id': 1
    })
    ###
    socket = f"wss://stream.binance.com:9443/ws/{pair}@aggTrade"
    ###

    PGPASSWORD = os.environ.get("PGPASSWORD")
    PGHOST = os.environ.get("PGHOST")

    dsn = f"postgres://postgres:{PGPASSWORD}@{PGHOST}/binancedb"
    table = f'binance_{pair}'

    

if __name__ == "__main__":
    main()