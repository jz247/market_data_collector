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

    # PGPASSWORD = os.environ.get("PGPASSWORD")
    # PGHOST = os.environ.get("PGHOST")
    # print(PGPASSWORD, PGHOST)

    dsn = f"postgres://postgres:st0plessA!@localhost:5432/binancedb"
    table = f'binance_{pair}'

    while True:
        pool = get_pool(dsn, asyncio.get_event_loop())
        insert2db_async = lambda msg: insert2db(msg, 
                                                pool=pool, 
                                                table=table)
        
        asyncio.get_event_loop().run_until_complete(
            binance_async(socket=socket, 
                          subscribe=subscribe,
                          insert2db=insert2db_async)
            )
        asyncio.get_event_loop().run_until_complete(pool.close())

if __name__ == "__main__":
    main()