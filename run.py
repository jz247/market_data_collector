import os
import sys
import asyncio
import nest_asyncio
import json
from dotenv import load_dotenv

# from wss.wss_binance import binance_async
from wss.wss_12data import twelvedata_async
from db.db_pool import get_pool
from db.db_insert import insert2db

def subscribe_event(symbols):
    return {
        "action": "subscribe",
        "params": {
            "symbols": ",".join(list(symbols))
        }
    }


def main() -> None:
    ###
    pair = "bnbbtc"
    CURRENCY_PAIRS = ['AUD/USD', 'CAD/USD', 'CHF/USD', 'EUR/USD', 'GBP/USD', 'NZD/USD', 'USD/AUD', 
                    'USD/CAD', 'USD/CHF', 'USD/EUR', 'USD/GBP', 'USD/JPY', 'USD/NZD', 'AUD/CAD', 'AUD/CHF', 
                    'AUD/EUR', 'AUD/GBP', 'AUD/JPY', 'AUD/NZD', 'CAD/AUD', 'CAD/CHF', 
                    'CAD/COP', 'CAD/EUR', 'CAD/GBP', 'CAD/JPY', 'CAD/NZD', 'CHF/AUD', 'CHF/CAD', 'CHF/EUR', 
                    'CHF/GBP', 'CHF/JPY', 'CHF/NZD', 'EUR/AOA', 'EUR/AUD', 'EUR/CAD', 'EUR/CHF', 
                    'EUR/GBP', 'EUR/JPY', 'EUR/NZD', 'GBP/AUD', 'GBP/CAD', 'GBP/CHF', 
                    'GBP/EUR', 'GBP/JPY', 'GBP/NZD', 'JPY/CHF', 'NZD/AUD', 'NZD/CAD', 'NZD/CHF', 'NZD/EUR',
                    'NZD/GBP', 'NZD/JPY', 'USD/RUB', 'CZK/PLN'
                    ] # Use this one for hourly & minutes - removed 'BHD/INR', 'USD/AOA' due to small datasets. 
                        # Needs to remove 'CAD/COP', 'EUR/AOA', 'CZK/PLN' as well due to no where to download historical tick data

    symbols = CURRENCY_PAIRS # ['AUD/USD', 'CAD/USD']

    ###
    load_dotenv()
    # subscribe = json.dumps({
    # "method": "SUBSCRIBE",
    # "params": [
    #     f"{pair}@aggTrade",
    #     # f"{pair}@depth"
    # ],
    # 'id': 1
    # })
    subscribe = json.dumps(subscribe_event(symbols))
    print('subscribe:', subscribe)

    ###
    # socket = f"wss://stream.binance.com:9443/ws/{pair}@aggTrade"
    socket = f"wss://ws.twelvedata.com/v1/quotes/price?apikey=7b742df1fccc4f428fa53aef377c05ab"
    ###

    # PGPASSWORD = os.environ.get("PGPASSWORD")
    # PGHOST = os.environ.get("PGHOST")
    # print(PGPASSWORD, PGHOST)

    # dsn = f"postgres://postgres:st0plessA!@localhost:5432/market_data"
    dsn = f"postgres://postgres:st0plessA!@localhost:5432/tutorial"
    # table = f'binance_{pair}'
    table = f'ticks'

    while True:
        pool = get_pool(dsn, asyncio.get_event_loop())
        insert2db_async = lambda msg: insert2db(msg, 
                                                pool=pool, 
                                                table=table)
        
        asyncio.get_event_loop().run_until_complete(
            twelvedata_async(socket=socket, 
                            subscribe=subscribe,
                            insert2db=insert2db_async)
            )
        asyncio.get_event_loop().run_until_complete(pool.close())

if __name__ == "__main__":
    main()