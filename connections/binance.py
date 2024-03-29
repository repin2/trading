from calendar import month_name
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Tuple, Optional, List, Dict
import asyncio

from binance.client import AsyncClient

from configs import TESTNET, MAX_SPREAD_LIMIT, TRADING_TIME_FRAME, CURRENCY_LIMIT, ACTUAL_DAYS_NUM, API_KEY, API_SECRET_KEY, BLACK_LIST_TESTNET

api_key = API_KEY
api_secret = API_SECRET_KEY


kline_columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                 'trades_number', 'base_volume', 'quote_volume', 'ignore']
dtype = {'open': np.float32}


def _close_client_decorator(func):
    async def wrapper(*args, **kwargs):
        if 'client' not in kwargs and not any(isinstance(arg, AsyncClient) for arg in args):
            kwargs['client'] = AsyncClient(api_key, api_secret, tld='com', testnet=TESTNET)
            result = await func(*args, **kwargs)
            await kwargs['client'].close_connection()
            return result
        else:
            return await func(*args, **kwargs)
    return wrapper


@_close_client_decorator
async def set_leverage(leverage=1, client: Optional[AsyncClient] = None):
    # Todo: Run it when bot's starting
    # Todo: Optimize async client creation
    symbol_list = [
        x['symbol'] for x in (await client.futures_exchange_info())['symbols'] if 'USDT' in x['symbol']
               ]
    tasks = [client.futures_change_leverage(symbol=symbol, leverage=leverage) for symbol in symbol_list]
    await asyncio.gather(*tasks)


# Todo: May be put this functions to binance-utils?
@_close_client_decorator
async def _symbols_filter_spread(symbols: List[str], client: AsyncClient=None) -> List[str]:
    tasks = [client.futures_order_book(symbol=symbol, limit=5) for symbol in symbols]
    order_books = await asyncio.gather(*tasks)
    return [symbol for symbol, order_book in zip(symbols, order_books) if _is_spread_under_limit(order_book)]


def _is_spread_under_limit(order_dict: Dict):
    bids_list, asks_list = order_dict['bids'], order_dict['asks']
    if not bids_list or not asks_list:
        return False
    best_bid, best_ask = float(bids_list[0][0]), float(asks_list[0][0])
    current_spread = (best_ask - best_bid) / best_bid
    return current_spread <= MAX_SPREAD_LIMIT


@_close_client_decorator
async def _get_hist_klines(pair: str, time_frame, start, stop, client: Optional[AsyncClient]=None):
    result = []
    try:
        async for kline in await client.futures_historical_klines_generator(pair, time_frame,
                                                                        start, stop):
            result.append(kline)
    except asyncio.exceptions.TimeoutError:
        return None
    return result


@_close_client_decorator
async def _get_all_histories(pairs, time_frame, start, stop, client=None):
    history_tasks = [_get_hist_klines(pair, time_frame, start, stop, client=client) for pair in pairs]
    results = await asyncio.gather(*history_tasks)
    return results


def get_actual_time_frame(now: Optional[datetime] = None, actual_days_num: int = ACTUAL_DAYS_NUM) -> Tuple[str, str]:
    if now is None:
        now = datetime.now()
    # now += timedelta(days=1)
    now_day, now_month, now_year = now.day, month_name[now.month][:3], now.year
    start = now - timedelta(days=actual_days_num)
    start_day, start_month, start_year = start.day, month_name[start.month][:3], start.year
    return f"{start_day} {start_month}, {start_year}", f"{now_day} {now_month}, {now_year}"


@_close_client_decorator
async def get_current_pairs(time_frame=TRADING_TIME_FRAME, start: str = "18 Sep, 2022", stop: str = "26 Sep, 2022",
                      symbols: Optional[List[str]] = None, client: Optional[AsyncClient] = None):
    if symbols is None:
        symbols = [
            x['symbol'] for x in (await client.futures_exchange_info())['symbols'] if
            'USDT' in x['symbol']
            and x['contractType']
            and x['contractType'][0] == 'P'
            and x['symbol']
        ]
        if TESTNET:
            symbols = [x for x in symbols if x not in BLACK_LIST_TESTNET]

    symbols = await _symbols_filter_spread(symbols, client=client)
    symbols = symbols[:CURRENCY_LIMIT]  # remove this line if you want to load full data

    history_dict = {}
    histories = await _get_all_histories(symbols, time_frame, start, stop, client=client)
    for symbol, history in zip(symbols, histories):
        if not history:
            continue
        df = pd.DataFrame(np.array(history), columns=kline_columns)
        for col in ( 'open', 'high', 'close', 'low',  'quote_asset_volume','base_volume', 'quote_volume'):
            df[col] = df[col].astype(np.float32)
        df.drop(columns=['close_time'], inplace=True)
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

        df.set_index('open_time', drop=False, inplace=True)
        df['price'] = (df['open'] + df['close'] + df['high'] + df['low']) / 4
        history_dict[symbol] = df
    return history_dict


def get_current_window_from_df(df: pd.DataFrame, start: datetime, stop: datetime = None):
    if stop is None:
        stop = datetime.now()
    return df[(df.index >= start) & (df.index <= stop)]



