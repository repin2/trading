from datetime import datetime

import pandas as pd
from binance.client import Client, AsyncClient
from binance.enums import *
from algorithms.coint_algs import get_coint_pairs_with_params
from algorithms.volatility import filter_by_volatilities
from typing import Dict
import asyncio
import aiohttp

from connections.binance import get_actual_time_frame, get_current_pairs, api_secret, api_key, TESTNET

from configs import MIN_PRICE_MULTUPLY_LIMIT, MAX_PRICE_MULTUPLY_LIMIT, TRADING_TIME_FRAME, MIN_ORDER_AMOUNT, \
    WALLET_START_PRICE


actual_pairs = {}
want_to_buy_pairs = {}
want_to_sell_pairs = {}


def _get_futures_info_dict() -> Dict:
    client = Client(api_key, api_secret, tld="com", testnet=TESTNET)
    futures_info_dict = client.futures_exchange_info()
    client.close_connection()
    return futures_info_dict


def _get_qty_limits() -> Dict:
    result = {}
    for symbol_info in futures_info_dict['symbols']:
        symbol = symbol_info['symbol']
        filter_dict = symbol_info['filters'][2]
        result[symbol] = {
            'minQty': float(filter_dict['minQty']),
            'maxQty': float(filter_dict['maxQty']),
            'precision': symbol_info['quantityPrecision']
        }
    return result


def _get_min_max_price() -> Dict[str, float]:
    result = {}
    for symbol_info in futures_info_dict['symbols']:
        symbol = symbol_info['symbol']
        filter_dict = symbol_info['filters'][0]
        result[symbol] = {
            'minPrice': float(filter_dict['minPrice']) * MIN_PRICE_MULTUPLY_LIMIT,
            'maxPrice': float(filter_dict['maxPrice']) / MAX_PRICE_MULTUPLY_LIMIT
        }
    return result


futures_info_dict = _get_futures_info_dict()
quantity_limits = _get_qty_limits()
min_max_prices = _get_min_max_price()


async def get_pairs_for_buy(time_frame=TRADING_TIME_FRAME):
    now = datetime.now()
    start, finish = get_actual_time_frame(now, actual_days_num=1)
    current_history_dict = await get_current_pairs(time_frame=time_frame, start=start, stop=finish)

    current_history_dict = filter_by_volatilities(current_history_dict)

    current_history_dict = _filter_by_price(current_history_dict)

    pairs, params, pairs_coint_order = get_coint_pairs_with_params(current_history_dict)
    cointegrated_pairs = {
        key: {
                'test_val': pairs[key][0],
                'p_val': pairs[key][1],
                'synt_middle_price': params[key][0],
                'b': params[key][1],
                'sygma': params[key][2]
            } for key in pairs_coint_order if pairs[key][0] < pairs[key][2][0] and params[key][2] > 0
                                              and params[key][1] > 0
    }
    client = AsyncClient(api_key, api_secret, tld="com", testnet=TESTNET)
    symbols = set()
    for symbol_1, symbol_2 in cointegrated_pairs:
        symbols.add(symbol_1)
        symbols.add(symbol_2)
    symbol_prices = await _get_prices(symbols)

    orders_list = []
    for pair in cointegrated_pairs:
        order_long, order_short = await buy_pair(pair[0], pair[1], symbol_prices, order_amount=MIN_ORDER_AMOUNT)
        if order_long is not None:
            orders_list.append((order_long, order_short))
    await client.close_connection()
    return orders_list


def _filter_by_price(current_history_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    result_dict = {}
    for symbol, histrory_df in current_history_dict.items():
        min_price, max_price = min_max_prices[symbol]['minPrice'], min_max_prices[symbol]['maxPrice']
        if histrory_df['price'].min() > min_price and histrory_df['price'].max() < max_price:
            result_dict[symbol] = histrory_df
    return result_dict


def _calculate_quantities(symbol_1, symbol_2, symbol_prices, order_amount=MIN_ORDER_AMOUNT):
    precision_1, precision_2 = quantity_limits[symbol_1]['precision'], quantity_limits[symbol_2]['precision']
    quote_1 = round(order_amount / symbol_prices[symbol_1], precision_1) if precision_1 else\
        round(order_amount / symbol_prices[symbol_1])
    quote_2 = round(order_amount / symbol_prices[symbol_2], precision_2) if precision_2 else \
        round(order_amount / symbol_prices[symbol_2])
    if not quote_1 or not quote_2:
        return None, None
    if quantity_limits[symbol_1]['minQty'] >  quote_1 or quote_1 >  quantity_limits[symbol_1]['maxQty']:
        return None, None
    if quantity_limits[symbol_2]['minQty'] >  quote_2 or quote_2 >  quantity_limits[symbol_2]['maxQty']:
        return None, None
    return quote_1, quote_2


async def _get_price(session, symbol):
    url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
    async with session.get(url) as response:
        response = await response.json()
        return response['symbol'], float(response['lastPrice'])


async def _get_prices(symbols):
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[_get_price(session, symbol) for symbol in symbols])
        return {key: value for key, value in results}


def update_current_pairs_info(cointegrated_pairs):
    for pair in list(actual_pairs):
        if pair not in cointegrated_pairs:
            actual_pairs.pop(pair)
    for pair, params in cointegrated_pairs.items():
        actual_pairs[pair] = params


def check_want_to_buy_pairs():
    for pair in list(want_to_buy_pairs):
        if pair not in actual_pairs:
            want_to_buy_pairs.pop(pair)


async def buy_symbol_market(symbol, quote_order_qty, is_long=True, client=None):
    if client is None:
        client = AsyncClient(api_key, api_secret, tld="com", testnet=TESTNET)
    if is_long:
        return await client.futures_create_order(
            symbol=symbol,
            side=client.SIDE_BUY,
            type=client.ORDER_TYPE_MARKET,
            quoteOrderQty=quote_order_qty
        )
    else:
        return await client.futures_create_order(
            symbol=symbol,
            side=client.SIDE_SELL,
            type=client.ORDER_TYPE_MARKET,
            positionSide="SHORT",
            quoteOrderQty=quote_order_qty
        )


async def close_pair(symbol_1, symbol_2, quote_1, quote_2):
    client = AsyncClient(api_key, api_secret, tld='com', testnet=TESTNET)
    order_long_close = client.futures_create_order(
        symbol=symbol_1,
        side=SIDE_SELL,
        type=FUTURE_ORDER_TYPE_MARKET,
        positionSide="LONG",
        quantity=quote_1
    )
    order_short_close = client.futures_create_order(
        symbol=symbol_2,
        side=SIDE_BUY,
        type=ORDER_TYPE_MARKET,
        positionSide="SHORT",
        quantity=quote_2
    )

    order_long, order_short = await asyncio.gather(order_long_close, order_short_close)
    return order_long, order_short


async def buy_pair(symbol_1, symbol_2, symbol_prices, order_amount):
    quote_1, quote_2 = _calculate_quantities(symbol_1, symbol_2, symbol_prices, order_amount)
    if None in (quote_1, quote_2):
        return None, None
    client = AsyncClient(api_key, api_secret, tld='com', testnet=TESTNET)
    order_long = client.futures_create_order(
        symbol=symbol_1,
        side=SIDE_BUY,
        type=FUTURE_ORDER_TYPE_MARKET,
        positionSide="LONG",
        quantity=quote_1
    )
    order_short = client.futures_create_order(
            symbol=symbol_2,
            side=SIDE_SELL,
            type=FUTURE_ORDER_TYPE_MARKET,
            positionSide="SHORT",
            quantity=quote_2
    )

    order_long, order_short = await asyncio.gather(order_long, order_short)

    return order_long, order_short
