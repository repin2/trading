from datetime import datetime

import pandas as pd
from binance.client import Client, AsyncClient
from binance.enums import *
from algorithms.coint_algs import get_coint_pairs_with_params
from algorithms.volatility import filter_by_volatilities
from typing import Dict, Tuple, Optional
from collections import Counter
import asyncio
import aiohttp

from connections.binance import get_actual_time_frame, get_current_pairs, api_secret, api_key, TESTNET, _close_client_decorator

from configs import MIN_PRICE_MULTUPLY_LIMIT, MAX_PRICE_MULTUPLY_LIMIT, TRADING_TIME_FRAME, MIN_ORDER_AMOUNT, \
    WALLET_START_PRICE, CHECK_COINTEGRATION_TIME_FRAME, TIMEOUT, SYGMA_MULTIPLIER


wallet_dict = {"current_capital": WALLET_START_PRICE}

# Todo: Create classes for pairs
# pairs which are cointegrated now
actual_pairs = {}

# cointegrated pairs for purchase
want_to_buy_pairs = {} # value: {'synt_middle_price': .., 'b': .., 'sygma': ..}
# pairs which are bought in short/long, try to sell and take profit

# Todo: Create class for pairs
want_to_sell_pairs = {} # value: {'quantity_long': .., 'quantity_short': .., 'bought_time': .., 'synt_middle_price': .., 'b': .., 'sygma': ..}

# if we have already bought some pair and want to buy it now, we put it here
sell_immediately = {} # value: {'quantity_long': .., 'quantity_short': ..}


def _get_futures_info_dict() -> Dict:
    client = Client(api_key, api_secret, tld="com", testnet=TESTNET)
    futures_info_dict = client.futures_exchange_info()
    client.close_connection()
    return futures_info_dict


def populate_immediately_sell_pairs():
    # finding expired pairs
    for pair in list(want_to_sell_pairs):
        if want_to_sell_pairs[pair]['bought_time'] + TIMEOUT >= datetime.now():
            sell_immediately[pair] = want_to_sell_pairs[pair]
            want_to_sell_pairs.pop(pair)

    # finding pairs with good prices
    # Todo: May be need optimization
    if want_to_sell_pairs:
        symbols = set()
        for pair in want_to_sell_pairs:
            symbols.add(pair[0])
            symbols.add(pair[1])
        symbol_price_dict = asyncio.run(_get_prices(symbols))
        for pair in list(want_to_sell_pairs):
            b, synt_middle_price = want_to_sell_pairs[pair]['b'], want_to_buy_pairs[pair]['synt_middle_price']
            symb_1, symb_2 = pair
            if symbol_price_dict[symb_1] + b * symbol_price_dict[symb_2] >= synt_middle_price:
                sell_immediately[pair] = want_to_sell_pairs[pair]
                want_to_sell_pairs.pop(pair)


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


async def update_new_cointegrations(time_frame=TRADING_TIME_FRAME):
    cointegrated_pairs = await _get_current_cointegrated_pairs(time_frame)
    update_current_pairs_info(cointegrated_pairs)


def update_current_pairs_info(cointegrated_pairs):
    """
    When we get new cointegrated pairs with new params ( synth price, b, etc..)
    we want to update:
        1 actual pairs. This is storage for last result of cointegration test
        2 want to buy pairs. Only pairs from actual pairs could be their.
            If we have already bought this pair we don't to buy it again. It means checking in want to sell pairs
            immediately sell
        3 want to sell pairs. If we see, that pair which we have already bought is not cointegrated now we need to sell it immediately
        4 immediately sell dict

    """
    for pair in list(actual_pairs):
        if pair not in cointegrated_pairs:
            actual_pairs.pop(pair)

    for pair, params in cointegrated_pairs.items():
        actual_pairs[pair] = params

    for pair in list(want_to_buy_pairs):
        if pair not in cointegrated_pairs:
            want_to_buy_pairs.pop(pair)

    for pair, params in cointegrated_pairs.items():
        if pair not in want_to_sell_pairs and pair not in sell_immediately:
            want_to_buy_pairs[pair] = params


    for pair in list(want_to_sell_pairs):
        if pair not in cointegrated_pairs:
            if pair in  sell_immediately:
                sell_immediately[pair] = _merge_params(sell_immediately[pair], want_to_sell_pairs[pair])
            else:
                sell_immediately[pair] = want_to_sell_pairs[pair]
            want_to_sell_pairs.pop(pair)


def _merge_params(value_1: Dict, value_2: Dict):
    return {
        'quantity_long': value_1['quantity_long'] + value_2['quantity_long'],
        'quantity_short': value_1['quantity_short'] + value_2['quantity_short']
    }


async def _get_current_cointegrated_pairs(time_frame=TRADING_TIME_FRAME):
    # to run every CHECK_COINTEGRATION_TIME_FRAME hours
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
            } for key in pairs_coint_order if pairs[key][0] < pairs[key][2][0] and pairs[key][1] < 0.02
                                              and params[key][2] > 0
                                              and params[key][1] > 0
    }
    cointegrated_pairs = _filter_multiple_symbols(cointegrated_pairs)
    return cointegrated_pairs
    # client = AsyncClient(api_key, api_secret, tld="com", testnet=TESTNET)
    # symbols = set()
    # for symbol_1, symbol_2 in cointegrated_pairs:
    #     symbols.add(symbol_1)
    #     symbols.add(symbol_2)
    # symbol_prices = await _get_prices(symbols)
    #
    # orders_list = []
    # for pair in cointegrated_pairs:
    #     order_long, order_short = await buy_pair(pair[0], pair[1], symbol_prices, order_amount=MIN_ORDER_AMOUNT)
    #     if order_long is not None:
    #         orders_list.append((order_long, order_short))
    # await client.close_connection()
    # return orders_list


def _filter_multiple_symbols(cointegrated_pairs: Dict[Tuple[str, str], Dict]) -> Dict[Tuple[str, str], Dict]:
    if len(cointegrated_pairs) < 3:
        return cointegrated_pairs
    symbol_counter_left = Counter()
    extra_symbols = set()
    for pair in cointegrated_pairs:
        symbol_counter_left[pair[0]] += 1
        if symbol_counter_left[pair[0]] > len(cointegrated_pairs) ** 0.5:
            extra_symbols.add(pair[0])
    for pair in list(cointegrated_pairs):
        if pair[0] in extra_symbols:
            cointegrated_pairs.pop(pair)
    return cointegrated_pairs


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


@_close_client_decorator
async def close_pair(symbol_1, symbol_2, quote_1, quote_2, client: Optional[AsyncClient] = None):
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
        type=FUTURE_ORDER_TYPE_MARKET,
        positionSide="SHORT",
        quantity=quote_2
    )

    order_long, order_short = await asyncio.gather(order_long_close, order_short_close)
    return order_long, order_short


@_close_client_decorator
async def buy_pair(symbol_1, symbol_2, symbol_prices, order_amount, client: Optional[AsyncClient] = None):
    quote_1, quote_2 = _calculate_quantities(symbol_1, symbol_2, symbol_prices, order_amount)
    if None in (quote_1, quote_2):
        return None, None

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


def _get_total_price_and_comission_order(result_sell: Dict):
    total_price = sum(float(x['price']) * float(x['qty']) for x in result_sell['fill'])
    total_comission = sum(float(x['comission']) for x in result_sell['fill'])
    return total_price, total_comission

def _get_total_quantity_order(result_sell: Dict):
    total_price = sum(float(x['price']) * float(x['qty']) for x in result_sell['fill'])
    total_comission = sum(float(x['comission']) for x in result_sell['fill'])
    return total_price, total_comission


def _get_stats_per_deal(result_sell: Dict, sell_dict: Dict):
    # ToDo: finish stats function
    symbol = result_sell['symbol']
    total_price = sum(float(x['price']) * float(x['qty']) - x['comission'] for x in result_sell['fill'])
    return total_price


@_close_client_decorator
async def sell_pairs_from_sell_immediately_if_have(client: Optional[AsyncClient] = None):
    if sell_immediately:
        tasks = []
        for pair, params in sell_immediately.items():
            task = close_pair(pair[0], pair[1], params['quantity_long'], params['quantity_short'], client)
            tasks.append(task)

        results_list = await asyncio.gather(*tasks)
        for order_long, order_short in results_list:
            long_price, long_comission = _get_total_price_and_comission_order(order_long)
            short_price, short_comission = _get_total_price_and_comission_order(order_short)
            wallet_dict['current_capital'] += long_price
            wallet_dict["current_capital"] -= short_price
            wallet_dict['current_capital'] -= long_comission + short_comission
            pair = (order_long['symbol'], order_short['symbol'])
            sell_immediately.pop(pair)
            if pair in actual_pairs:
                want_to_buy_pairs[pair] = actual_pairs[pair]

        print ("After sell", wallet_dict)


@_close_client_decorator
async def buy_pairs_if_good_price(client: Optional[AsyncClient] = None):
    if want_to_buy_pairs:
        symbols = set()
        for pair in want_to_buy_pairs:
            symbols.add(pair[0])
            symbols.add(pair[1])
        price_dict = await _get_prices(symbols)
        buy_tasks = []
        for pair in list(want_to_buy_pairs):
            if pair in want_to_sell_pairs:
                want_to_buy_pairs.pop(pair)
                continue
            pair_params = want_to_buy_pairs[pair]
            b, synt_middle_price, sygma = pair_params['b'], pair_params['synt_middle_price'], pair_params['sygma']
            symb_1, symb_2 = pair
            order_amount_coeff = max(wallet_dict['current_capital'] / WALLET_START_PRICE, 1)
            if price_dict[symb_1] + b * price_dict[symb_2] <= synt_middle_price - sygma * SYGMA_MULTIPLIER:
                buy_tasks.append(buy_pair(symb_1, symb_2, price_dict, MIN_ORDER_AMOUNT * order_amount_coeff, client=client))
        if not buy_tasks:
            return
        results_list = await asyncio.gather(*buy_tasks)
        buy_something = False
        for order_long, order_short in results_list:
            if order_long is None or order_short is None:
                continue
            long_price, long_comission = _get_total_price_and_comission_order(order_long)
            short_price, short_comission = _get_total_price_and_comission_order(order_short)
            wallet_dict['current_capital'] -= long_price
            wallet_dict["current_capital"] += short_price
            wallet_dict['current_capital'] -= long_comission + short_comission
            pair = (order_long['symbol'], order_short['symbol'])
            pair_params = want_to_buy_pairs[pair]
            long_quantity, short_quantity = order_long['executedQty'], order_short['executedQty']

            want_to_sell_pairs[pair] = {
                'quantity_long': long_quantity,
                'quantity_short': short_quantity,
                'bought_time': datetime.now(),
                'synt_middle_price': pair_params['synt_middle_price'],
                'b': pair_params['b'],
                'sygma': pair_params['sygma']
            }
            want_to_buy_pairs.pop(pair)
            buy_something = True

        if buy_something:
            print("After bought", wallet_dict)


