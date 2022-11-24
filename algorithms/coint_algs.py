import pandas as pd
from datetime import datetime
from typing import Optional
from statsmodels.tsa.stattools import coint
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from pandas import DataFrame
from typing import Dict

from configs import ACTUAL_VALUES_NUM, ACTUAL_FRAME


def get_coint_pairs_with_params(trading_dict: Dict[str, DataFrame], current_time: Optional[pd.Timestamp] = None):
    """
    Function to find pairs with volatility, their linear combination and the same pairs sorted by results of stats test
    """
    if current_time is None:
        current_time = pd.Timestamp(datetime.now())
    results, params = {}, {}
    currencies = list(trading_dict)
    currencies.sort()

    for i in range(len(currencies)):
        currency_1 = currencies[i]
        df_currency_1 = trading_dict[currency_1]
        actual_df_currency_1 = df_currency_1[
            (df_currency_1['open_time'] < current_time)
                &
            (df_currency_1['open_time'] >= current_time - ACTUAL_FRAME)]
        if len(actual_df_currency_1) < ACTUAL_VALUES_NUM:
            continue
        for j in range(len(currencies)):
            if i == j:
                continue
            currency_2 = currencies[j]
            df_currency_2 = trading_dict[currency_2]
            actual_df_currency_2 = df_currency_2[(df_currency_2['open_time'] < current_time)
                &
            (df_currency_2['open_time'] >= current_time - ACTUAL_FRAME)]
            if len(actual_df_currency_2) < ACTUAL_VALUES_NUM:
                continue
            if len(actual_df_currency_1) != len(actual_df_currency_2):
                continue
            # May be use numba or taichi to improve calculations performance
            results[(currency_1, currency_2)] = coint_test(actual_df_currency_1, actual_df_currency_2)   # Engle grage coint test
            params[(currency_1, currency_2)] = get_coint_params(actual_df_currency_1, actual_df_currency_2)  # Finding linear combination by simple linear regression

    if not results:
        return None, None, None
    pairs_coint_order = list(results)
    pairs_coint_order.sort(key=lambda x: results[x][0])
    return results, params, pairs_coint_order


def coint_test(window_1: DataFrame, window_2: DataFrame):
    price_1, price_2 = window_1['price'].values, window_2['price'].values
    result = coint(price_1, price_2)
    return result


def get_synthetic_df(window_1: DataFrame, window_2: DataFrame, b: float):
    synthetic_price = window_1['price'].values - window_2['price'].values * b
    synthetic_df = pd.DataFrame({"price": synthetic_price, "open_time": window_1['open_time'].values})
    return synthetic_df


def process_syntetic_df(synt_df: DataFrame, math_ex: float, sigma: float):
    # None, None, False - no trades
    threshold = math_ex - sigma
    buy_time, sell_time = None, None
    timeout = False
    for open_time, price in zip(synt_df.open_time.values, synt_df.price.values):
        if buy_time is None and price <= threshold:
            buy_time = open_time
        elif buy_time is not None and price >= math_ex:
            sell_time = open_time
    if buy_time is not None and sell_time is None:
        sell_time = synt_df.open_time.values[-1]
        timeout = True

    return buy_time, sell_time, timeout


def get_coint_params(window_1: DataFrame, window_2: DataFrame, return_df=True):
    length = len(window_2['price'].values)
    model = LinearRegression().fit(
        window_2['price'].values.reshape((length, 1)),
        window_1['price'].values
    )
    mse = mean_squared_error(
        window_1['price'].values,
        model.predict(window_2['price'].values.reshape((length, 1))),
        squared=False
    )
    if return_df:
        synthetic_price = window_1['price'].values - window_2['price'].values * model.coef_[0]
        synthetic_df = pd.DataFrame({"price": synthetic_price, "open_time": window_1['open_time'].values})
    else:
        synthetic_df = None
    return model.intercept_, model.coef_[0], mse, synthetic_df

