from pandas import DataFrame, Timedelta
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from typing import Optional
from algorithms.coint_algs import get_coint_pairs_with_params, get_synthetic_df, process_syntetic_df

current_coint_pairs_data = {}
from configs import FEE, TIMEOUT


def get_future_df(trading_dict: Dict[str, DataFrame], currency: str, current_time: Optional[datetime]= None,
                  timeout: timedelta = TIMEOUT):
    if current_time is None:
        current_time = datetime.now()
    actual_df = trading_dict[currency]
    actual_df = actual_df[(actual_df.open_time >= current_time) & (actual_df.open_time <= current_time + timeout)]
    return actual_df


def estimate_pair_profit(trading_dict: DataFrame, currency_1: str, currency_2: str, math_ex: float, b: float,
                         sigma: float, current_time: Optional[datetime] = None, capital = 0):
    if current_time is None:
        current_time = datetime.now()

    df_1 = get_future_df(trading_dict, currency_1, current_time, timeout=TIMEOUT)
    df_2 = get_future_df(trading_dict, currency_2, current_time, timeout=TIMEOUT)
    if len(df_1) != len(df_2):
        return None, False
    synth_df = get_synthetic_df(df_1, df_2, b)
    buy_time, sell_time, is_timeout = process_syntetic_df(synth_df, math_ex, sigma)
    if buy_time is None:
        return None, False

    curr_1_buy_price = trading_dict[currency_1].loc[buy_time].price
    curr_1_sell_price = trading_dict[currency_1].loc[sell_time].price

    curr_2_buy_price = trading_dict[currency_2].loc[buy_time].price
    curr_2_sell_price = trading_dict[currency_2].loc[sell_time].price

    curr_1_profit = 0.05 * capital * (curr_1_sell_price * (1 - FEE) / (curr_1_buy_price * (1 + FEE)) - 1)
    curr_2_profit = 0.05 * capital * (1 - curr_2_sell_price * (1 - FEE) / (1 + FEE) /curr_2_buy_price)

    return curr_1_profit + curr_2_profit, is_timeout


def show_stats(trades: List[Tuple[datetime, float, bool]]):
    profit_trades = [x for x in trades if x[1] > 0]
    negative_trades = [x for x in trades if x[1] < 0]
    print (" profit  %", len(profit_trades) / len(trades))
    print (" negative  %", len(negative_trades) / len(trades))
    print(" Total Profit", sum([x[1] for x in trades]))
    print(" Middle Profit", sum([x[1] for x in profit_trades]) / len(profit_trades))
    print(" Middle Negative", sum([x[1] for x in negative_trades]) / len(negative_trades))
    print ("% of timeout:", len([x for x in trades if x[2]]) / len (trades))


def test_purchase(trading_dict: Dict[str, DataFrame]):
    capital = 1000
    delta = Timedelta(hours=1)
    random_key = list(trading_dict)[0]
    current_time = trading_dict[random_key].open_time.values[0]
    finish_time = trading_dict[random_key].open_time.values[-1] - TIMEOUT
    trades = []

    while current_time < finish_time:
        current_coint_dict, current_coint_params, current_order = get_coint_pairs_with_params(trading_dict, current_time)
        if current_coint_dict is None:
            current_time += delta
            continue
        best_pair = current_order[0]
        engle, pval = current_coint_dict[best_pair][0], current_coint_dict[best_pair][1]
        if engle > current_coint_dict[best_pair][2][1]:
            current_time += delta
            print( f"No trade, engle > crit: {engle}, {current_coint_dict[best_pair][2][1]}")
            continue
        best_math_ex, best_b, best_mse, _ = current_coint_params[best_pair]

        engle, pval = current_coint_dict[best_pair][0], current_coint_dict[best_pair][1]


        profit, is_timeout = estimate_pair_profit(trading_dict, best_pair[0], best_pair[1], best_math_ex, best_b,
                                                   best_mse, current_time, capital)
        if profit is None:
            pass

        else:
            capital += profit
            print (current_time, profit, is_timeout, capital)
            trades.append((current_time, profit, is_timeout))
        current_time += delta
    show_stats(trades)


def run_backtest():
    # ToDo: Create full backtest function with checkings of volatilities, spreads etc on historical windows
    pass





