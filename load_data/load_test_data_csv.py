import datetime

import pandas as pd
from os import path
from pathlib import Path
from typing import Dict
from collections import Counter

test_file = path.join(Path(__file__).parent.parent, 'data_test', 'binance1.csv')

df = pd.read_csv(test_file, header=1)

df['close_diff'] = df['close'].diff(periods=-1)
df['close_diff_abs'] = df['close_diff'].abs()


def get_window_freq(df: pd.DataFrame, date_time: datetime.datetime, window_size: datetime.timedelta) -> Dict[int, float]:
    unix_min = datetime.datetime.timestamp(date_time)
    unix_max = datetime.datetime.timestamp(date_time + window_size)
    df_window = df[(df['unix'] >= unix_min) & (df['unix'] < unix_max)]
    sigma_positive = df_window[df_window['close_diff'] >= 0]['close_diff'].mean()
    sigma_negative = - df_window[df_window['close_diff'] < 0]['close_diff'].mean()
    
    count_1 = df_window[df_window['close_diff'] > 2 * sigma_positive]['close_diff'].count()
    count_2 = df_window[(df_window['close_diff'] > 0) & (df_window['close_diff'] <= 2 * sigma_positive)]['close_diff'].count()
    count_3 = df_window[(df_window['close_diff'] < 0) & (df_window['close_diff'] >= -2 * sigma_negative)]['close_diff'].count()
    count_4 = df_window[df_window['close_diff'] < -2 * sigma_negative]['close_diff'].count()
    count_sum = count_1 + count_2 + count_3 + count_4

    
    return count_1 / count_sum, count_2 / count_sum, count_3/count_sum, count_4 / count_sum, sigma_positive, sigma_negative
    
    
def window_points(df: pd.DataFrame, date_time: datetime.datetime, window_size: datetime.timedelta):
    prev_prev_date_time = date_time - 2 * window_size
    prev_date_time = date_time - window_size
    prev_prev_profit = get_window_profit(df, prev_prev_date_time, window_size, 1)
    prev_profit = get_window_profit(df, prev_date_time, window_size, 1)
    pp_f_1, pp_f_2, pp_f_3, pp_f_4, pp_s_n, pp_s_p= get_window_freq(df, prev_prev_date_time, window_size)
    p_f_1, p_f_2, p_f_3, p_f_4, p_s_n, p_s_p = get_window_freq(df, prev_date_time, window_size)
    counts = 0
    if p_f_2 * p_s_p > (p_f_3 + p_f_4) * p_s_n:
        counts += 1
    if p_f_2 < p_f_3 + p_f_4:
        counts = 0
    elif p_f_2 > p_f_3 + p_f_4 + p_f_1:
        counts += 1
    if p_f_2 * p_s_p + p_f_1 * 3*p_s_p > abs(p_f_3 * p_s_n + p_f_3 * 3 * p_s_n):
        counts += 1

    return counts if counts > 1 else 0
    

def get_window_profit(df: pd.DataFrame, date_time: datetime.datetime, window_size: datetime.timedelta, money: float):
    unix_min = datetime.datetime.timestamp(date_time)
    unix_max = datetime.datetime.timestamp(date_time + window_size)
    df_window = df[(df['unix'] >= unix_min) & (df['unix'] < unix_max)]
    real_unix_min, real_unix_max = df_window['unix'].min(), df_window['unix'].max()
    if len(df_window) == 0:
        return 0
    price_begin = df_window.loc[real_unix_min]['close'].mean()
    price_end = df_window.loc[real_unix_max]['close'].mean()
    return money * (
        (price_end - price_begin) / price_begin
    )


def get_window_max_profit(df: pd.DataFrame, date_time: datetime.datetime, window_size: datetime.timedelta, money: float):
    unix_min = datetime.datetime.timestamp(date_time)
    unix_max = datetime.datetime.timestamp(date_time + window_size)
    df_window = df[(df['unix'] >= unix_min) & (df['unix'] < unix_max)]
    real_unix_min, real_unix_max = df_window['unix'].min(), df_window['unix'].max()
    if len(df_window) == 0:
        return 0
    price_begin = df_window.loc[real_unix_min]['close'].mean()
    price_max = df_window['close'].max()
    return money * (
            (price_max - price_begin) / price_begin
    )


def get_window_min_profit(df: pd.DataFrame, date_time: datetime.datetime, window_size: datetime.timedelta, money: float):
    unix_min = datetime.datetime.timestamp(date_time)
    unix_max = datetime.datetime.timestamp(date_time + window_size)
    df_window = df[(df['unix'] >= unix_min) & (df['unix'] < unix_max)]
    real_unix_min, real_unix_max = df_window['unix'].min(), df_window['unix'].max()
    if len(df_window) == 0:
        return 0
    price_begin = df_window.loc[real_unix_min]['close'].mean()
    price_min = df_window['close'].min()
    return money * (
            (price_min - price_begin) / price_begin
    )


def get_window_median_profit(df: pd.DataFrame, date_time: datetime.datetime, window_size: datetime.timedelta, money: float):
    unix_min = datetime.datetime.timestamp(date_time)
    unix_max = datetime.datetime.timestamp(date_time + window_size)
    df_window = df[(df['unix'] >= unix_min) & (df['unix'] < unix_max)]
    real_unix_min, real_unix_max = df_window['unix'].min(), df_window['unix'].max()
    if len(df_window) == 0:
        return 0
    price_begin = df_window.loc[real_unix_min]['close'].mean()
    price_median = df_window['close'].median()
    return money * (
            (price_median - price_begin) / price_begin
    )



date_time = datetime.datetime.now() - datetime.timedelta(days=100)
time_delta = datetime.timedelta(days=3)
get_window_freq(df, date_time, time_delta)

# for i in range(20):
#     print(get_window_freq(df, date_time, time_delta))
#     date_time = date_time + datetime.timedelta(days=3)
    
    
def trade_all_windows():
    date_time = datetime.datetime.now() - datetime.timedelta(days=172)
    time_delta = datetime.timedelta(hours=18)
    # get_window_freq(df, date_time, time_delta)
    money = 1
    percent = 0.1
    counter_p = Counter()
    counter_n = Counter()
    
    for i in range(180):
        points = window_points(df, date_time, time_delta)
        profit = get_window_profit(df, date_time, time_delta, money=(points) * percent * money / 3)
        if profit > 0:
            counter_p[points] += 1
        elif profit < 0:
            counter_n[points] += 1
        else:
            profit_test = get_window_profit(df, date_time, time_delta, money= percent * money)
            if profit_test > 0:
                counter_p[0] += 1
            elif profit_test < 0:
                counter_n[0] += 1
        money += profit
        date_time += time_delta
    print(money)
    for i in range(4):
        print (i, counter_p[i], counter_n[i])
    return profit

# trade_all_windows()