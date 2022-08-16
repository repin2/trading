import pandas as pd
from pathlib import Path
from os import path
from datetime import datetime, timedelta
import numpy as np

from models.train.checking_median_window_profit import CheckinMedianWindowProfitModel, CheckinMedianWindowProfitModelSVM
from matplotlib import pyplot

from load_test_data_csv import get_window_freq, get_window_profit, get_window_max_profit, \
    get_window_min_profit, get_window_median_profit


def load_binance_csv_tick(file_name):
    file_path = path.join(Path(__file__).parent.parent, 'data_test', file_name)

    df = pd.read_csv(file_path, names=['id', 'close', 'qty', 'quoteQty', 'unix', 'isBuyerMaker', 'Somebool'])
    df['unix'] /= 1000
    df.drop('id', axis=1, inplace=True)
    df.drop('Somebool', axis=1, inplace=True)
    df['unix_index'] = df['unix']
    df.set_index('unix_index', inplace=True)

    df['close_diff'] = df['close'].diff(periods=-1)
    df['close_diff_abs'] = df['close_diff'].abs()
    return df

df = load_binance_csv_tick('BTCUSDT-trades-2022-07-09.csv')

min_datetime = datetime.fromtimestamp(df.unix.min())
current_datetime = min_datetime
max_datetime = datetime.fromtimestamp(df.unix.max())
window_size = timedelta(seconds=2)

from collections import Counter
counter = Counter()
i = 0

freq_list = []
min_f_2, min_f_3, min_f_4 = 10000, 10000, 10000
profit_list, check_list = [], []
train_var = []
profit_train = []

while current_datetime <= max_datetime - window_size:
    i += 1
    f_1, f_2, f_3, f_4, s_p, s_n = get_window_freq(df, current_datetime, window_size)
    train_var.append([f_1, f_2, f_3, f_4, s_p, s_n, 1, s_p * s_n, s_p * f_2, s_n * f_3, s_p * f_1, s_n * f_4, f_1*f_2 - f_3*f_4])
    profit = get_window_median_profit(df, current_datetime, window_size, 1)
    profit_train.append(profit)
    if f_4 < min_f_4:
        min_f_4 = f_4
    if f_3 < min_f_3:
        min_f_3 = f_3
    if f_2 < min_f_2:
        min_f_2 = f_2
    check = s_p * (2 * f_1 + f_2) - s_n * (f_3 + 2 * f_4)
    profit_list.append((profit, check))

    if check >= 0:
        if profit > 0:
            counter['1_1'] += 1
        else:
            counter['1_0'] += 1
    freq_list.append([current_datetime, f_1, f_2,])

    if i % (600 - 1) == 0:
        # checking stationary row

        # df_f_2 = pd.DataFrame(freq_list, columns=['datetime', 'f_2', 'f_3'])
        # df_f_2.set_index('datetime', inplace=True)
        # df_f_2['f_1'] -= min_f_1
        # df_f_2['f_2'] -= min_f_2
        # x = df_f_2.plot(figsize=(12, 6))
        # pyplot.show()
        # df_profit_check = pd.DataFrame(profit_list, columns=['profit', 'checks'])
        # df_profit_check.plot(x='checks', y='profit', kind='scatter')
        # pyplot.show()
        train_var = np.array(train_var)
        profit_train = np.array(profit_train)
        profit_train = np.sign(profit_train)
        regress_model = CheckinMedianWindowProfitModelSVM(train_vars=train_var, train_profit=profit_train)
        regress_model.test_predict()
        assert 1
    current_datetime += window_size

assert 1