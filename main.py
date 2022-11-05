from connections.binance import *
from matplotlib import pyplot as plt
from algorithms.coint_algs import coint_test, get_coint_params
from trade.strategy import strategy

from backtest.simple_test import run_backtest


if __name__ == '__main__':
    # Todo: run strategy() or run backtest()
    asyncio.run(set_leverage())
    orders_list = strategy()






