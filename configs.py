import pandas as pd
from binance.client import Client
import os


ACTUAL_VALUES_NUM = 30  # If we have less the ACTUAL_VALUES_NUM of price records for some currency we escape this currency
ACTUAL_FRAME = pd.Timedelta(days=1) # We think that data that older that ACTUAL_FRAME expired for cointegrations calculation, actual time window

TRADING_TIME_FRAME = Client.KLINE_INTERVAL_15MINUTE # Data from binance aggregated by this windows
FEE = 0.002 # Fee for binance
MIN_ORDER_AMOUNT = 50 # Minimum price of order in USDT
WALLET_START_PRICE = 1000  # We think that we start trading with this money amount in USDT. Better to get it from wallet, need to add sync with wallet
MIN_PRICE_MULTUPLY_LIMIT = 3  # Binance has limits for currency price, if price is closed to the limit and we buy we have variety to not buy/sell it
MAX_PRICE_MULTUPLY_LIMIT = 3  # the same param for max limit
ORDER_BOOK_MAX_DEPTH = 5 # Min binance depth for order book
VOLATILITY_LIMIT = FEE * 1.5 # We need some filtration by volatility, it's related to FEE
SYGMA_MULTIPLIER = 2 # If We have E(price) = e and sygma(price) = s for synthetic price for cointegrated pair,
                    # we will buy pair when price = e - SYGMA_MULTIPLIER * s, expectation is raising price to e

CURRENCY_LIMIT = 100 # Max number of currencies which we will load.
ACTUAL_DAYS_NUM = 170 # For backtest
TESTNET = True
MAX_SPREAD_LIMIT = 0.05 # Param for spread filter. If bigger then MAX_SPREAD_LIMIT we avoid this currency

API_KEY = os.getenv('API_KEY_BINANCE')
API_SECRET_KEY = os.getenv('API_SECRET_KEY_BINANCE')

TIMEOUT = pd.Timedelta(hours=12) # If we bought pair TIMEOUT ago and it has not raised yet, we sell it now by MARKET
# orders, time is over

SLEEP_AFTER_ITERATION = 1 # seconds. WE don't want to ping binance non-stop, it has limits for traffic.
# Sleep SLEEP_AFTER_ITERATION on every iteration in strategy

CHECK_COINTEGRATION_TIME_FRAME = pd.Timedelta(hours=2)  # We try to find actual cointegrations every CHECK_COINTEGRATION_TIME_FRAME

BLACK_LIST_TESTNET = set(("FLMUSDT", "LINAUSDT", "HOTUSDT", "ATAUSDT")) # Low liquidity for my tests, avoid it
