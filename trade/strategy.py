import datetime

from trade.utils import *


# ToDO: create full strategy

def strategy():
    """
    In this function we need to:
    RUN iterations in cycle while True
    This iteration must do:
       1    BUY cointegrated pairs when price is good
       2    SELL bought pairs when price is good
       3    SELL pairs when time is over

    It means that:
     Any iteration must do under hood:
        1 Checking prices for cointegrated pairs, if price is good Buy it
        2 Checking prices for cointegrated pairs, if price is good Sell it
    Sometimes before calling of iteration we must update info about pairs:
        1 Finding current cointegrated pairs, good prices for them
        2 Control of bought pairs: If we bought non-cointegrated pair or price for sell is good
        we need to sell it immediately
    """
    last_checking_time = None
    while True:
        if last_checking_time is None or datetime.now() - CHECK_COINTEGRATION_TIME_FRAME >= last_checking_time:
            last_checking_time = datetime.now()
            asyncio.run(update_new_cointegrations())
        populate_immediately_sell_pairs()
        asyncio.run(sell_pairs_from_sell_immediately_if_have())
        asyncio.run(buy_pairs_if_good_price())






