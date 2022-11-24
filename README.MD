My Trading bot
=========

SET GLOBAL ENVS `API_KEY_BINANCE` and `API_SECRET_KEY_BINANCE`. Run `main` locally, docker container will be done later

`main.py` is calling `trade.strategy`. See doc in strategy.
Don't see `backtest` module, this does not work now

Main strategy patterns are collected in `trade.utils`.
`connections` module is for connections. It has only binance connections.
`algorithms` has `coint_algs` for calculation of cointegration and `volatility` for volatility filter.

To see how I find actual cointegrated pairs see function `_get_current_cointegrated_pairs` 