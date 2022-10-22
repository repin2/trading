from pandas import DataFrame
from typing import Dict
from configs import VOLATILITY_LIMIT


def _get_df_volatility(df: DataFrame) -> float:
    return (df['price'].diff().abs()[1:] / df['price'][:-1]).mean()


def get_all_volatilities(current_history_dict: Dict[str, DataFrame]):
    result = []
    for symbol, df in current_history_dict.items():
        result.append((symbol, _get_df_volatility(df)))
    result.sort(key=lambda x: x[1], reverse=True)
    return result


def filter_by_volatilities(current_history_dict: Dict[str, DataFrame]):
    result_dict = {}
    for symbol, volatility in get_all_volatilities(current_history_dict):
        if volatility >= VOLATILITY_LIMIT:
            result_dict[symbol] = current_history_dict[symbol]
    return result_dict