from paprika.exchange.data_processor import DataProcessor
from paprika.data.data_type import DataType
from paprika.core.function_utils import add_return_to_dict_or_pandas_col_decorator
from paprika.exchange.processor_utils import get_return_series
from paprika.exchange.processor_utils import merge_data_frames_in_dict_values

import numpy as np
import pandas as pd

def test_alloc_Kelly():
    DataProcessor.MAKE_AVAILABLE_IN_FEEDS = False

    return_dict = dict()
    tckrs = ['GLD', 'GDX', 'IGE', 'KO', 'OIH', 'PEP', 'RKH', 'RTH', 'SPY']

    @add_return_to_dict_or_pandas_col_decorator(return_dict)
    def set_df_get_return_series(*args, **kwargs):
        return get_return_series(*args, **kwargs)

    for ticker in tckrs:
        set_df_get_return_series(ticker, DataType.OHLCVAC_PRICE,
                                 extract_returns_args=("Adj Close", "LOG_RETURN", f'{ticker}_Close'))

    merged_data = merge_data_frames_in_dict_values(return_dict)
    print(merged_data.shape)
    print(merged_data.tail(10))

    oos_number = 300
    iis_data = merged_data[:merged_data.index[-oos_number]].values;
    risk_free = 0.04 / 252
    iis_data -= risk_free
    # annualized returns
    M = 252 * np.nanmean(iis_data, axis=0)
    C = 252 * pd.DataFrame(iis_data, columns=merged_data.columns.values).cov().values
    # 252 * np.cov(iis_data.T) # need to transpose matrix
    # iis_corr = merged_data[:merged_data.index[-oos_number]].corr().values
    
    # Kelly optimal leverages
    F = np.dot(np.linalg.inv(C), M)

    # maximum compounded growth rate of a multi-strategy Gaussian process is
    g = 252 * risk_free + 0.5 * np.dot(F, np.dot(C, F))
    # the respective Sharpe ratio
    
    temp = np.dot(F, np.dot(C, F))
    sharp = np.sqrt(np.abs(temp)) * np.sign(temp)
    print(f'Best possible Sharpe from this portfolio is {str(sharp)} according to Kelly criterion')
    