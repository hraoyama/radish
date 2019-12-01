from paprika.data.data_processor import DataProcessor
from paprika.data.data_type import DataType
from paprika.data.processor_utils import get_return_series
from paprika.data.processor_utils import merge_data_frames_in_dict_values
from paprika.core.function_utils import \
    add_return_to_dict_or_pandas_col_decorator  # , add_return_to_pandas_indexed_col_decorator

def test_get_return_series():
    DataProcessor.MAKE_AVAILABLE_IN_FEEDS = False
    
    return_dict = dict()
    tickers = ['GLD', 'GDX']
    
    @add_return_to_dict_or_pandas_col_decorator(return_dict)
    def set_df_get_return_series(*args, **kwargs):
        return get_return_series(*args, **kwargs)
    
    for ticker in tickers:
        set_df_get_return_series(ticker, DataType.OHLCVAC_PRICE,
                                 extract_returns_args=("Adj Close", "LOG_RETURN", f'{ticker}_Close'))
    
    for index, val in enumerate(return_dict.items()):
        print(val[1].shape)
    
    merged_data = merge_data_frames_in_dict_values(return_dict)
    print(merged_data.head(3))
    print(merged_data.tail(3))
    print(merged_data.shape)
