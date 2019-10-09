from paprika.data.data_utils import get_data_frame_from_csv
from paprika.core.function_utils import add_return_to_dict_or_pandas_col_decorator
from paprika.data.data_utils import new_data_upload

import functools
import pandas as pd
from datetime import datetime

if __name__ == "__main__":
    tickers = ['GLD_USO', 'EWA_EWC_IGE']
    
    to_agg_dict = dict()
    
    
    @add_return_to_dict_or_pandas_col_decorator(to_agg_dict)
    def f1(*args, **kwargs):
        return get_data_frame_from_csv(*args, **kwargs)
    
    
    for ticker in tickers:
        f1(f'inputData_{ticker}', r'../../../resources/data/')
    to_upload_frame = functools.reduce(lambda df1, df2: pd.concat([df1, df2], axis=1, ignore_index=False),
                                       to_agg_dict.values())
    
    date_index = [datetime.strptime(str(x), '%Y%m%d') for x in to_upload_frame.index]
    to_upload_frame = to_upload_frame.reset_index(drop=True)
    to_upload_frame['date'] = date_index
    to_upload_frame.set_index('date', inplace=True)
    
    new_data_upload(to_upload_frame, "_".join(tickers), "ONE_OBS")
