from paprika.core.function_utils import log_decorator, add_return_to_dict_or_pandas_col_decorator, add_return_to_pandas_indexed_col_decorator

import pandas as pd
import numpy as np


def test_custom_decorators():
    @log_decorator(True)
    def f1(x):
        print(f'{str(x), str(x)}')
    
    f1(2)
    
    accumulate_dict = dict()
    @add_return_to_dict_or_pandas_col_decorator(accumulate_dict)
    def f2(identifier, x):
        return x * x
    f2('thrity', 30)
    f2('three', 3)
    print(accumulate_dict)
    
    accumulate_df = pd.DataFrame()
    @add_return_to_dict_or_pandas_col_decorator(accumulate_df)
    def f3(identifier, x):
        return [x, x * x, x + x]
    f3('thrity', 30)
    f3('three', 3)
    print(accumulate_df)

