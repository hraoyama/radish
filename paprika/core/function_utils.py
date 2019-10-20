import functools
import numpy as np
import pandas as pd


def log_decorator(log_enabled):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if log_enabled:
                print(f"Calling Function: {func.__name__} with args {str(args)} and kwargs {str(kwargs)}")
            return func(*args, **kwargs)
        
        return wrapper
    
    return actual_decorator


def add_return_to_dict_or_pandas_col_decorator(return_dict):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal return_dict
            return_dict[args[0]] = func(*args, **kwargs)
        
        return wrapper
    
    return actual_decorator


def add_return_to_pandas_indexed_col_decorator(return_object):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal return_object
            if return_object.frame is not None:
                return_object.frame = pd.merge(return_object.frame, func(*args, **kwargs), how='outer', left_index=True,
                                               right_index=True)
            else:
                return_object.frame = func(*args, **kwargs)
        
        return wrapper
    
    return actual_decorator


def add_return_to_numpy_decorator(np_array, axis_num):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal np_array
            nonlocal axis_num
            np_array = np.append(np_array, func(*args, **kwargs), axis=axis_num)
        
        return wrapper
    
    return actual_decorator
