import pandas as pd
import numpy as np
import haidata as hd

class DataTransformer(object):
    def __init__(self):
        self._functions = []
    
    @property
    def function(self, index=0):
        return self._functions[index] if self._functions else None
    
    def function_count(self):
        return len(self._functions)
        
    pass
