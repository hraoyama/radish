import numpy as np
import pandas as pd
from typing import Optional, Tuple, Union, List, Callable
from functools import wraps
from datetime import datetime
from multiprocessing import Pool
import os
import importlib
import re
from absl import logging

from paprika.data.data_processor import DataProcessor
from paprika.alpha.beta import Beta
from paprika.alpha.utils import *
from paprika.data.feed_filter import TimeFreqFilter


class Alpha(object):
    alpha_folder = os.path.join(os.path.dirname(__file__), 'alpha')
    alpha_pattern = re.compile('alpha_\w*')

    def __init__(self, dp: DataProcessor, alpha_pattern_list: Optional[List[str]] = []):
        self._alpha_universe = {}
        self.load_alpha_universe()
        self._dp = dp
        self._alpha = {}
        if len(alpha_pattern_list):
            self.calc_alpha(alpha_pattern_list)
        self._beta = {}

    def __getitem__(self, name: str) -> pd.DataFrame:
        return self._alpha[name]

    def load_alpha_universe(self):
        self._alpha_universe = {}
        logging.info(f'Load Alpha from folder as {self.alpha_folder}')
        for file in os.listdir(self.alpha_folder):
            if self.alpha_pattern.match(file):
                logging.info(f'Load Alpha from file as {file}')
                module_name = f"paprika.alpha.alpha.{file.replace('.py', '')}"
                import_module = importlib.import_module(module_name)
                for _attr in dir(import_module):
                    if self.alpha_pattern.match(_attr):
                        # logging.info(f'Load Alpha as {_attr}')
                        self._alpha_universe[_attr] = getattr(import_module, _attr)

    def calc_alpha(self, alpha_pattern_list: List[str]):
        alpha_patterns = [re.compile(x) for x in alpha_pattern_list]
        use_alpha = {}
        for name, alpha in self._alpha_universe.items():
            for alpha_pattern in alpha_patterns:
                if alpha_pattern.match(name):
                    use_alpha[name] = alpha
                    self._alpha[name] = alpha(self._dp)

    def list_alpha(self):
        return list(self._alpha.keys())

    def list_alpha_universe(self):
        return list(self._alpha_universe.keys())

    def add_alpha(self, alpha: Callable, *args, **kwargs):
        name = alpha.__name__
        self._alpha_universe[name] = alpha
        self._alpha[name] = alpha(self._dp, *args, **kwargs)

    def alpha_info(self, name: str):
        try:
            return self._alpha_universe[name].info
        except AttributeError:
            return None

    def analyze(self):
        raise NotImplementedError

    def add_beta(self, beta: List[Beta]):
        raise NotImplementedError

    def list_beta(self):
        return list(self._beta.keys())


