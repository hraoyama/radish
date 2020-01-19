import numpy as np
import pandas as pd
from typing import Optional, Tuple, Union, List, Callable
from functools import wraps
from datetime import datetime
from multiprocessing import Pool
import statsmodels.api as sm
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
    alpha_pattern = re.compile('alpha_.*')

    def __init__(self, dp: DataProcessor, alpha_pattern_list: Optional[List[str]] = []):
        self._alpha_universe = {}
        self.load_alpha_universe()
        self._dp = dp
        self._alpha = None
        if len(alpha_pattern_list):
            self.calc_alpha(alpha_pattern_list)
        self._beta = {}

    def __getitem__(self, name: str) -> pd.DataFrame:
        return self._alpha[name].unstack('Symbol')

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
        alphas = {}
        for name, alpha in self._alpha_universe.items():
            for alpha_pattern in alpha_patterns:
                if alpha_pattern.match(name):
                    alphas[name] = alpha(self._dp)
                    # self._alpha[name] = alpha(self._dp)
        df = pd.concat(alphas)
        df.index.names = ['Alpha', 'Start_Period']
        self._alpha = df.groupby(['Start_Period', 'Alpha']).first().unstack('Alpha').stack('Symbol')

    def list_alpha(self) -> List[str]:
        return self._alpha.columns.to_list()

    def list_alpha_universe(self) -> List[str]:
        return list(self._alpha_universe.keys())

    def add_alpha(self, alpha: Callable, *args, **kwargs):
        name = alpha.__name__
        self._alpha_universe[name] = alpha
        if self._alpha is None:
            self._alpha = pd.DataFrame(alpha(self._dp, *args, **kwargs).stack('Symbol'),
                                       columns=[name])
        else:
            self._alpha[name] = alpha(self._dp, *args, **kwargs).stack('Symbol')

    def alpha_info(self, name: str) -> str:
        try:
            return self._alpha_universe[name].info
        except AttributeError:
            return f'No alpha info about {name}.'

    @property
    def list_symbols(self):
        return self._dp.close.columns.to_list()

    # def analyze(self):
    #     # TODO it's a slow way
    #     df = pd.DataFrame(columns=self.symbols,
    #                       index=self._alpha.keys())
    #     for name, value in self._alpha.items():
    #         df.loc[name, :] = value.corrwith(self._dp.ret)

    def corr_between_alphas(self):
        # TODO it's a slow way

        df = self._alpha.unstack('Symbol').stack('Alpha')
        df_corr = {}
        for symbol in df.columns:
            tmp = df.loc[:, symbol].unstack('Alpha')
            df_corr[symbol] = tmp.corr()

        df_corr = pd.concat(df_corr)
        df_corr.index.name = ['Symbol', 'Alpha']
        return df_corr

    def corr_between_alpha_and_return(self):
        # TODO it's a slow way
        df = pd.DataFrame(columns=self.list_symbols,
                          index=self._alpha.columns)
        for name, value in self._alpha.items():
            df.loc[name, :] = value.unstack('Symbol').corrwith(self._dp.ret.shift(-1))
        return df

    def add_beta(self, beta: List[Beta]):
        raise NotImplementedError

    def list_beta(self):
        return list(self._beta.keys())

    def analyze_whole(self,
                      predict_period: Optional[int] = 1,
                      mini_period: Optional[int] = 200):
        symbols = self._alpha.index.get_level_values('Symbol')
        for symbol in symbols:
            alpha = self._alpha.xs(symbol, level='Symbol')
            ret = self._dp.ret[symbol]
            for start in range(mini_period + predict_period, ret.shape[0]):

                pass

    def analyze_with_lookback_window(self,
                                     lookback_window: Optional[int] = 60,
                                     predict_period: Optional[int] = 1):
        raise NotImplementedError

