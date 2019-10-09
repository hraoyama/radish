from haidata.fix_colnames import fix_colnames
from haidata.extract_returns import extract_returns

from paprika.exchange.data_processor import DataProcessor
from paprika.data.feed_filter import FilterInterface
from paprika.data.feed_filter import TimePeriod, TimeFreqFilter
from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType
from paprika.utils.record import Timeseries

from pprint import pprint as pp
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from arch import arch_model
from absl import app, logging
import matplotlib.pyplot as plt


class DataSimulation(object):
    def __init__(self, *args, **kwargs):
        assert(isinstance(args[0], pd.DataFrame)), "Need input data type to be Pandas DataFrame"
        self._data = args[0]
        self._model = kwargs['sim_model'] if 'sim_model' in kwargs.keys() else 'Garch'
        self._field = kwargs['on'] if 'on' in kwargs.keys() else 'Price'
        self._ret_type = kwargs['ret_type'] if 'ret_type' in kwargs.keys() else 'log'
        self._split_date = kwargs['split_date'] if 'split_date' in kwargs.keys() else None
        self._kwargs = kwargs
        self._returns = pd.DataFrame()
        self._sim_returns = pd.DataFrame()
        self._sims = pd.DataFrame()

    def calc_returns(self, **kwargs):
        if self._ret_type == 'log':
            logging.info('Using log change as return.')
            return np.log(self._data[[self._field]]).diff().dropna()
        elif self._ret_type == 'pct':
            logging.info('Using percent change as return.')
            return self._data[[self._field]].pct_change().dropna()
        else:
            raise NotImplementedError

    def simulation(self, **kwargs):
        # model = kwargs['sim_model'] if 'sim_model' in kwargs.keys() else 'Garch'
        if 'arch' in self._model:
            logging.info(f'Using {self._model} model.')
            return self.sim_arch_family(**kwargs)
        else:
            raise NotImplementedError

    def sim_arch_family(self):
        dist = self._kwargs['dist'] if 'dist' in self._kwargs.keys() else 'Normal'
        mean = self._kwargs['mean'] if 'mean' in self._kwargs.keys() else 'Constant'
        lag = self._kwargs['lag'] if 'lag' in self._kwargs.keys() else 0
        p = self._kwargs['p'] if 'p' in self._kwargs.keys() else 1
        o = self._kwargs['o'] if '0' in self._kwargs.keys() else 0
        q = self._kwargs['q'] if 'q' in self._kwargs.keys() else 1
        horizon = self._kwargs['horizon'] if 'horizon' in self._kwargs.keys() else 1
        method = self._kwargs['method'] if 'method' in self._kwargs.keys() else 'simulation'

        am = arch_model(self.returns, vol=self._model, p=p, o=o, q=q, dist=dist, mean=mean, lags=lag)
        res = am.fit(update_freq=5, last_obs=self._split_date)
        logging.info(res.summary)
        if res.convergence_flag:
            logging.warning(f'Please choose another split date or change model parameter to allow fitting convergence')
        forecasts = res.forecast(horizon=horizon, method=method)
        dims = forecasts.simulations.values.shape
        sim_returns = forecasts.simulations.values.reshape(dims[0:2])
        return pd.DataFrame(sim_returns,
                            index=self._returns.index,
                            columns=[f'{self._field}_S{i}' for i in range(sim_returns.shape[1])]).dropna()

    # TODO: finish pct type
    def inverse_return(self):
        # split_date = kwargs['split_date'] if 'split_date' in kwargs.keys() else None
        split_nearest_date = self._data.index[self._data.index.get_loc(self._split_date, method='nearest')]
        if self._ret_type == 'log':
            logging.info('Using log change as return.')
            df = np.exp(self._sim_returns.cumsum())
        elif self._ret_type == 'pct':
            logging.info('Using percent change as return.')
            raise NotImplementedError
        else:
            raise NotImplementedError
        df += self._data.loc[split_nearest_date, self._field]
        return df

    @property
    def data(self):
        return self._data.copy()

    @property
    def sims(self):
        self._sim_returns = self.sim_arch_family()
        self._sims = self.inverse_return()
        # return pd.concat([self._sims, self._data], axis=1, sort=False)
        return self._sims.copy()

    @property
    def returns(self):
        self._returns = self.calc_returns()
        return self._returns.copy()


def test():
    # to see what is available
    source = 'mdb'
    # symbols = DataChannel.table_names(arctic_source_name=source)
    # for symbol in symbols:
    #     if 'Trade' in symbol:
    #         pp(symbol)
    #         data = DataChannel.download(symbol, arctic_source_name=source, string_format=False)
    #         print(data.index[0], data.index[-1], data.shape[0])

    # extract data for one symbol
    data = DataChannel.download('ETF.DE0002635307.Trade', arctic_source_name=source, string_format=False)
    pp(data.head())
    data = data[data.Price != 0]

    z = DataProcessor(data) \
        (TimeFreqFilter(TimePeriod.MINUTE, 15)).data

    # simulate the data through GARCH
    print(z.index[0], z.index[-1])
    split_date = datetime(2019, 2, 10)
    ret = DataSimulation(z, split_date=split_date, model='Garch').returns
    # plt.plot(ret)
    # plt.show()
    sim = DataSimulation(z, split_date=split_date, model='Garch').sims
    sim.plot()
    plt.show()
    pp(sim)


if __name__ == '__main__':
    test()

