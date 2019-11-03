import pandas as pd
import numpy as np
import os
import sys
import typing
from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.utils import utils
import matplotlib.pyplot as plt

from collections import defaultdict

from paprika.signals.signal_data import SignalData


class Assessor:
    # should we think about asessing multiple signals at once ? yes!
    _data = None
    _derived_data = defaultdict()

    def __init__(self, signal_data: SignalData):
        self._data = signal_data

    def plot_returns(self, prices_name: str = "prices", positions_name: str = "positions"):
        self.check_init(prices_name, positions_name)
        port_return = self._derived_data["port_returns"]
        plt.plot(self._data.get_frame(prices_name).index, port_return.cumsum())
        plt.xticks(rotation=45)
        plt.ylabel('Cumulative return')
        plt.title("Cointegration of {} vs. {}.".format(self._data.get_frame(prices_name).columns.values[0],
                                                       self._data.get_frame(prices_name).columns.values[1]))
        plt.show()

    def check_init(self, prices_name: str = "prices", positions_name: str = "positions"):
        if self._derived_data["returns"] is None:
            self._derived_data["returns"] = utils.returns_calculator(self._data.get_frame(prices_name).values, 1)
        if self._derived_data["port_returns"] is None:
            self._derived_data["port_returns"] = utils.portfolio_return_calculator(
                self._data.get_frame(positions_name).values,
                self._derived_data["returns"])

    def sharpe(self, periods_per_year, between_times, prices_name: str = "prices", positions_name: str = "positions"):
        self.check_init(prices_name, positions_name)
        utils.sharpe(self._derived_data["port_returns"].between_time(between_times[0], between_times[1]),
                     periods_per_year)

    def sharpe_cost_adj(self, periods_per_year, between_times, cost_per_transaction=0.0005, prices_name: str = "prices",
                        positions_name: str = "positions"):
        self.check_init(prices_name, positions_name)
        port_return_minus_costs = self._derived_data["port_returns"] - utils.simple_transaction_costs(
            self._data.get_frame(positions_name).values, cost_per_transaction)
        return utils.sharpe(port_return_minus_costs, periods_per_year, between_times)
