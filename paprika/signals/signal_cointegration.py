import numpy as np
import pandas as pd
from typing import List, Tuple
import logging
from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber


class CointegrationSpread(FeedSubscriber):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.beta = self.get_parameter("BETA")
        self.mean = self.get_parameter("MEAN")
        self.sigma = self.get_parameter("STD")
        self.entry = self.get_parameter("ENTRY")
        self.exit = self.get_parameter("EXIT")
        self.positions = pd.DataFrame()
        self.prices = pd.DataFrame()

    def calc_position(self, z_score, data1, data2):
        if z_score <= -self.entry:
            self.positions = self.positions.append(
                pd.DataFrame({'DateTime': [data1.index],
                              data1['Symbol']: [1.0],
                              data2['Symbol']: [-1.0]}))
        elif z_score >= self.entry:
            self.positions = self.positions.append(
                pd.DataFrame({'DateTime': [data1.index],
                              data1['Symbol']: [-1.0],
                              data2['Symbol']: [1.0]}))
        elif np.abs(z_score) <= self.exit:
            self.positions = self.positions.append(
                pd.DataFrame({'DateTime': [data1.index],
                              data1['Symbol']: [0.0],
                              data2['Symbol']: [0.0]}))
        else:
            self.positions = self.positions.append(
                pd.DataFrame({'DateTime': [data1.index],
                              data1['Symbol']: [np.nan],
                              data2['Symbol']: [np.nan]}))
        logging.info(f'{self.positions}')

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)

        if len(events) == 2:
            event1 = events[0]
            event2 = events[0]
            if event1[0] == DataType.TRADES and event2[0] == DataType.TRADES:
                data1 = event1[1]
                data2 = event2[1]
                if data1.index == data2.index:
                    self.prices = self.prices.append(
                        pd.DataFrame({'DateTime': [data1.index],
                                      data1['Symbol']: data1['Price'],
                                      data2['Symbol']: data2['Price']}))
                    z_score = (data1['Price'] - self.beta * data2['Price'] - self.mean) / self.sigma
                    self.calc_position(z_score, data1, data2)
            else:
                raise TypeError(f'Different data types ({event1[0]}, {event2[0]}')

        else:
            logging.info(f'There are {len(events)} events.')



        # # data = events[0][1]
        # timestamp = data.index[0]
        # self.prices = self.prices.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': data['GLD'][0], 'GDX': data['GDX'][0]}))
        # z_score = (data['GLD'][0] - self.beta*data['GDX'][0] - self.mean) / self.sigma
        #
        # if z_score <= -self.entry:
        #     self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [1.0], 'GDX': [-1.0]}))
        # elif z_score >= self.entry:
        #     self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [-1.0], 'GDX': [1.0]}))
        # elif np.abs(z_score) <= self.exit:
        #     self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [0.0], 'GDX': [0.0]}))
        # else:
        #     self.positions = self.positions.append(pd.DataFrame({'DateTime': [timestamp], 'GLD': [np.nan], 'GDX': [np.nan]}))
        #
        #

