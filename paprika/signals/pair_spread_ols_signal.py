import numpy as np
import pandas as pd
from typing import List, Tuple
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from collections import deque
from pprint import pprint as pp

from paprika.data.fetcher import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import TimeFreqFilter, Filtration, TimePeriod


class PairSpread(FeedSubscriber):
    def __init__(self, **kwargs):
        super(PairSpread, self).__init__(**kwargs)
        
        self.scaler = StandardScaler() if self.get_parameter("NUM_OBS") else None
        self.history = deque([np.nan] * int(self.get_parameter("NUM_OBS"))) if self.scaler else None
        self.beta = self.get_parameter("BETA")
        self.keep_track = pd.DataFrame()
        self.num_y_obs = 0;
        self.num_x_obs = 0;
        self.last_y = np.nan;
        self.last_x = np.nan;
        
        # this we don't need to create these (duplicate data - but doing for clarity)
        self.sd = self.get_parameter("SD_SUPPLIED")
        self.sd_factor = self.get_parameter("SD_FACTOR")
        self.beta_update_factor = self.get_parameter("BETA_UPDATE_FACTOR")
        self.beta_history = []
        #
        
        # make sure we don't supply a standard deviation and then require
        # standard deviation to be calculated from observations (does not compute!)
        assert self.scaler is None if self.sd else not None
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(PairSpread, self).handle_event(events)
        data = events[0][1]
        
        if any(data.ISIN == self.get_parameter("Y")):
            self.num_y_obs = 1 # only keep the last observation
            self.last_y = data.loc[data.ISIN == self.get_parameter("Y"), "Price"][-1]
        if any(data.ISIN == self.get_parameter("X")):
            self.num_x_obs = 1
            self.last_x = data.loc[data.ISIN == self.get_parameter("X"), "Price"][-1]
        
        if self.num_x_obs == self.num_y_obs and self.num_x_obs > 0:
            # we have data, get the residual
            residual = self.last_y - self.beta * self.last_x
            
            # update the beta if this feature is used
            if self.beta_update_factor:
                self.beta = self.beta * self.beta_update_factor if np.sign(residual) > 0 else \
                    self.beta / self.beta_update_factor
                self.beta_history.append(self.beta)
                residual = self.last_y - self.beta * self.last_x
            
            timestamp = data.index.max()
            
            if self.history:
                self.history.append(residual)
                self.history.popleft()
                valid_history = list(filter(lambda x: ~np.isnan(x), self.history))
                if len(valid_history) >= self.get_parameter("NUM_OBS"):
                    # use historical standard deviation if this feature is used
                    normalized = self.scaler.fit_transform(np.array(valid_history).reshape(-1, 1))
                    normalized = list(normalized.reshape(1, -1)[0])
                    norm_residual = normalized[-1]
                    if np.abs(norm_residual) > self.sd_factor:
                        self.keep_track = self.keep_track.append(pd.DataFrame({'DateTime': [timestamp],
                                                                               'Y': [
                                                                                   -1.0 if norm_residual > 0.0 else 1.0],
                                                                               'X': [
                                                                                   1.0 if norm_residual > 0.0 else -1.0]})
                                                                 )
                    else:
                        self.keep_track = self.keep_track.append(
                            pd.DataFrame({'DateTime': [timestamp], 'Y': [np.nan], 'X': [np.nan]}))
                else:
                    self.keep_track = self.keep_track.append(
                        pd.DataFrame({'DateTime': [timestamp], 'Y': [np.nan], 'X': [np.nan]}))
            else:
                # use the supplied standard deviation
                if np.abs(residual) > self.sd_factor * self.sd:
                    self.keep_track = self.keep_track.append(pd.DataFrame({'DateTime': [timestamp],
                                                                           'Y': [-1.0 if residual > 0.0 else 1.0],
                                                                           'X': [1.0 if residual > 0.0 else -1.0]}))
                else:
                    self.keep_track = self.keep_track.append(
                        pd.DataFrame({'DateTime': [timestamp], 'Y': [np.nan], 'X': [np.nan]}))
            self.num_x_obs = 0
            self.num_y_obs = 0
    
    def run(self):
        super(PairSpread, self).run()
        self.set_parameter("WEIGHT_ALLOCATION", self.keep_track)
        self.set_parameter("BETA_HISTORY", self.beta_history)


if __name__ == "__main__":
    
    # to check what is available:
    # print(DataChannel.table_names(arctic_source_name='mdb'))
    
    # set up the pairs data you will be looking at
    pairs_feed = Feed('PAIRS_FEED', datetime(2017, 7, 1), datetime(2017, 7, 28))
    pairs_feed.set_feed(["EUX.FBTP201709", "EUX.FBTS201709"], DataType.TRADES)
    
    # how much data did we actually get?
    pp(pairs_feed.shape)
    
    pair_signal = PairSpread(Y="FBTP201709", X="FBTS201709", BETA=1.2, SD_SUPPLIED=1.2, SD_FACTOR=0.5)
    pair_signal.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    pairs_feed.add_subscriber(pair_signal)

    # execute the signal
    pair_signal.run()

    # show some of your results
    print(pair_signal.get_parameter("WEIGHT_ALLOCATION"))
    print(pair_signal.get_parameter("BETA_HISTORY"))
    print(pair_signal.parameters)
    
    # feel free to uncomment, moved most of it to the test file (under tests)
    # this will get removed to eliminate clutter once familiarity is gained

    # # let's adapt the beta with time
    # pair_signal_2 = PairSpread(Y="FBTP201709", X="FBTS201709",
    #                          BETA=1.2, BETA_UPDATE_FACTOR = 1.01,
    #                          SD_SUPPLIED=1.2, SD_FACTOR=0.5)
    # pair_signal_2.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    # pairs_feed.add_subscriber(pair_signal_2)
    # pair_signal_2.run()
    # print(pair_signal_2.get_parameter("WEIGHT_ALLOCATION"))
    # print(pair_signal_2.get_parameter("BETA_HISTORY"))
    # print(pair_signal_2.parameters)
    #
    # # let's use the rolling historical standard deviation
    # pair_signal_3 = PairSpread(Y="FBTP201709", X="FBTS201709",
    #                            BETA=1.2, SD_FACTOR=2.0, NUM_OBS=20.0)
    # pair_signal_3.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    # pairs_feed.add_subscriber(pair_signal_3)
    # pair_signal_3.run()
    # print(pair_signal_3.get_parameter("WEIGHT_ALLOCATION"))
    # print(pair_signal_3.get_parameter("BETA_HISTORY"))
    # print(pair_signal_3.parameters)
    #
    # # let's use the rolling historical standard deviation and some adaptive beta
    # pair_signal_4 = PairSpread(Y="FBTP201709", X="FBTS201709",
    #                            BETA=1.2, SD_FACTOR=1.0, NUM_OBS=20.0, BETA_UPDATE_FACTOR=1.05)
    # pair_signal_4.add_filtration(Filtration(TimeFreqFilter(TimePeriod.SECOND, 600)))
    # pairs_feed.add_subscriber(pair_signal_4)
    # pair_signal_4.run()
    # print(pair_signal_4.get_parameter("WEIGHT_ALLOCATION"))
    # print(pair_signal_4.get_parameter("BETA_HISTORY"))
    # print(pair_signal_4.parameters)

    # clear out your used feeds
    DataChannel.clear_all_feeds()
