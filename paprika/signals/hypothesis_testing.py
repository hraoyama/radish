# Hypothesis Testing on a Futures Momentum Strategy
import numpy as np
import pandas as pd
from paprika.utils import utils
import os
from scipy.stats import pearson3

PATH = r'D:\_enterprise\strats\data'

def main():

    df = pd.read_csv(os.path.join(PATH, 'TU.csv'))
    df['Time'] = pd.to_datetime(df['Time']).dt.date  # remove HH:MM:SS
    df.set_index('Time', inplace=True)

    lookback = 250
    holding_period = 25

    longs = df['Close'] > df['Close'].shift(lookback)
    shorts = df['Close'] < df['Close'].shift(lookback)

    pos = np.zeros(df.shape[0])

    for h in range(0, holding_period):
        long_lag = longs.shift(h)
        long_lag[long_lag.isna()] = False
        long_lag = long_lag.astype(bool)

        short_lag = shorts.shift(h)
        short_lag[short_lag.isna()] = False
        short_lag = short_lag.astype(bool)

        pos[long_lag] = pos[long_lag] + 1
        pos[short_lag] = pos[short_lag] - 1

    capital = np.nansum(np.array(pd.DataFrame(np.abs(pos)).shift(1)), axis=1)
    pos[capital == 0, ] = 0
    capital[capital == 0] = 1

    market_return = utils.returns_calculator(df['Close'], 1)
    ret = utils.pnl_calculator(pos, market_return) / capital / holding_period
    sharpe = np.sqrt(len(ret)) * np.nanmean(ret) / np.nanstd(ret)

    print("Gaussian Test statistic=%f" % sharpe)
    # Gaussian Test statistic=2.769741

    # Randomized market returns hypothesis test
    # =============================================================================
    # _,_,mean,var,skew,kurt=describe(marketRet, nan_policy='omit')
    # =============================================================================
    skew_, loc_, scale_ = pearson3.fit(market_return[1:])  # First element is NaN
    num_samples_better_observed = 0
    for sample in range(10000):
        market_return_simulated = \
            pearson3.rvs(skew=skew_, loc=loc_, scale=scale_, size=market_return.shape[0], random_state=sample)
        cl_sim = np.cumproduct(1 + market_return_simulated) - 1

        longs_sim = cl_sim > pd.Series(cl_sim).shift(lookback)
        shorts_sim = cl_sim < pd.Series(cl_sim).shift(lookback)

        pos_sim = np.zeros(cl_sim.shape[0])

        for h in range(0, holding_period):
            long_sim_lag = longs_sim.shift(h)
            long_sim_lag[long_sim_lag.isna()] = False
            long_sim_lag = long_sim_lag.astype(bool)

            short_sim_lag = shorts_sim.shift(h)
            short_sim_lag[short_sim_lag.isna()] = False
            short_sim_lag = short_sim_lag.astype(bool)

            pos_sim[long_sim_lag] = pos_sim[long_sim_lag] + 1
            pos_sim[short_sim_lag] = pos_sim[short_sim_lag] - 1

        capital = np.nansum(np.array(pd.DataFrame(np.abs(pos_sim)).shift(1)), axis=1)
        pos_sim[capital == 0, ] = 0
        capital[capital == 0] = 1

        ret_sim = utils.pnl_calculator(pos_sim, market_return_simulated) / capital / holding_period
        if np.mean(ret_sim) >= np.mean(ret):
            num_samples_better_observed += 1

    print("Randomized prices: p-value=%f" % (num_samples_better_observed / 10000))
    # Randomized prices: p-value=23.617800

    # Randomized entry trades hypothesis test
    num_samples_better_observed = 0
    for sample in range(10000):
        P = np.random.permutation(len(longs))
        longs_sim = longs[P]
        shorts_sim = shorts[P]

        pos_sim = np.zeros(cl_sim.shape[0])

        for h in range(0, holding_period):
            long_sim_lag = longs_sim.shift(h)
            long_sim_lag[long_sim_lag.isna()] = False
            long_sim_lag = long_sim_lag.astype(bool)

            short_sim_lag = shorts_sim.shift(h)
            short_sim_lag[short_sim_lag.isna()] = False
            short_sim_lag = short_sim_lag.astype(bool)

            pos_sim[long_sim_lag] = pos_sim[long_sim_lag] + 1
            pos_sim[short_sim_lag] = pos_sim[short_sim_lag] - 1

        capital = np.nansum(np.array(pd.DataFrame(abs(pos_sim)).shift()), axis=1)
        pos_sim[capital == 0, ] = 0
        capital[capital == 0] = 1

        ret_sim = utils.pnl_calculator(pos_sim, market_return) / capital / holding_period
        if np.mean(ret_sim) >= np.mean(ret):
            num_samples_better_observed += 1

    print("Randomized trades: p-value=%f" % (num_samples_better_observed / 10000))
    # Randomized trades: p-value=1.365600


if __name__ == "__main__":
    main()
