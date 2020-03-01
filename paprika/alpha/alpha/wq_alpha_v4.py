# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 16:12:17 2019

@author: zhang
"""
import sqlite3

conn = sqlite3.connect("D:/Mega Data/yueDB")
c = conn.cursor()
from alpha.alpha_util_v4 import *
def WQ_1(input_df):	
    '''(rank(Ts_ArgMax(SignedPower(((returns < 0)?stddev(returns, 20):close), 2), 5))-0.5)'''	
    df = input_df.copy()	
    df['pct_change'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp1'] = df.groupby('ticker')['pct_change'].rolling(20,min_periods=16).std().values	
    df['tmp2'] = df['tmp1'] * (df['pct_change']<0).astype(int) + df['close']* (df['pct_change']>=0).astype(int)	
    df['tmp3'] = df['tmp2'] ** 2	
    df['tsrank'] = df.groupby('ticker')['tmp3'].rolling(5,min_periods=4).apply(np.argmax,raw=True).values	
    df['WQ_1'] = (df['tsrank'] * df['valid']).dropna().groupby(df['date']).rank(pct=True) - 0.5	
    df['WQ_1'] *= df['valid']	
    return df[['ticker','date','WQ_1']]	
def WQ_2(input_df):	
    '''(-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))'''	
    df = input_df.copy()	
    df['tmp1'] = df['volume'].apply(np.log).groupby(df['ticker']).diff(2)	
    df['tmp2'] = df['close'] / df['open'] - 1	
    df['rank1'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_2'] = qk_corr(df,'rank1','rank2',6) * -1	
    df['WQ_2'] *= df['valid']	
    return df[['ticker','date','WQ_2']]	
def WQ_3(input_df):	
    '''(-1 * correlation(rank(open), rank(volume), 10))'''	
    df = input_df.copy()	
    df['rank1'] = (df['raw_open'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_3'] = qk_corr(df,'rank1','rank2',10) * -1	
    df['WQ_3'] *= df['valid']	
    return df[['ticker','date','WQ_3']]    	
def WQ_4(input_df):	
    '''(-1 * Ts_Rank(rank(low), 9))'''	
    df = input_df.copy()	
    df['rank'] = (df['raw_low'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_4'] = qk_ts_rank(df,'rank',9) * -1	
    df['WQ_4'] *= df['valid']	
    return df[['ticker','date','WQ_4']]	
def WQ_5(input_df):	
    '''(rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['vwap'].rolling(10,min_periods=8).mean().values	
    df['tmp2'] = df['open'] - df['tmp1']	
    df['tmp3'] = df['close'] - df['vwap']	
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_5'] = -df['rank1'] * df['rank2']	
    df['WQ_5'] *= df['valid']	
    return df[['ticker','date','WQ_5']]    	
def WQ_6(input_df):	
    '''(-1 * correlation(open, volume, 10))'''	
    df = input_df.copy()	
    df['WQ_6'] = qk_corr(df,'open','volume',10) * -1	
    df['WQ_6'] *= df['valid']	
    return df[['ticker','date','WQ_6']]	
def WQ_7(input_df):	
    '''((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1* 1))'''	
    df=input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['tmp2'] = (df['tmp1'] < df['volume']).astype(int) * df['tmp1'] / df['tmp1']	
    df['tmp3'] = df.groupby('ticker')['close'].diff(7).abs()	
    df['ts_rank'] = qk_ts_rank(df,'tmp3',60)	
    df['tmp4'] = -1 * df['ts_rank'] * df.groupby('ticker')['close'].diff(7).apply(np.sign)	
    df['WQ_7'] = df['tmp4'] * df['tmp2'] + (-1) * (1 - df['tmp2'])	
    df['WQ_7'] *= df['valid']	
    return df[['ticker','date','WQ_7']]	
def WQ_8(input_df):	
    '''(-1 * rank(((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)),10))))'''	
    df=input_df.copy()	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['sum1'] = df.groupby('ticker')['open'].rolling(5,min_periods=4).mean().values	
    df['sum2'] = df.groupby('ticker')['return'].rolling(5,min_periods=4).mean().values	
    df['tmp1'] = df['sum1'] * df['sum2']	
    df['tmp2'] = df.groupby('ticker')['tmp1'].diff(10)	
    df['WQ_8'] = -1 * (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_8'] *= df['valid']	
    return df[['ticker','date','WQ_8']]	
def WQ_9(input_df):	
    '''((0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) :	
        ((ts_max(delta(close, 1), 5) < 0) ?	
         delta(close, 1) : (-1 * delta(close, 1))))'''	
    df=input_df.copy()	
    df['diff'] = df.groupby('ticker')['close'].diff()	
    df['tmp1'] = df.groupby('ticker')['diff'].rolling(5,min_periods=4).min().values	
    df['tmp2'] = df.groupby('ticker')['diff'].rolling(5,min_periods=4).max().values	
    df['con'] = ((df['tmp1']>0)|(df['tmp2']<0)).astype(int) * df['tmp1'] / df['tmp1']	
    df['WQ_9'] = (df['diff'] * df['con'] + (-1) * df['diff'] * (1-df['con'])) * df['prc_fact']	
    df['WQ_9'] *= df['valid']	
    return df[['ticker','date','WQ_9']]	
def WQ_10(input_df):	
    ''''''	
    df=input_df.copy()	
    '''rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) :	
        ((ts_max(delta(close, 1), 4) < 0)? delta(close, 1) : (-1 * delta(close, 1)))))'''	
    df['diff'] = df.groupby('ticker')['close'].diff()	
    df['tmp1'] = df.groupby('ticker')['diff'].rolling(4,min_periods=4).min().values	
    df['tmp2'] = df.groupby('ticker')['diff'].rolling(4,min_periods=4).max().values	
    df['con'] = ((df['tmp1']>0)|(df['tmp2']<0)).astype(int) * df['tmp1'] / df['tmp1']	
    df['tmp3'] = df['diff'] * df['con'] + (-1) * df['diff'] * (1-df['con'])	
    df['WQ_10' ] = (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_10'] *= df['valid']	
    return df[['ticker','date','WQ_10']]	
def WQ_11(input_df):	
    '''((rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) * 	
    rank(delta(volume, 3)))'''	
    df=input_df.copy()	
    df['tmp1'] = (df['close'] - df['vwap']).groupby(df['ticker']).rolling(3).max().values	
    df['tmp2'] = (df['close'] - df['vwap']).groupby(df['ticker']).rolling(3).min().values	
    df['tmp3'] = df.groupby('ticker')['volume'].diff(3)	
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank3'] = (df['tmp3'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_11'] = (df['rank1'] + df['rank2']) * df['rank3']	
    df['WQ_11'] *= df['valid']	
    return df[['ticker','date','WQ_11']]	
def WQ_12(input_df):	
    '''(sign(delta(volume, 1)) * (-1 * delta(close, 1)))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['volume'].diff().apply(np.sign)	
    df['tmp2'] = -1 * df.groupby('ticker')['close'].diff()	
    df['WQ_12'] = df['tmp1'] * df['tmp2'] * df['prc_fact']	
    df['WQ_12'] *= df['valid']	
    return df[['ticker','date','WQ_12']]	
def WQ_13(input_df):	
    '''(-1 * rank(covariance(rank(close), rank(volume), 5)))'''	
    df = input_df.copy()	
    df['rank1'] = (df['raw_close'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['cov'] = qk_cov(df,'rank1','rank2',5)	
    df['WQ_13'] = -1 * (df['cov'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_13'] *= df['valid']	
    return df[['ticker','date','WQ_13']]	
def WQ_14(input_df):	
    '''((-1 * rank(delta(returns, 3))) * correlation(open, volume, 10))'''	
    df = input_df.copy()	
    df['corr'] = qk_corr(df,'open','volume',10)	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp1'] = df.groupby('ticker')['return'].diff(3)	
    df['rank1'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_14'] = -1 * df['rank1'] * df['corr']	
    df['WQ_14'] *= df['valid']	
    return df[['ticker','date','WQ_14']]	
def WQ_15(input_df):	
    '''(-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3))'''	
    df = input_df.copy()	
    df['rank1'] = (df['raw_high'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['corr1'] = qk_corr(df,'rank1','rank2',3)	
    df['rank3'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_15'] = df.groupby('ticker')['rank3'].rolling(3,min_periods=3).sum().values	
    df['WQ_15'] *= df['valid']	
    return df[['ticker','date','WQ_15']]	
def WQ_16(input_df):	
    '''(-1 * rank(covariance(rank(high), rank(volume), 5)))'''	
    df = input_df.copy()	
    df['rank1'] = (df['raw_high'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['cov'] = qk_cov(df,'rank1','rank2',5)	
    df['WQ_16'] = -1 * (df['cov'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_16'] *= df['valid']	
    return df[['ticker','date','WQ_16']]	
def WQ_17(input_df):	
    '''(((-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1))) *	
        rank(ts_rank((volume / adv20), 5)))'''	
    df = input_df.copy()	
    df['ts_rank1'] = qk_ts_rank(df,'close',10)	
    df['diff'] = df.groupby('ticker')['close'].diff()	
    df['diff1'] = df.groupby('ticker')['diff'].diff()	
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['tmp2'] = df['volume'] / df['tmp1']	
    df['ts_rank2'] = qk_ts_rank(df,'tmp2',5)	
    df['rank1'] = (df['ts_rank1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['diff1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank3'] = (df['ts_rank2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_17'] = -1 * df['rank1'] * df['rank2'] * df['rank3']	
    df['WQ_17'] *= df['valid']	
    return df[['ticker','date','WQ_17']]	
def WQ_18(input_df):	
    '''(-1 * rank(((stddev(abs((close - open)), 5) + (close - open)) 	
    + correlation(close, open,10))))'''	
    df = input_df.copy()	
    df['tmp1'] = (df['close'] - df['open']).abs().groupby(df['ticker']).rolling(5,min_periods=4).std().values	
    df['corr'] = qk_corr(df,'close','open',10)	
    df['tmp2'] = (df['tmp1'] + df['open'] + df['close']) * df['prc_fact'] + df['corr']	
    df['WQ_18'] = -1 * (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_18'] *= df['valid']	
    return df[['ticker','date','WQ_18']]	
def WQ_19(input_df):	
    '''((-1 * sign(((close - delay(close, 7)) + delta(close, 7)))) 	
    * (1 + rank((1 + sum(returns,250)))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].diff(7).apply(np.sign) * -1	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp2'] = df.groupby('ticker')['return'].rolling(250,min_periods=200).mean().values * 250 + 1	
    df['rank1'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_19'] = df['tmp1'] * (1 + df['rank1'])	
    df['WQ_19'] *= df['valid']	
    return df[['ticker','date','WQ_19']]	
def WQ_20(input_df):	
    '''(((-1 * rank((open - delay(high, 1)))) * rank((open - delay(close, 1)))) * rank((open -	
delay(low, 1))))'''	
    df = input_df.copy()	
    df['tmp1'] = df['open'] - df.groupby('ticker')['high'].shift()	
    df['tmp2'] = df['open'] - df.groupby('ticker')['close'].shift()	
    df['tmp3'] = df['open'] - df.groupby('ticker')['low'].shift()	
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank3'] = (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_20'] = -1 * df['rank1'] * df['rank2'] * df['rank3']	
    df['WQ_20'] *= df['valid']	
    return df[['ticker','date','WQ_20']]	
def WQ_21(input_df):	
    '''((((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? (-1 * 1) : 	
        (((sum(close,2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 :	
        (((1 < (volume / adv20)) || ((volume / adv20) == 1)) ? 1 : (-1 * 1))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].rolling(8,min_periods=7).mean().values	
    df['tmp2'] = df.groupby('ticker')['close'].rolling(8,min_periods=7).std().values	
    df['tmp3'] = df.groupby('ticker')['close'].rolling(2,min_periods=2).mean().values	
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['con1'] = ((df['tmp1'] + df['tmp2']) < df['tmp3']).astype(int)	
    df['con2'] = ((df['tmp1'] - df['tmp2']) > df['tmp3']).astype(int)	
    df['con3'] = (((df['tmp1'] + df['tmp2']) >= df['tmp3']) 	
                    & ((df['tmp1'] - df['tmp2']) <= df['tmp3'])	
                    & (df['volume'] >= df['tmp4'])).astype(int)	
    df['con4'] = (((df['tmp1'] + df['tmp2']) >= df['tmp3']) 	
                    & ((df['tmp1'] - df['tmp2']) <= df['tmp3'])	
                    & (df['volume'] < df['tmp4'])).astype(int)	
    df['WQ_21'] = -df['con1'] + df['con2'] + df['con3'] - df['con4']	
    df['WQ_21'] *= df['valid']	
    return df[['ticker','date','WQ_21']]	
def WQ_22(input_df):	
    '''(-1 * (delta(correlation(high, volume, 5), 5) * rank(stddev(close, 20))))'''	
    df = input_df.copy()	
    df['corr'] = qk_corr(df,'high','volume',5)	
    df['tmp1'] = df.groupby('ticker')['corr'].diff(5)	
    df['std'] = df.groupby('ticker')['close'].rolling(20,min_periods=16).std().values	
    df['rank'] = (df['std'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_22'] = -1 * df['tmp1'] * df['rank']	
    df['WQ_22'] *= df['valid']	
    return df[['ticker','date','WQ_22']]	
def WQ_23(input_df):	
    '''(((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['high'].rolling(20,min_periods=16).mean().values	
    df['tmp2'] = -1 * df.groupby('ticker')['high'].diff(2)	
    df['WQ_23'] = df['tmp2'] * (df['high']>df['tmp1']).astype(int) * df['prc_fact']	
    df['WQ_23'] *= df['valid']	
    return df[['ticker','date','WQ_23']]	
def WQ_24(input_df):	
    '''((((delta((sum(close, 100) / 100), 100) / delay(close, 100)) < 0.05) ||	
    ((delta((sum(close, 100) / 100), 100) / delay(close, 100)) == 0.05)) ? 	
        (-1 * (close - ts_min(close,100))) : (-1 * delta(close, 3)))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].rolling(100,min_periods=80).mean().values	
    df['tmp2'] = df.groupby('ticker')['tmp1'].diff(100) / df.groupby('ticker')['close'].shift(100)	
    df['con'] = (df['tmp2'] <= 0.05).astype(int) * df['tmp2'] / df['tmp2']	
    df['tmp3'] = df.groupby('ticker')['close'].rolling(100,min_periods=80).min().values	
    df['tmp4'] = -df.groupby('ticker')['close'].diff(3)	
    df['WQ_24'] = ((df['tmp3'] - df['close']) * df['con'] + df['tmp4'] * (1-df['con'])) * df['prc_fact']	
    df['WQ_24'] *= df['valid']	
    return df[['ticker','date','WQ_24']]	
def WQ_25(input_df):	
    '''rank(((((-1 * returns) * adv20) * vwap) * (high - close)))'''	
    df = input_df.copy()	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['tmp2'] = -1 * df['return'] * df['tmp1'] * df['vol_fact'] * df['raw_vwap'] * (df['raw_high'] - df['raw_low'])	
    df['WQ_25'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_25'] *= df['valid']	
    return df[['ticker','date','WQ_25']]	
def WQ_26(input_df):	
    '''(-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3))'''	
    df = input_df.copy()	
    df['ts_rank1'] = qk_ts_rank(df,'volume',5)	
    df['ts_rank2'] = qk_ts_rank(df,'high',5)	
    df['corr'] = qk_corr(df,'ts_rank1','ts_rank2',5)	
    df['WQ_26'] = -1 * df.groupby('ticker')['corr'].rolling(3).max().values	
    df['WQ_26'] *= df['valid']	
    return df[['ticker','date','WQ_26']]	
def WQ_27(input_df):	
    '''((0.5 < rank((sum(correlation(rank(volume), rank(vwap), 6), 2) / 2.0))) ? (-1 * 1) : 1)'''	
    df = input_df.copy()	
    df['rank1'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_vwap'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['corr'] = qk_corr(df,'rank1','rank2',6)	
    df['tmp1'] = df.groupby('ticker')['corr'].rolling(2,min_periods=2).mean().values	
    df['rank3'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_27'] = ((df['rank3'] > 0.5).astype(int) * df['rank3']/df['rank3'] - 0.5) * 2	
    df['WQ_27'] *= df['valid']	
    return df[['ticker','date','WQ_27']]	
def WQ_28(input_df):	
    '''scale(((correlation(adv20, low, 5) + ((high + low) / 2)) - close))'''	
    df = input_df.copy()	
    df['adv20'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['corr'] = qk_corr(df,'adv20','low',5)	
    df['WQ_28'] = df['corr'] + ((df['high'] + df['low']) / 2 - df['close']) * df['prc_fact']	
    df['WQ_28'] *= df['valid']	
    return df[['ticker','date','WQ_28']]	
def WQ_29(input_df):	
    '''(min(product(rank(rank(scale(log(sum(ts_min(rank(rank((-1 * rank(delta((close - 1),5))))), 2), 1))))), 1), 5)	
    + ts_rank(delay((-1 * returns), 6), 5))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].diff(5)	
    df['rank1'] = (1 - df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp2'] = df.groupby('ticker')['rank1'].rolling(2).min().values	
    df['rank2'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp3'] = df.groupby('ticker')['rank2'].rolling(5).min().values	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp4'] = -1 * df.groupby('ticker')['return'].shift(6)	
    df['ts_rank1'] = qk_ts_rank(df,'tmp4',5)	
    df['WQ_29'] = df['tmp3'] + df['ts_rank1']	
    df['WQ_29'] *= df['valid']	
    return df[['ticker','date','WQ_29']]	
def WQ_30(input_df):	
    '''(((1.0 - rank(((sign((close - delay(close, 1))) + sign((delay(close, 1) - delay(close, 2)))) + 	
    sign((delay(close, 2) - delay(close, 3)))))) * sum(volume, 5)) / sum(volume, 20))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].diff().apply(np.sign)	
    df['tmp2'] = df['tmp1'] + df.groupby('ticker')['tmp1'].shift() + df.groupby('ticker')['tmp1'].shift(2)	
    df['rank1'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(5,min_periods=4).mean().values * 5	
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values * 20	
    df['WQ_30'] = (1 - df['rank1']) * df['tmp3'] / df['tmp4']	
    df['WQ_30'] *= df['valid']	
    return df[['ticker','date','WQ_30']]	
def WQ_31(input_df):	
    '''((rank(rank(rank(decay_linear((-1 * rank(rank(delta(close, 10)))), 10)))) + 	
    rank((-1 * delta(close, 3)))) + sign(scale(correlation(adv20, low, 12))))'''	
    df = input_df.copy()	
    df['tmp1'] = -1 * df.groupby('ticker')['close'].diff(10)	
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp2'] = qk_decay_linear(df,'rank1',10)	
    df['rank2'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp3'] = -1 * df.groupby('ticker')['close'].diff(3)	
    df['rank3'] = (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['adv20'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['corr'] = qk_corr(df,'adv20','low',12)	
    df['sign'] = df['corr'].apply(np.sign)	
    df['WQ_31'] = df['rank2'] + df['rank3'] + df['sign']	
    df['WQ_31'] *= df['valid']	
    return df[['ticker','date','WQ_31']]	
def WQ_32(input_df):	
    '''(scale(((sum(close, 7) / 7) - close)) + (20 * scale(correlation(vwap, delay(close, 5),230))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].rolling(7,min_periods=6).mean().values	
    df['tmp2'] = (df['tmp1']- df['close']) * df['prc_fact']	
    df['tmp3'] = df['tmp2'] / df['tmp2'].abs().groupby(df['date']).sum()	
    df['tmp4'] = df.groupby('ticker')['close'].shift(5)	
    df['corr'] = qk_corr(df,'vwap','tmp4',230)	
    df['tmp5'] = df['corr'] / df['corr'].abs().groupby(df['date']).sum()	
    df['WQ_32'] = df['tmp3'] + 20 * df['tmp5']	
    df['WQ_32'] *= df['valid']	
    return df[['ticker','date','WQ_32']]	
def WQ_33(input_df):	
    '''rank((-1 * ((1 - (open / close))^1)))'''	
    df = input_df.copy()	
    df['tmp'] = df['open'] / df['close'] - 1	
    df['WQ_33'] = (df['tmp'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_33'] *= df['valid']	
    return df[['ticker','date','WQ_33']]	
def WQ_34(input_df):	
    '''rank(((1 - rank((stddev(returns, 2) / stddev(returns, 5)))) + (1 - rank(delta(close, 1)))))'''	
    df = input_df.copy()	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp1'] = df.groupby('ticker')['return'].rolling(2).std().values	
    df['tmp2'] = df.groupby('ticker')['return'].rolling(5).std().values	
    df['rank1'] = 1 - ((df['tmp1'] / df['tmp2']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp3'] = df.groupby('ticker')['close'].diff()	
    df['rank2'] = 1- (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_34'] = df['rank1'] + df['rank2']	
    df['WQ_34'] *= df['valid']	
    return df[['ticker','date','WQ_34']]	
def WQ_35(input_df):	
    '''((Ts_Rank(volume, 32) * (1 - Ts_Rank(((close + high) - low), 16))) * (1 -Ts_Rank(returns, 32)))'''	
    df = input_df.copy()	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['ts_rank1'] = qk_ts_rank(df,'volume',32)	
    df['tmp1'] = df['close'] + df['high'] - df['low']	
    df['ts_rank2'] = qk_ts_rank(df,'tmp1',16)	
    df['ts_rank3'] = qk_ts_rank(df,'return',32)	
    df['WQ_35'] = df['ts_rank1'] * (1 - df['ts_rank2']) * (1 - df['ts_rank3'])	
    df['WQ_35'] *= df['valid']	
    return df[['ticker','date','WQ_35']]	
def WQ_36(input_df):	
    '''(((((2.21 * rank(correlation((close - open), delay(volume, 1), 15))) + (0.7 * rank((open- close))))	
    + (0.73 * rank(Ts_Rank(delay((-1 * returns), 6), 5)))) + rank(abs(correlation(vwap,adv20, 6))))	
        + (0.6 * rank((((sum(close, 200) / 200) - open) * (close - open)))))'''	
    df = input_df.copy()	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp1'] = df.groupby('ticker')['volume'].shift()	
    df['tmp2'] = df['close'] - df['open']	
    df['corr1'] = qk_corr(df,'tmp1','tmp2',15)	
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = ((df['raw_open'] - df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp3'] = -1 * df.groupby('ticker')['return'].shift(6)	
    df['ts_rank1'] = qk_ts_rank(df,'tmp3',5)	
    df['rank3'] = (df['ts_rank1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['adv20'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['corr2'] = qk_corr(df,'vwap','adv20',6)	
    df['rank4'] = (df['corr2'].abs() * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp4'] = df.groupby('ticker')['close'].rolling(200,min_periods=160).mean().values	
    df['tmp5'] = (df['tmp4'] - df['open']) * (df['close'] - df['open'])	
    df['rank5'] = (df['tmp5'] * df['prc_fact'] ** 2 * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_36'] = 2.21 * df['rank1'] + 0.7 * df['rank2'] + 0.73 * df['rank3'] + df['rank4'] + 0.6 * df['rank5'] 	
    df['WQ_36'] *= df['valid']	
    return df[['ticker','date','WQ_36']]	
def WQ_37(input_df):	
    '''(rank(correlation(delay((open - close), 1), close, 200)) + rank((open - close)))'''	
    df = input_df.copy()	
    df['tmp1'] = (df['open'] - df['close']).groupby(df['ticker']).shift()	
    df['corr'] = qk_corr(df,'tmp1','close',200)	
    df['rank1'] = (df['corr'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = ((df['raw_open'] - df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_37'] = df['rank1'] + df['rank2']	
    df['WQ_37'] *= df['valid']	
    return df[['ticker','date','WQ_37']]	
def WQ_38(input_df):	
    '''((-1 * rank(Ts_Rank(close, 10))) * rank((close / open)))'''	
    df = input_df.copy()	
    df['ts_rank'] = qk_ts_rank(df,'close',10)	
    df['rank1'] = (df['ts_rank'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['close'] / df['open'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_38'] = -1 * df['rank1'] * df['rank2']	
    df['WQ_38'] *= df['valid']	
    return df[['ticker','date','WQ_38']]	
def WQ_39(input_df):	
    '''((-1 * rank((delta(close, 7) * (1 - rank(decay_linear((volume / adv20), 9)))))) * (1 +rank(sum(returns, 250))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].diff(7)	
    df['adv20'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['tmp2'] = df['volume'] / df['adv20']	
    df['tmp3'] = qk_decay_linear(df,'tmp2',9)	
    df['rank1'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['tmp1'] * df['prc_fact'] * (1 - df['rank1']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp4'] = df.groupby('ticker')['return'].rolling(250,min_periods=200).mean().values * 250	
    df['rank3'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_39'] = -1 * df['rank2'] * (1 + df['rank3'])	
    df['WQ_39'] *= df['valid']	
    return df[['ticker','date','WQ_39']]	
def WQ_40(input_df):	
    '''((-1 * rank(stddev(high, 10))) * correlation(high, volume, 10))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['high'].rolling(10,min_periods=8).std().values	
    df['rank'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['corr'] = qk_corr(df,'high','volume',10)	
    df['WQ_40'] = -1 * df['rank'] * df['corr']	
    df['WQ_40'] *= df['valid']	
    return df[['ticker','date','WQ_40']]	
def WQ_41(input_df):	
    '''(((high * low)^0.5) - vwap)'''	
    df = input_df.copy()	
    df['WQ_41'] = ((df['high'] * df['low']) ** 0.5 - df['vwap']) * df['prc_fact']	
    df['WQ_41'] *= df['valid']	
    return df[['ticker','date','WQ_41']]	
def WQ_42(input_df):	
    '''(rank((vwap - close)) / rank((vwap + close)))'''	
    df = input_df.copy()	
    df['rank1'] = ((df['raw_vwap'] - df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = ((df['raw_vwap'] + df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_42'] = df['rank1'] / df['rank2']	
    df['WQ_42'] *= df['valid']	
    return df[['ticker','date','WQ_42']]	
def WQ_43(input_df):	
    '''(ts_rank((volume / adv20), 20) * ts_rank((-1 * delta(close, 7)), 8))'''	
    df = input_df.copy()	
    df['adv20'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['tmp1'] = df['volume'] / df['adv20']	
    df['ts_rank1'] = qk_ts_rank(df,'tmp1',20)	
    df['tmp2'] = -1 * df.groupby('ticker')['close'].diff(7)	
    df['ts_rank2'] = qk_ts_rank(df,'tmp2',8)	
    df['WQ_43'] = df['ts_rank1'] * df['ts_rank2']	
    df['WQ_43'] *= df['valid']	
    return df[['ticker','date','WQ_43']]	
def WQ_44(input_df):	
    '''(-1 * correlation(high, rank(volume), 5))'''	
    df = input_df.copy()	
    df['rank'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_44'] = -1 * qk_corr(df, 'high','rank',5)	
    df['WQ_44'] *= df['valid']	
    return df[['ticker','date','WQ_44']]	
def WQ_45(input_df):	
    '''(-1 * ((rank((sum(delay(close, 5), 20) / 20)) * correlation(close, volume, 2)) 	
    * rank(correlation(sum(close, 5), sum(close, 20), 2))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].shift(5)	
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values	
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['corr1'] = qk_corr(df, 'close','volume',2)	
    df['tmp3'] = df.groupby('ticker')['close'].rolling(5,min_periods=4).mean().values * 5	
    df['tmp4'] = df.groupby('ticker')['close'].rolling(20,min_periods=16).mean().values * 20	
    df['corr2'] = qk_corr(df, 'tmp3','tmp4',2)	
    df['rank2'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_45'] = -1 * df['rank1'] * df['corr1'] * df['rank2']	
    df['WQ_45'] *= df['valid']	
    return df[['ticker','date','WQ_45']]	
def WQ_46(input_df):	
    '''((0.25 < (((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10))) ? 	
    (-1 * 1) : (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < 0) ? 	
    1 : ((-1 * 1) * (close - delay(close, 1)))))'''	
    df = input_df.copy()	
    df['tmp1'] = - df.groupby('ticker')['close'].diff(10)	
    df['tmp2'] = - df.groupby('ticker')['tmp1'].diff(10) / 10	
    df['con1'] = (df['tmp2'] > 0.25).astype(int) * df['tmp2'] / df['tmp2']	
    df['con2'] = (df['tmp2'] < 0).astype(int) * df['tmp2'] / df['tmp2']	
    df['con3'] = ((df['tmp2'] <= 0.25)&(df['tmp2'] >=0)).astype(int) * df['tmp2'] / df['tmp2']	
    df['WQ_46'] = -1 * df['con1'] + 1 * df['con2'] - df.groupby('ticker')['close'].diff() * df['con3'] * df['prc_fact']	
    df['WQ_46'] *= df['valid']	
    return df[['ticker','date','WQ_46']]	
def WQ_47(input_df):	
    '''((((rank((1 / close)) * volume) / adv20) * ((high * rank((high - close))) / (sum(high, 5) /5)))	
    - rank((vwap - delay(vwap, 5))))'''	
    df = input_df.copy()	
    df['rank1'] = (1 / df['raw_close'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['adv20'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values	
    df['rank2'] = ((df['raw_high'] - df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp1'] = df.groupby('ticker')['vwap'].diff(5)	
    df['tmp2'] = df.groupby('ticker')['high'].rolling(5,min_periods=4).mean().values	
    df['rank3'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_47'] = df['rank1'] * df['volume'] / df['adv20'] * df['high'] * df['rank2'] / df['tmp2'] - df['rank3']	
    df['WQ_47'] *= df['valid']	
    return df[['ticker','date','WQ_47']]	
def WQ_48(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_48'] = 0	
    df['WQ_48'] *= df['valid']	
    return df[['ticker','date','WQ_48']]	
def WQ_49(input_df):	
    '''(((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 *0.1))	
    ? 1 : ((-1 * 1) * (close - delay(close, 1))))'''	
    df = input_df.copy()	
    df['tmp1'] = - df.groupby('ticker')['close'].diff(10)	
    df['tmp2'] = - df.groupby('ticker')['tmp1'].diff(10) / 10	
    df['con1'] = (df['tmp2'] < -0.1).astype(int) * df['tmp2'] / df['tmp2']	
    df['WQ_49'] = df['con1'] * 1 + (1 - df['con1']) * -1 * df.groupby('ticker')['close'].diff() * df['prc_fact']	
    df['WQ_49'] *= df['valid']	
    return df[['ticker','date','WQ_49']]	
def WQ_50(input_df):	
    '''(-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))'''	
    df = input_df.copy()	
    df['rank1'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_vwap'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['corr'] = qk_corr(df,'rank1','rank2',5)	
    df['rank3'] = (df['corr'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_50'] = -1 * df.groupby('ticker')['rank3'].rolling(5,min_periods=4).max().values	
    df['WQ_50'] *= df['valid']	
    return df[['ticker','date','WQ_50']]	
def WQ_51(input_df):	
    '''(((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 * 0.05)) ?	
    1 : ((-1 * 1) * (close - delay(close, 1))))'''	
    df = input_df.copy()	
    df = input_df.copy()	
    df['tmp1'] = - df.groupby('ticker')['close'].diff(10)	
    df['tmp2'] = - df.groupby('ticker')['tmp1'].diff(10) / 10	
    df['con1'] = (df['tmp2'] < -0.05).astype(int) * df['tmp2'] / df['tmp2']	
    df['WQ_51'] = df['con1'] * 1 + (1 - df['con1']) * -1 * df.groupby('ticker')['close'].diff() * df['prc_fact']	
    df['WQ_51'] *= df['valid']	
    return df[['ticker','date','WQ_51']]	
def WQ_52(input_df):	
    '''((((-1 * ts_min(low, 5)) + delay(ts_min(low, 5), 5)) * rank(((sum(returns, 240) - 	
    sum(returns, 20)) / 220))) * ts_rank(volume, 5))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['low'].rolling(5,min_periods=4).min().values	
    df['tmp2'] = -1 * df.groupby('ticker')['tmp1'].diff(5)	
    df['return'] = df['close'] / df.groupby('ticker')['close'].shift() - 1	
    df['tmp3'] = df.groupby('ticker')['return'].rolling(240,min_periods=192).mean().values * 240	
    df['tmp4'] = df.groupby('ticker')['return'].rolling(20,min_periods=16).mean().values * 20	
    df['tmp5'] = (df['tmp3'] - df['tmp4']) / 220	
    df['rank1'] = (df['tmp5'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['ts_rank'] = qk_ts_rank(df,'volume',5)	
    df['WQ_52'] = df['tmp2'] * df['rank1'] * df['ts_rank'] * df['prc_fact']	
    df['WQ_52'] *= df['valid']	
    return df[['ticker','date','WQ_52']]	
def WQ_53(input_df):	
    '''(-1 * delta((((close - low) - (high - close)) / (close - low)), 9))'''	
    df = input_df.copy()	
    df['tmp1'] = (df['close'] * 2 - df['low'] - df['high']) / (df['close'] - df['low'])	
    df['WQ_53'] = -1 * df.groupby('ticker')['tmp1'].diff(9)	
    df['WQ_53'] *= df['valid']	
    return df[['ticker','date','WQ_53']]	
def WQ_54(input_df):	
    '''((-1 * ((low - close) * (open^5))) / ((low - high) * (close^5)))'''	
    df = input_df.copy()	
    df['WQ_54'] = -1 * ((df['low'] - df['close']) * df['open'] ** 5) / ((df['low'] - df['high']) * df['close'] ** 5)	
    df['WQ_54'] *= df['valid']	
    return df[['ticker','date','WQ_54']]	
def WQ_55(input_df):	
    '''(-1 * correlation(rank(((close - ts_min(low, 12)) / (ts_max(high, 12) - ts_min(low,12)))), rank(volume), 6))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['low'].rolling(12,min_periods=10).min().values	
    df['tmp2'] = df.groupby('ticker')['high'].rolling(12,min_periods=10).max().values	
    df['tmp3'] = (df['close'] - df['tmp1']) / (df['tmp2'] - df['tmp1'])	
    df['rank1'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_55'] = -1 * qk_corr(df,'rank1','rank2',6)	
    df['WQ_55'] *= df['valid']	
    return df[['ticker','date','WQ_55']]	
def WQ_56(input_df):	
    '''(0 - (1 * (rank((sum(returns, 10) / sum(sum(returns, 2), 3))) * rank((returns * cap)))))'''	
    df = input_df.copy()	
    ''''''	
    df['WQ_56'] = 0	
    df['WQ_56'] *= df['valid']	
    return df[['ticker','date','WQ_56']]	
def WQ_57(input_df):	
    '''(0 - (1 * ((close - vwap) / decay_linear(rank(ts_argmax(close, 30)), 2))))'''	
    df = input_df.copy()	
    df['tmp1'] = df.groupby('ticker')['close'].rolling(30,min_periods=24).apply(np.argmax,raw=True).values	
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp2'] = qk_decay_linear(df,'rank1',2)	
    df['WQ_57'] = -1 * (df['close'] - df['vwap']) / df['tmp2'] * df['prc_fact']	
    df['WQ_57'] *= df['valid']	
    return df[['ticker','date','WQ_57']]	
def WQ_58(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_58'] = 0	
    df['WQ_58'] *= df['valid']	
    return df[['ticker','date','WQ_58']]	
def WQ_59(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_59'] = 0	
    df['WQ_59'] *= df['valid']	
    return df[['ticker','date','WQ_59']]	
def WQ_60(input_df):	
    '''(0 - (1 * ((2 * scale(rank(((((close - low) - (high - close)) / (high - low)) * volume)))) - 	
    scale(rank(ts_argmax(close, 10))))))'''	
    df = input_df.copy()	
    df['tmp1'] = (df['close'] * 2 - df['low'] - df['high']) / (df['high'] - df['low']) * df['volume']	
    df['rank1'] = (df['tmp1'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['tmp2'] = df.groupby('ticker')['close'].rolling(10,min_periods=8).apply(np.argmax,raw=True).values	
    df['rank2'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_60'] = -1 * (2 * df['rank1'] - df['rank2'])	
    df['WQ_60'] *= df['valid']	
    return df[['ticker','date','WQ_60']]	
def WQ_61(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_61'] = 0	
    df['WQ_61'] *= df['valid']	
    return df[['ticker','date','WQ_61']]	
def WQ_62(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_62'] = 0	
    df['WQ_62'] *= df['valid']	
    return df[['ticker','date','WQ_62']]	
def WQ_63(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_63'] = 0	
    df['WQ_63'] *= df['valid']	
    return df[['ticker','date','WQ_63']]	
def WQ_64(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_64'] = 0	
    df['WQ_64'] *= df['valid']	
    return df[['ticker','date','WQ_64']]	
def WQ_65(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_65'] = 0	
    df['WQ_65'] *= df['valid']	
    return df[['ticker','date','WQ_65']]	
def WQ_66(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_66'] = 0	
    df['WQ_66'] *= df['valid']	
    return df[['ticker','date','WQ_66']]	
def WQ_67(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_67'] = 0	
    df['WQ_67'] *= df['valid']	
    return df[['ticker','date','WQ_67']]	
def WQ_68(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_68'] = 0	
    df['WQ_68'] *= df['valid']	
    return df[['ticker','date','WQ_68']]	
def WQ_69(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_69'] = 0	
    df['WQ_69'] *= df['valid']	
    return df[['ticker','date','WQ_69']]	
def WQ_70(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_70'] = 0	
    df['WQ_70'] *= df['valid']	
    return df[['ticker','date','WQ_70']]	
def WQ_71(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_71'] = 0	
    df['WQ_71'] *= df['valid']	
    return df[['ticker','date','WQ_71']]	
def WQ_72(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_72'] = 0	
    df['WQ_72'] *= df['valid']	
    return df[['ticker','date','WQ_72']]	
def WQ_73(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_73'] = 0	
    df['WQ_73'] *= df['valid']	
    return df[['ticker','date','WQ_73']]	
def WQ_74(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_74'] = 0	
    df['WQ_74'] *= df['valid']	
    return df[['ticker','date','WQ_74']]	
def WQ_75(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_75'] = 0	
    df['WQ_75'] *= df['valid']	
    return df[['ticker','date','WQ_75']]	
def WQ_76(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_76'] = 0	
    df['WQ_76'] *= df['valid']	
    return df[['ticker','date','WQ_76']]	
def WQ_77(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_77'] = 0	
    df['WQ_77'] *= df['valid']	
    return df[['ticker','date','WQ_77']]	
def WQ_78(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_78'] = 0	
    df['WQ_78'] *= df['valid']	
    return df[['ticker','date','WQ_78']]	
def WQ_79(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_79'] = 0	
    df['WQ_79'] *= df['valid']	
    return df[['ticker','date','WQ_79']]	
def WQ_80(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_80'] = 0	
    df['WQ_80'] *= df['valid']	
    return df[['ticker','date','WQ_80']]	
def WQ_81(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_81'] = 0	
    df['WQ_81'] *= df['valid']	
    return df[['ticker','date','WQ_81']]	
def WQ_82(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_82'] = 0	
    df['WQ_82'] *= df['valid']	
    return df[['ticker','date','WQ_82']]	
def WQ_83(input_df):	
    '''((rank(delay(((high - low) / (sum(close, 5) / 5)), 2)) * rank(rank(volume))) /	
    (((high -low) / (sum(close, 5) / 5)) / (vwap - close)))'''	
    df = input_df.copy()	
    df['tmp1'] = (df['high'] - df['low']) / df.groupby('ticker')['close'].rolling(5,min_periods=4).mean().values	
    df['tmp2'] = df.groupby('ticker')['tmp1'].shift(2)	
    df['rank1'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)	
    df['WQ_83'] = df['rank1'] * df['rank2'] / df['tmp1'] / (df['vwap'] - df['close']) /  df['prc_fact']	
    df['WQ_83'] *= df['valid']	
    return df[['ticker','date','WQ_83']]	
def WQ_84(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_84'] = 0	
    df['WQ_84'] *= df['valid']	
    return df[['ticker','date','WQ_84']]	
def WQ_85(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_85'] = 0	
    df['WQ_85'] *= df['valid']	
    return df[['ticker','date','WQ_85']]	
def WQ_86(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_86'] = 0	
    df['WQ_86'] *= df['valid']	
    return df[['ticker','date','WQ_86']]	
def WQ_87(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_87'] = 0	
    df['WQ_87'] *= df['valid']	
    return df[['ticker','date','WQ_87']]	
def WQ_88(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_88'] = 0	
    df['WQ_88'] *= df['valid']	
    return df[['ticker','date','WQ_88']]	
def WQ_89(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_89'] = 0	
    df['WQ_89'] *= df['valid']	
    return df[['ticker','date','WQ_89']]	
def WQ_90(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_90'] = 0	
    df['WQ_90'] *= df['valid']	
    return df[['ticker','date','WQ_90']]	
def WQ_91(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_91'] = 0	
    df['WQ_91'] *= df['valid']	
    return df[['ticker','date','WQ_91']]	
def WQ_92(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_92'] = 0	
    df['WQ_92'] *= df['valid']	
    return df[['ticker','date','WQ_92']]	
def WQ_93(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_93'] = 0	
    df['WQ_93'] *= df['valid']	
    return df[['ticker','date','WQ_93']]	
def WQ_94(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_94'] = 0	
    df['WQ_94'] *= df['valid']	
    return df[['ticker','date','WQ_94']]	
def WQ_95(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_95'] = 0	
    df['WQ_95'] *= df['valid']	
    return df[['ticker','date','WQ_95']]	
def WQ_96(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_96'] = 0	
    df['WQ_96'] *= df['valid']	
    return df[['ticker','date','WQ_96']]	
def WQ_97(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_97'] = 0	
    df['WQ_97'] *= df['valid']	
    return df[['ticker','date','WQ_97']]	
def WQ_98(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_98'] = 0	
    df['WQ_98'] *= df['valid']	
    return df[['ticker','date','WQ_98']]	
def WQ_99(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_99'] = 0	
    df['WQ_99'] *= df['valid']	
    return df[['ticker','date','WQ_99']]	
def WQ_100(input_df):	
    ''''''	
    df = input_df.copy()	
    ''''''	
    df['WQ_100'] = 0	
    df['WQ_100'] *= df['valid']	
    return df[['ticker','date','WQ_100']]	
def WQ_101(input_df):	
    '''((close - open) / ((high - low) + .001))'''	
    df = input_df.copy()	
    df['WQ_101'] = (df['close'] - df['open']) / (df['high'] - df['low'] + 0.001)	
    df['WQ_101'] *= df['valid']	
    return df[['ticker','date','WQ_101']]	
