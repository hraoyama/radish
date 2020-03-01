# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 15:54:54 2019

@author: zhang
"""
# import sqlite3
#
# conn = sqlite3.connect("D:/Mega Data/yueDB")
# c = conn.cursor()
from alpha.alpha_util_v4 import *
def GT_1(input_df):
    '''(-1 * CORR(RANK(DELTA(LOG(VOLUME), 1)), RANK(((CLOSE - OPEN) / OPEN)), 6)) '''
    df = input_df.copy()
    df['volume'] = df['volume'].apply(np.log)
    df['d_volume'] = df.groupby('ticker')['volume'].diff()
    df['rank1'] = (df['d_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp'] = (df['close']-df['open']) /df['open']
    df['rank2'] = (df['tmp'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_1'] = qk_corr(df,'rank1','rank2',6) * -1
    df['GT_1'] *= df['valid']
    return df[['ticker','date','GT_1']]
def GT_2(input_df):
    '''(-1 * DELTA((((CLOSE - LOW) - (HIGH - CLOSE)) / (HIGH - LOW)), 1)) '''
    df = input_df.copy()
    df['tmp'] = (df['close'] * 2 - df['high'] - df['low'])/(df['high'] - df['low'])
    df['GT_2'] = df.groupby('ticker')['tmp'].diff() * -1
    df['GT_2'] *= df['valid']
    return df[['ticker','date','GT_2']]
def GT_3(input_df):
    '''SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),6) '''
    df = input_df.copy()
    df['delay'] = df.groupby('ticker')['close'].shift().values
    df['tmp'] = ((df['close']-df[['low','delay']].min(1))*(df['close']>df['delay']).astype(int)+(df['close']-df[['high','delay']].max(1))*(df['close']<df['delay']).astype(int)).values 
    df['GT_3'] = df.groupby('ticker')['tmp'].rolling(6,min_periods=5).mean().values * 6 * df['prc_fact']
    df['GT_3'] *= df['valid']
    return df[['ticker','date','GT_3']]
def GT_4(input_df):
    '''((((SUM(CLOSE, 8) / 8) + STD(CLOSE, 8)) < (SUM(CLOSE, 2) / 2)) ? (-1 * 1) : 
        (((SUM(CLOSE, 2) / 2) < ((SUM(CLOSE, 8) / 8) - STD(CLOSE, 8))) ? 1 : 
            (((1 < (VOLUME / MEAN(VOLUME,20))) || ((VOLUME / MEAN(VOLUME,20)) == 1)) ? 1 : (-1 * 1)))) '''
    df = input_df.copy()
    df['mean1'] = df.groupby('ticker')['close'].rolling(8,min_periods=6).mean().values
    df['mean2'] = df.groupby('ticker')['close'].rolling(2).mean().values
    df['std1'] = df.groupby('ticker')['close'].rolling(8,min_periods=6).std().values
    df['mean_vol'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['tmp1'] = df['close'] * 0
    df['tmp1'] += (df['mean2']>df['mean1']+df['std1']).astype(int)*-1
    df['tmp1'] += (df['mean2']<df['mean1']-df['std1']).astype(int)
    sub_df = df[df['tmp1']==0]
    df['tmp2'] = ((sub_df['volume'] >= sub_df['mean_vol']).astype(int)*2-1)
    df['tmp2'] =df['tmp2'].fillna(0)
    df['GT_4'] = df['tmp1']+df['tmp2']
    df['GT_4'] *= df['valid']
    return df[['ticker','date','GT_4']]
def GT_5(input_df):
    '''(-1 * TSMAX(CORR(TSRANK(VOLUME, 5), TSRANK(HIGH, 5), 5), 3)) '''
    df = input_df.copy()
    n = 5
    df['rank1'] = qk_ts_rank(df,'volume',n)
    df['rank2'] = qk_ts_rank(df,'high',n)
    df['corr'] = qk_corr(df,'rank1','rank2',n)
    df['GT_5'] = df.groupby('ticker')['corr'].rolling(3).max().values * -1
    df['GT_5'] *= df['valid']
    return df[['ticker','date','GT_5']]
def GT_6(input_df):
    '''(RANK(SIGN(DELTA((((OPEN * 0.85) + (HIGH * 0.15))), 4)))* -1) '''
    df = input_df.copy()
    rho=0.85
    df['tmp'] = df['open'] * rho + df['high'] * (1-rho)
    df['sign'] = df.groupby('ticker')['tmp'].diff(4).apply(np.sign)
    df['GT_6']= (df['sign'] * df['valid']).dropna().groupby(df['date']).rank(pct=True) * -1
    df['GT_6'] *= df['valid']
    return df[['ticker','date','GT_6']]
def GT_7(input_df):
    '''((RANK(MAX((VWAP - CLOSE), 3)) + RANK(MIN((VWAP - CLOSE), 3))) * RANK(DELTA(VOLUME, 3))'''
    df = input_df.copy()
    n = 3
    df['tmp'] = df['vwap']-df['close']
    df['max_tmp'] = df.groupby('ticker')['tmp'].rolling(n).max().values
    df['min_tmp'] = df.groupby('ticker')['tmp'].rolling(n).min().values
    df['rank1'] = (df['max_tmp'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['min_tmp'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['d_volume'] = df.groupby('ticker')['volume'].diff(n)
    df['rank3'] = (df['d_volume'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_7'] = (df['rank1']+df['rank2'])*df['rank3']
    df['GT_7'] *= df['valid']
    return df[['ticker','date','GT_7']]
def GT_8(input_df):
    '''RANK(DELTA(((((HIGH + LOW) / 2) * 0.2) + (VWAP * 0.8)), 4) * -1) '''
    df = input_df.copy()
    rho=0.8
    df['tmp'] = (df['high']+df['low'])/2 * (1-rho) + df['vwap']*rho
    df['diff'] = df.groupby('ticker')['tmp'].diff(4)*-1
    df['GT_8'] = (df['diff'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_8'] *= df['valid']
    return df[['ticker','date','GT_8']]
def GT_9(input_df):
    '''SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,7,2)'''
    df = input_df.copy()
    df['tmp'] = (df['high']+df['low'])/2
    df['diff'] = df.groupby('ticker')['tmp'].diff(1)
    df['tmp1'] = df['diff'] * (df['high']-df['low'])/df['volume']
    df['GT_9'] = qk_ewma(df,'tmp1',2/7)*1000 * df['prc_fact'] ** 2 / df['vol_fact']
    df['GT_9'] *= df['valid']
    return df[['ticker','date','GT_9']]
def GT_10(input_df):
    '''(RANK(MAX(((RET < 0) ? STD(RET, 20) : CLOSE)^2),5))'''
    df = input_df.copy()
    df['log_close'] = df['close'].apply(np.log)
    df['return'] = df.groupby('ticker')['log_close'].diff()
    df['std'] =df.groupby('ticker')['return'].rolling(20,min_periods=16).std().values
    df['tmp'] = (df['std']*(df['return']<0).astype(int)+df['close']*(df['return']>=0).astype(int)) ** 2
    df['tmp1'] = df.groupby('ticker')['tmp'].rolling(5).apply(np.argmax,raw=True).values
    df['GT_10'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True) - 0.5
    df['GT_10'] *= df['valid']
    return df[['ticker','date','GT_10']]
def GT_11(input_df):
    '''SUM(((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-LOW)*VOLUME,6)'''
    df = input_df.copy()
    df['tmp'] = (df['close'] * 2 - df['high'] - df['low'])/(df['high'] - df['low'])*df['volume']
    df['GT_11'] = df.groupby('ticker')['tmp'].rolling(6,min_periods=5).mean().values * 6 * df['vol_fact']
    df['GT_11'] *= df['valid']
    return df[['ticker','date','GT_11']]
def GT_12(input_df):
    '''(RANK((OPEN - (SUM(VWAP, 10) / 10)))) * (-1 * (RANK(ABS((CLOSE - VWAP)))))'''
    df = input_df.copy()
    df['tmp1'] = df['open'] - df.groupby('ticker')['vwap'].rolling(10,min_periods=8).mean().values
    df['tmp2'] = (df['close'] - df['vwap']).abs()
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_12'] = df['rank1'] * df['rank2'] * -1
    df['GT_12'] *= df['valid']
    return df[['ticker','date','GT_12']]
def GT_13(input_df):
    '''(((HIGH * LOW)^0.5) - VWAP) '''
    df = input_df.copy()
    df['GT_13'] = ((df['high']*df['low'])**0.5 - df['vwap']) * df['prc_fact']
    df['GT_13'] *= df['valid']
    return df[['ticker','date','GT_13']]
def GT_14(input_df):
    '''CLOSE-DELAY(CLOSE,5) '''
    df = input_df.copy()
    df['GT_14'] = df.groupby('ticker')['close'].diff(5) * df['prc_fact']
    df['GT_14'] *= df['valid']
    return df[['ticker','date','GT_14']]
def GT_15(input_df):
    '''OPEN/DELAY(CLOSE,1)-1 '''
    df = input_df.copy()
    df['GT_15'] = df['open']/df.groupby('ticker')['close'].shift() -1
    df['GT_15'] *= df['valid']
    return df[['ticker','date','GT_15']]
def GT_16(input_df):
    df = input_df.copy()
    n = 5
    df['rank1'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_vwap'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr'] = qk_corr(df,'rank1','rank2',n)
    df['rank3'] = (df['corr'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_16'] = df.groupby('ticker')['rank3'].rolling(n).max().values * -1
    df['GT_16'] *= df['valid']
    return df[['ticker','date','GT_16']]
def GT_17(input_df):
    '''RANK((VWAP - MAX(VWAP, 15)))^DELTA(CLOSE, 5)'''
    df = input_df.copy()
    df['tmp1'] = df['vwap'] - df.groupby('ticker')['vwap'].rolling(15,min_periods=12).max().values
    df['tmp2'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_17'] = df['tmp2'] ** (df.groupby('ticker')['close'].diff(5) * df['prc_fact'])
    df['GT_17'] *= df['valid']
    return df[['ticker','date','GT_17']]
def GT_18(input_df):
    '''CLOSE/DELAY(CLOSE,5)'''
    df = input_df.copy()
    df['GT_18'] = df['close']/df.groupby('ticker')['close'].shift(5) -1
    df['GT_18'] *= df['valid']
    return df[['ticker','date','GT_18']]
def GT_19(input_df):
    '''(CLOSE<DELAY(CLOSE,5)?(CLOSE-DELAY(CLOSE,5))/DELAY(CLOSE,5):(CLOSE=DELAY(CLOSE,5)?0:(CLOSE-DELAY(CLOSE,5))/CLOSE)) '''
    df = input_df.copy()
    n = 5
    df['delay'] = df.groupby('ticker')['close'].shift(n)
    df['GT_19'] = (df['close']-df['delay'])/(df[['close','delay']].max(1))
    df['GT_19'] *= df['valid']
    return df[['ticker','date','GT_19']]
def GT_20(input_df):
    '''(CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*100'''
    df = input_df.copy()
    n = 6
    df['delay'] = df.groupby('ticker')['close'].shift(n)
    df['GT_20'] = (df['close']-df['delay'])/df['delay'] * 100
    df['GT_20'] *= df['valid']
    return df[['ticker','date','GT_20']]
def GT_21(input_df):
    '''REGBETA(MEAN(CLOSE,6),SEQUENCE(6)) '''
    df = input_df.copy()
    n = 6
    df['ma'] = df.groupby('ticker')['close'].rolling(n,min_periods=int(n*0.8)).mean().values
    df['GT_21'] = reg_beta(df,'ma',n) * df['prc_fact']
    df['GT_21'] *= df['valid']
    return df[['ticker','date','GT_21']]
def GT_22(input_df):
    '''SMEAN(((CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6)-DELAY((CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6),3)),12,1) '''
    df = input_df.copy()
    n = 6
    df['tmp1'] = df.groupby('ticker')['close'].rolling(n,min_periods=5).mean().values
    df['tmp2'] = (df['close'] - df['tmp1'])/df['tmp1']
    df['tmp3'] = df.groupby('ticker')['tmp2'].diff(3)
    df['GT_22'] = qk_ewma(df,'tmp3',1/12)
    df['GT_22'] *= df['valid']
    return df[['ticker','date','GT_22']]
def GT_23(input_df):
    '''SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE:20),0),20,1)/(SMA((CLOSE>DELAY(CLOSE,1)
    ?STD(CLOSE,20):0),20,1 )+SMA((CLOSE<=DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1))*100 '''
    df = input_df.copy()
    n = 20
    df['std'] = df.groupby('ticker')['close'].rolling(n,min_periods=int(n*0.8)).std().values
    df['tmp1'] = df['std'][df['close']>df.groupby('ticker')['close'].shift()]
    df['tmp1'] = df['tmp1'].fillna(0)
    df['tmp2'] = df['std'][df['close']<=df.groupby('ticker')['close'].shift()]
    df['tmp2'] = df['tmp2'].fillna(0)
    df['ema1'] = qk_ewma(df,'tmp1',1/n)
    df['ema2'] = qk_ewma(df,'tmp2',1/n)
    df['GT_23'] = df['ema1'] / (df['ema1']+df['ema2']) *100
    df['GT_23'] *= df['valid']
    return df[['ticker','date','GT_23']]
def GT_24(input_df):
    '''SMA(CLOSE-DELAY(CLOSE,5),5,1) '''
    df = input_df.copy()
    n = 5
    df['tmp'] = df.groupby('ticker')['close'].diff(n)
    df['GT_24'] = qk_ewma(df,'tmp',1/n) * df['prc_fact']
    df['GT_24'] *= df['valid']
    return df[['ticker','date','GT_24']]
def GT_25(input_df):
    '''((-1 * RANK((DELTA(CLOSE, 7) * (1 - RANK(DECAYLINEAR((VOLUME / MEAN(VOLUME,20)), 9)))))) * (1 + RANK(SUM(RET, 250))))'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['tmp1'] = df['volume'] /df['tmp']
    df['tmp2'] = qk_decay_linear(df,'tmp1',9)
    df['rank1'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['close'].diff(7)
    df['tmp4'] = df['tmp3'] * (1 - df['rank1'])
    df['rank2'] = (df['tmp4'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['ret'] = df['close'] / df.groupby('ticker')['close'].shift(250) - 1
    df['rank3'] = (df['ret'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_25'] = -1 * df['rank2'] * (1 + df['rank3'])
    df['GT_25'] *= df['valid']
    return df[['ticker','date','GT_25']]
def GT_26(input_df):
    '''((((SUM(CLOSE, 7) / 7) - CLOSE)) + ((CORR(VWAP, DELAY(CLOSE, 5), 230)))'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].rolling(7,min_periods=5).mean().values
    df['tmp1'] = df.groupby('ticker')['close'].shift(5)
    df['corr'] = qk_corr(df,'vwap','tmp1',230)
    df['GT_26'] = (df['tmp'] - df['close']) * df['prc_fact'] + df['corr']
    df['GT_26'] *= df['valid']
    return df[['ticker','date','GT_26']]
def GT_27(input_df):
    '''WMA((CLOSE-DELAY(CLOSE,3))/DELAY(CLOSE,3)*100+(CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*100,12) '''
    df = input_df.copy()
    df['tmp1'] = df['close'] / df.groupby('ticker')['close'].shift(3) - 1
    df['tmp2'] = df['close'] / df.groupby('ticker')['close'].shift(6) - 1
    df['tmp3'] = (df['tmp1']+df['tmp2'])*100
    df['GT_27'] = df.groupby('ticker')['tmp3'].rolling(12,min_periods=10).apply(wma,raw=True).values
    df['GT_27'] *= df['valid']
    return df[['ticker','date','GT_27']]
def GT_28(input_df):
    '''3*SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1)-2*SMA(SMA((CLOSE-TSMIN(LOW,9))/( MAX(HIGH,9)-TSMAX(LOW,9))*100,3,1),3,1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(9,min_periods=7).max().values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(9,min_periods=7).min().values
    df['tmp3'] = (df['close']- df['tmp2'])/(df['tmp1'] - df['tmp2']) * 100
    df['sma1'] = qk_ewma(df,'tmp3',1/3)
    df['sma2'] = qk_ewma(df,'sma1',1/3)
    df['GT_28'] = df['sma1'] * 3 - df['sma2'] * 2
    df['GT_28'] *= df['valid']
    return df[['ticker','date','GT_28']]
def GT_29(input_df):
    '''(CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*VOLUME'''
    df = input_df.copy()
    n = 6
    df['delay'] = df.groupby('ticker')['close'].shift(n)
    df['GT_29'] = (df['close'] - df['delay'])/df['delay'] * df['volume'] * df['vol_fact']
    df['GT_29'] *= df['valid']
    return df[['ticker','date','GT_29']]
def GT_30(input_df):
    '''WMA((REGRESI(CLOSE/DELAY(CLOSE)-1,MKT,SMB,HML,60))^2,20)'''
    df = input_df.copy()
    '''hold on'''
    df['GT_30'] = 0
    df['GT_30'] *= df['valid']
    return df[['ticker','date','GT_30']]
def GT_31(input_df):
    '''(CLOSE-MEAN(CLOSE,12))/MEAN(CLOSE,12)*100'''
    df = input_df.copy()
    n = 12
    df['tmp'] = df.groupby('ticker')['close'].rolling(n,min_periods=10).mean().values
    df['GT_31'] = (df['close'] - df['tmp'])/df['tmp'] * 100
    df['GT_31'] *= df['valid']
    return df[['ticker','date','GT_31']]
def GT_32(input_df):
    '''(-1 * SUM(RANK(CORR(RANK(HIGH), RANK(VOLUME), 3)), 3)) '''
    df = input_df.copy()
    n = 3
    df['rank1'] = (df['raw_high'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr'] = qk_corr(df,'rank1','rank2',n)
    df['rank3'] = (df['corr'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_32']= df.groupby('ticker')['rank3'].rolling(n).sum().values * -1
    df['GT_32'] *= df['valid']
    return df[['ticker','date','GT_32']]
def GT_33(input_df):
    '''((((-1 * TSMIN(LOW, 5)) + DELAY(TSMIN(LOW, 5), 5)) * RANK(((SUM(RET, 240) - SUM(RET, 20)) / 220))) * TSRANK(VOLUME, 5))'''
    df = input_df.copy()
    n = 5
    df['tmp1'] = df.groupby('ticker')['low'].rolling(n,min_periods=4).min().values
    df['tmp2'] = -df.groupby('ticker')['tmp1'].diff(n)
    df['tmp3'] = (df['close'] / df.groupby('ticker')['close'].shift(240) - df['close'] / df.groupby('ticker')['close'].shift(20))/220
    df['rank1'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp4'] = qk_ts_rank(df,'volume',n)
    df['GT_33'] = df['tmp2'] * df['prc_fact'] * df['rank1'] * df['tmp4']
    df['GT_33'] *= df['valid']
    return df[['ticker','date','GT_33']]
def GT_34(input_df):
    '''MEAN(CLOSE,12)/CLOSE '''
    df = input_df.copy()
    n = 12
    df['tmp'] = df.groupby('ticker')['close'].rolling(n,min_periods=10).mean().values
    df['GT_34'] = df['tmp']/df['close']
    df['GT_34'] *= df['valid']
    return df[['ticker','date','GT_34']]
def GT_35(input_df):
    '''(MIN(RANK(DECAYLINEAR(DELTA(OPEN, 1), 15)), RANK(DECAYLINEAR(CORR((VOLUME), ((OPEN * 0.65) + (OPEN *0.35)), 17),7))) * -1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['open'].diff()
    df['tmp2'] = qk_decay_linear(df,'tmp1',15)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df['open'] * 0.65 + df['close'] * 0.35
    df['corr'] = qk_corr(df,'volume','tmp3',17)
    df['tmp4'] = qk_decay_linear(df,'corr',7)
    df['rank2'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_35'] = df[['rank1','rank2']].min(1) * -1
    df['GT_35'] *= df['valid']
    return df[['ticker','date','GT_35']]
def GT_36(input_df):
    '''RANK(SUM(CORR(RANK(VOLUME), RANK(VWAP)), 6), 2) '''
    df = input_df.copy()
    df['rank1'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_vwap'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr'] = qk_corr(df,'rank1','rank2',6)
    df['tmp'] = df.groupby('ticker')['corr'].rolling(2).sum().values
    df['GT_36'] = (df['tmp'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_36'] *= df['valid']
    return df[['ticker','date','GT_36']]
def GT_37(input_df):
    '''(-1 * RANK(((SUM(OPEN, 5) * SUM(RET, 5)) - DELAY((SUM(OPEN, 5) * SUM(RET, 5)), 10))))'''
    df = input_df.copy()
    n = 5
    df['tmp1'] = df.groupby('ticker')['open'].rolling(n,min_periods=4).mean().values * 5
    df['tmp2'] = df['close'] / df.groupby('ticker')['close'].shift(n) - 1
    df['tmp3'] = df['tmp1'] * df['tmp2']
    df['tmp4'] = df.groupby('ticker')['tmp3'].diff(n*2)
    df['GT_37'] = -1 * (df['tmp4'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_37'] *= df['valid']
    return df[['ticker','date','GT_37']]
def GT_38(input_df):
    '''(((SUM(HIGH, 20) / 20) < HIGH) ? (-1 * DELTA(HIGH, 2)) : 0)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(20,min_periods=16).mean().values
    df['GT_38'] = -1 * df.groupby('ticker')['high'].diff(2) * (df['high']>df['tmp1']).astype(int) * df['prc_fact']
    df['GT_38'] *= df['valid']
    return df[['ticker','date','GT_38']]
def GT_39(input_df):
    '''((RANK(DECAYLINEAR(DELTA((CLOSE), 2),8)) - RANK(DECAYLINEAR(CORR(((VWAP * 0.3) + (OPEN * 0.7)), SUM(MEAN(VOLUME,180), 37), 14), 12))) * -1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff(2)
    df['tmp2'] = qk_decay_linear(df,'tmp1',8)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df['vwap'] * 0.3+df['open'] * 0.7
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(180,min_periods=144).mean().values
    df['tmp5'] = df.groupby('ticker')['tmp4'].rolling(37,min_periods=30).mean().values * 37
    df['corr'] = qk_corr(df,'tmp3','tmp5',14)
    df['tmp6'] = qk_decay_linear(df,'corr',12)
    df['rank2'] = (df['tmp6'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_39'] = df['rank2'] - df['rank1']
    df['GT_39'] *= df['valid']
    return df[['ticker','date','GT_39']]
def GT_40(input_df):
    '''SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:0),26)/SUM((CLOSE<=DELAY(CLOSE,1)?VOLUME:0),26)*100 '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['tmp2'] = df['volume'] * ((df['tmp1']>0).astype(int))
    df['tmp3'] = df['volume'] * ((df['tmp1']<=0).astype(int))
    df['sum1'] = df.groupby('ticker')['tmp2'].rolling(26,min_periods=21).mean().values * 26
    df['sum2'] = df.groupby('ticker')['tmp3'].rolling(26,min_periods=21).mean().values * 26
    df['GT_40'] = df['sum1']/df['sum2'] * 100
    df['GT_40'] *= df['valid']
    return df[['ticker','date','GT_40']]
def GT_41(input_df):
    '''(RANK(MAX(DELTA((VWAP), 3), 5))* -1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['vwap'].diff(3)
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(5,min_periods=4).max().values
    df['GT_41'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True) * -1
    df['GT_41'] *= df['valid']
    return df[['ticker','date','GT_41']]
def GT_42(input_df):
    '''((-1 * RANK(STD(HIGH, 10))) * CORR(HIGH, VOLUME, 10)) '''
    df = input_df.copy()
    n = 10
    df['tmp1'] = df.groupby('ticker')['high'].rolling(n,min_periods=8).std().values
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr'] = qk_corr(df,'high','volume',n)
    df['GT_42'] = - df['rank1'] * df['corr']
    df['GT_42'] *= df['valid']
    return df[['ticker','date','GT_42']]
def GT_43(input_df):
    '''SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),6) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['tmp2'] = df['volume'] * (df['tmp1'].apply(np.sign))
    df['GT_43'] = df.groupby('ticker')['tmp2'].rolling(6,min_periods=5).mean().values * 6 * df['vol_fact']
    df['GT_43'] *= df['valid']
    return df[['ticker','date','GT_43']]
def GT_44(input_df):
    '''(TSRANK(DECAYLINEAR(CORR(((LOW )), MEAN(VOLUME,10), 7), 6),4) + TSRANK(DECAYLINEAR(DELTA((VWAP), 3), 10), 15))'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(10,min_periods=8).mean().values
    df['tmp2'] = qk_corr(df,'low','tmp1',7)
    df['tmp3'] = qk_decay_linear(df,'tmp2',6)
    df['rank1'] = qk_ts_rank(df,'tmp3',4)
    df['tmp4'] = df.groupby('ticker')['vwap'].diff(3)
    df['tmp5'] = qk_decay_linear(df,'tmp4',10)
    df['rank2'] = qk_ts_rank(df,'tmp5',15)
    df['GT_44'] = df['rank1'] + df['rank2']
    df['GT_44'] *= df['valid']
    return df[['ticker','date','GT_44']]
def GT_45(input_df):
    '''(RANK(DELTA((((CLOSE * 0.6) + (OPEN *0.4))), 1)) * RANK(CORR(VWAP, MEAN(VOLUME,150), 15))) '''
    df = input_df.copy()
    df['tmp1'] = df['close'] * 0.6 + df['open'] * 0.4
    df['tmp2'] = df.groupby('ticker')['tmp1'].diff(1)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(150,min_periods=120).mean().values
    df['tmp4'] = qk_corr(df,'vwap','tmp3',15)
    df['rank2'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_45'] = df['rank1'] * df['rank2']
    df['GT_45'] *= df['valid']
    return df[['ticker','date','GT_45']]
def GT_46(input_df):
    '''(MEAN(CLOSE,3)+MEAN(CLOSE,6)+MEAN(CLOSE,12)+MEAN(CLOSE,24))/(4*CLOSE) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].rolling(3).mean().values
    df['tmp2'] = df.groupby('ticker')['close'].rolling(6,min_periods=5).mean().values
    df['tmp3'] = df.groupby('ticker')['close'].rolling(12,min_periods=10).mean().values
    df['tmp4'] = df.groupby('ticker')['close'].rolling(24,min_periods=20).mean().values
    df['GT_46'] = df[['tmp1','tmp2','tmp3','tmp4']].sum(1)/df['close']/4
    df['GT_46'] *= df['valid']
    return df[['ticker','date','GT_46']]
def GT_47(input_df):
    '''SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,9,1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(6,min_periods=5).max().values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(6,min_periods=5).min().values
    df['tmp3'] = (df['tmp1'] - df['close'])/(df['tmp1'] - df['tmp2']) * 100
    df['GT_47'] = qk_ewma(df,'tmp3',1/9)
    df['GT_47'] *= df['valid']
    return df[['ticker','date','GT_47']]
def GT_48(input_df):
    '''(-1*((RANK(((SIGN((CLOSE - DELAY(CLOSE, 1))) + SIGN((DELAY(CLOSE, 1) - DELAY(CLOSE, 2))))
    + SIGN((DELAY(CLOSE, 2) - DELAY(CLOSE, 3)))))) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['sign'] = df['tmp1'].apply(np.sign)
    df['sum1'] = df['sign'] + df.groupby('ticker')['sign'].shift() + df.groupby('ticker')['sign'].shift(2)
    df['rank'] = (df['sum1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(5,min_periods=4).mean().values * 5
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values * 20
    df['GT_48'] = - df['rank'] * df['tmp2']/df['tmp3']
    df['GT_48'] *= df['valid']
    return df[['ticker','date','GT_48']]
def GT_49(input_df):
    '''SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    /(SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    +SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HI GH,1)),ABS(LOW-DELAY(LOW,1)))),12))'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].diff()
    df['tmp2'] = df.groupby('ticker')['low'].diff()
    df['tmp3'] = df['tmp1'].abs()
    df['tmp4'] = df['tmp2'].abs()
    df['tmp5'] = df[['tmp3','tmp4']].max(1)
    df['tmp6'] = df['tmp5'] * ((df['tmp1']+df['tmp2'])<0).astype(int)
    df['tmp7'] = df['tmp5'] * ((df['tmp1']+df['tmp2'])>0).astype(int)
    df['tmp8'] = df.groupby('ticker')['tmp6'].rolling(12,min_periods=10).mean().values * 12
    df['tmp9'] = df.groupby('ticker')['tmp7'].rolling(12,min_periods=10).mean().values * 12
    df['GT_49'] = df['tmp8']/(df['tmp8']+df['tmp9'])
    df['GT_49'] *= df['valid']
    return df[['ticker','date','GT_49']]
def GT_50(input_df):
    '''SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    /(SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    +SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12))
    -SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HI GH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    /(SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    +SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELA Y(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12))''' 
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].diff()
    df['tmp2'] = df.groupby('ticker')['low'].diff()
    df['tmp3'] = df['tmp1'].abs()
    df['tmp4'] = df['tmp2'].abs()
    df['tmp5'] = df[['tmp3','tmp4']].max(1)
    df['tmp6'] = df['tmp5'] * ((df['tmp1']+df['tmp2'])<0).astype(int)
    df['tmp7'] = df['tmp5'] * ((df['tmp1']+df['tmp2'])>0).astype(int)
    df['tmp8'] = df.groupby('ticker')['tmp6'].rolling(12,min_periods=10).mean().values * 12
    df['tmp9'] = df.groupby('ticker')['tmp7'].rolling(12,min_periods=10).mean().values * 12
    df['GT_50'] = (df['tmp9']-df['tmp8'])/(df['tmp8']+df['tmp9'])
    df['GT_50'] *= df['valid']
    return df[['ticker','date','GT_50']]
def GT_51(input_df):
    '''SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    /(SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    +SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HI GH,1)),ABS(LOW-DELAY(LOW,1)))),12))''' 
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].diff()
    df['tmp2'] = df.groupby('ticker')['low'].diff()
    df['tmp3'] = df['tmp1'].abs()
    df['tmp4'] = df['tmp2'].abs()
    df['tmp5'] = df[['tmp3','tmp4']].max(1)
    df['tmp6'] = df['tmp5'] * ((df['tmp1']+df['tmp2'])<0).astype(int)
    df['tmp7'] = df['tmp5'] * ((df['tmp1']+df['tmp2'])>0).astype(int)
    df['tmp8'] = df.groupby('ticker')['tmp6'].rolling(12,min_periods=10).mean().values * 12
    df['tmp9'] = df.groupby('ticker')['tmp7'].rolling(12,min_periods=10).mean().values * 12
    df['GT_51'] = df['tmp9']/(df['tmp8']+df['tmp9'])
    df['GT_51'] *= df['valid']
    return df[['ticker','date','GT_51']]
def GT_52(input_df):
    '''SUM(MAX(0,HIGH-DELAY((HIGH+LOW+CLOSE)/3,1)),26)/SUM(MAX(0,DELAY((HIGH+LOW+CLOSE)/3,1)-L),26)* 100'''
    df = input_df.copy()
    df['tmp1'] = df[['high','low','close']].mean(1).groupby(df['ticker']).shift()
    df['tmp2'] = (df['high'] - df['tmp1']).apply(max,args=(0,))
    df['tmp3'] = (df['tmp1'] - df['low']).apply(max,args=(0,))
    df['tmp4'] = df.groupby('ticker')['tmp2'].rolling(26,min_periods=21).mean().values * 26
    df['tmp5'] = df.groupby('ticker')['tmp3'].rolling(26,min_periods=21).mean().values * 26
    df['GT_52'] = df['tmp4']/df['tmp5'] * 100
    df['GT_52'] *= df['valid']
    return df[['ticker','date','GT_52']]
def GT_53(input_df):
    '''COUNT(CLOSE>DELAY(CLOSE,1),12)/12*100 '''
    df = input_df.copy()
    df['tmp1'] = (df['close'] > df.groupby('ticker')['close'].shift()).astype(int)
    df['GT_53'] = df.groupby('ticker')['tmp1'].rolling(12,min_periods=10).mean().values * 100
    df['GT_53'] *= df['valid']
    return df[['ticker','date','GT_53']]
def GT_54(input_df):
    '''(-1 * RANK((STD(ABS(CLOSE - OPEN)) + (CLOSE - OPEN)) + CORR(CLOSE, OPEN,10))) '''
    df = input_df.copy()
    df['tmp1'] = (df['close'] - df['open']).abs()
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(10,min_periods=8).std().values
    df['tmp3'] = qk_corr(df,'close','open',10)
    df['tmp4'] = (df['tmp2'] + df['close'] - df['open']) * df['prc_fact'] + df['tmp3']
    df['rank1'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_54'] = -df['rank1']
    df['GT_54'] *= df['valid']
    return df[['ticker','date','GT_54']]
def GT_55(input_df):
    '''SUM(16*(CLOSE-DELAY(CLOSE,1)+(CLOSE-OPEN)/2+DELAY(CLOSE,1)-DELAY(OPEN,1))/
    ((ABS(HIGH-DELAY(CLOSE,1))>ABS(LOW-DELAY(CLOSE,1)) & ABS(HIGH-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1))?
    ABS(HIGH-DELAY(CLOSE,1))+ABS(LOW-DELAY(CLOS E,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
    (ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) & ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(CLOSE,1))?
    ABS(LOW-DELAY(CLOSE,1))+ABS(HIGH-DELAY(CLO SE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
    ABS(HIGH-DELAY(LOW,1))+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4)))
    *MAX(ABS(HIGH-DELAY(CLOSE,1)),ABS(LOW-DELAY(CLOSE,1))),20) '''
    df = input_df.copy()
    df['tmp1'] = (df['high'] - df.groupby('ticker')['close'].shift()).abs()
    df['tmp2'] = (df['low'] - df.groupby('ticker')['close'].shift()).abs()
    df['tmp3'] = (df['high'] - df.groupby('ticker')['low'].shift()).abs()
    df['tmp4'] = (df['close'] - df['open']).abs().groupby(df['ticker']).shift()
    df['cond1'] = ((df['tmp1'] > df['tmp2']) & (df['tmp1'] > df['tmp3'])).astype(int)
    df['op1'] = df['tmp1'] +df['tmp2']/2 + df['tmp4']/4
    df['cond2'] = ((df['tmp2'] > df['tmp3']) & (df['tmp2'] > df['tmp1'])).astype(int)
    df['op2'] = df['tmp2'] +df['tmp1']/2 + df['tmp4']/4
    df['cond3'] = 1-(df['cond1']+df['cond2'])
    df['op3'] = df['tmp3'] + df['tmp4']/4
    df['tmp5'] = df['cond1'] * df['op1'] + df['cond2'] * df['op2'] + df['cond3'] * df['op3']
    df['tmp6'] = df[['tmp1','tmp2']].max(1) * 16 * (df['close'] + (df['close'] - df['open']) / 2 - df.groupby('ticker')['open'].shift()) / df['tmp5']
    df['GT_55'] = df.groupby('ticker')['tmp6'].rolling(20,min_periods=16).mean().values * 20
    df['GT_55'] *= df['valid']
    return df[['ticker','date','GT_55']]
def GT_56(input_df):
    '''(RANK((OPEN - TSMIN(OPEN, 12))) < RANK((RANK(CORR(SUM(((HIGH + LOW) / 2), 19), SUM(MEAN(VOLUME,40), 19), 13))^5)))'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['open'].rolling(12).min().values
    df['tmp2'] = df['open'] - df['tmp1']
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df[['high','low']].mean(1).groupby(df['ticker']).rolling(19,min_periods=16).mean().values * 19
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(40,min_periods=32).mean().values
    df['tmp5'] = df.groupby('ticker')['tmp4'].rolling(19,min_periods=16).mean().values * 19
    df['tmp6'] = qk_corr(df, 'tmp3','tmp5',13)
    df['rank2'] = (df['tmp6'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['rank2'] ** 5 * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_56'] = (df['rank1']<df['rank3']).astype(int)
    df['GT_56'] *= df['valid']
    return df[['ticker','date','GT_56']]
def GT_57(input_df):
    '''SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(9,min_periods=7).max().values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(9,min_periods=7).min().values
    df['tmp3'] = (df['close'] - df['tmp2']) / (df['tmp1'] - df['tmp2']) * 100
    df['GT_57'] = qk_ewma(df,'tmp3',1/3) 
    df['GT_57'] *= df['valid']
    return df[['ticker','date','GT_57']]
def GT_58(input_df):
    '''COUNT(CLOSE>DELAY(CLOSE,1),20)/20*100 '''
    df = input_df.copy()
    df['tmp1'] = (df['close'] > df.groupby('ticker')['close'].shift()).astype(int)
    df['GT_58'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values * 100
    df['GT_58'] *= df['valid']
    return df[['ticker','date','GT_58']]
def GT_59(input_df):
    '''SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),20)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].shift()
    df['cond1'] = (df['close'] == df['tmp1']).astype(int)
    df['op1'] = 0
    df['cond2'] = (df['close'] > df['tmp1']).astype(int)
    df['op2'] = df['close'] - df[['low','tmp1']].min(1)
    df['cond3'] = (df['close'] < df['tmp1']).astype(int)
    df['op3'] = df['close'] - df[['high','tmp1']].max(1)
    df['tmp2'] = df['cond1'] * df['op1'] + df['cond2'] * df['op2'] + df['cond3'] * df['op3']
    df['GT_59'] = df.groupby('ticker')['tmp2'].rolling(20,min_periods=16).mean().values * 20 * df['prc_fact']
    df['GT_59'] *= df['valid']
    return df[['ticker','date','GT_59']]
def GT_60(input_df):
    '''SUM(((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-LOW)*VOLUME,20)'''
    df = input_df.copy()
    df['tmp'] = (df['close'] * 2 - df['high'] - df['low'])/(df['high'] - df['low']) * df['volume'] 
    df['GT_60'] = df.groupby('ticker')['tmp'].rolling(20,min_periods=16).mean().values * 20 * df['vol_fact']
    df['GT_60'] *= df['valid']
    return df[['ticker','date','GT_60']]
def GT_61(input_df):
    '''(MAX(RANK(DECAYLINEAR(DELTA(VWAP, 1), 12)), RANK(DECAYLINEAR(RANK(CORR((LOW),MEAN(VOLUME,80), 8)), 17))) * -1)'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['vwap'].diff()
    df['tmp1'] = qk_decay_linear(df,'tmp',12)
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(80,min_periods=64).mean().values
    df['tmp3'] = qk_corr(df,'tmp2','low',8)
    df['rank2'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp4'] = qk_decay_linear(df,'rank2',17)
    df['rank3'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_61'] = - df[['rank1','rank3']].max(1)
    df['GT_61'] *= df['valid']
    return df[['ticker','date','GT_61']]
def GT_62(input_df):
    '''(-1 * CORR(HIGH, RANK(VOLUME), 5))'''
    df = input_df.copy()
    df['rank1'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_62'] = - qk_corr(df,'rank1','high',5)
    df['GT_62'] *= df['valid']
    return df[['ticker','date','GT_62']]
def GT_63(input_df):
    '''SMA(MAX(CLOSE-DELAY(CLOSE,1),0),6,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),6,1)*100'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].diff()
    df['tmp1'] = df['tmp'].apply(max,args=(0,))
    df['tmp2'] = df['tmp'].abs()
    df['tmp3'] = qk_ewma(df,'tmp1',1/6)
    df['tmp4'] = qk_ewma(df,'tmp2',1/6)
    df['GT_63'] = df['tmp3'] / df['tmp4'] * 100
    df['GT_63'] *= df['valid']
    return df[['ticker','date','GT_63']]
def GT_64(input_df):
    '''(MAX(RANK(DECAYLINEAR(CORR(RANK(VWAP), RANK(VOLUME), 4), 4)), RANK(DECAYLINEAR(MAX(CORR(RANK(CLOSE), RANK(MEAN(VOLUME,60)), 4), 13), 14))) * -1)'''
    df = input_df.copy()
    df['rank1'] = (df['raw_vwap'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr1'] = qk_corr(df,'rank1','rank2',4)
    df['tmp1'] = qk_decay_linear(df,'corr1',4)
    df['rank3'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(60,min_periods=48).mean().values
    df['rank4'] = (df['raw_close'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank5'] = (df['tmp2'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'rank4','rank5',4)
    df['tmp3'] = df.groupby('ticker')['corr2'].rolling(13,min_periods=11).max().values
    df['tmp4'] = qk_decay_linear(df,'tmp3',14)
    df['rank6'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_64'] = - df[['rank3','rank6']].max(1)
    df['GT_64'] *= df['valid']
    return df[['ticker','date','GT_64']]
def GT_65(input_df):
    '''MEAN(CLOSE,6)/CLOSE'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].rolling(6,min_periods=5).mean().values
    df['GT_65'] = df['tmp'] / df['close']
    df['GT_65'] *= df['valid']
    return df[['ticker','date','GT_65']]
def GT_66(input_df):
    '''(CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6)*100 '''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].rolling(6,min_periods=5).mean().values
    df['GT_66'] =  df['close'] / df['tmp'] - 1
    df['GT_66'] *= df['valid']
    return df[['ticker','date','GT_66']]
def GT_67(input_df):
    '''SMA(MAX(CLOSE-DELAY(CLOSE,1),0),24,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),24,1)*100'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].diff()
    df['tmp1'] = df['tmp'].apply(max,args=(0,))
    df['tmp2'] = df['tmp'].abs()
    df['tmp3'] = qk_ewma(df,'tmp1',1/24)
    df['tmp4'] = qk_ewma(df,'tmp2',1/24)
    df['GT_67'] = df['tmp3'] / df['tmp4'] * 100
    df['GT_67'] *= df['valid']
    return df[['ticker','date','GT_67']]
def GT_68(input_df):
    '''SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,15,2) '''
    df = input_df.copy()
    df['tmp'] = df[['high','low']].mean(1).groupby(df['ticker']).diff()
    df['tmp1'] =  df['tmp'] * (df['high'] - df['low']) / df['volume']
    df['GT_68'] = qk_ewma(df,'tmp1',2/15) * df['prc_fact'] ** 2 / df['vol_fact']
    df['GT_68'] *= df['valid']
    return df[['ticker','date','GT_68']]
def GT_69(input_df):
    '''(SUM(DTM,20)>SUM(DBM,20)?(SUM(DTM,20)-SUM(DBM,20))/SUM(DTM,20):
        (SUM(DTM,20)=SUM(DBM,20)?0:(SUM(DTM,20)-SUM(DBM,20))/SUM(DBM,20))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['open'].diff()
    df['tmp2'] =  df['high'] - df['open']
    df['tmp3'] =  df['open'] - df['low']
    df['dtm'] = (df['tmp1']>0).astype(int) * df[['tmp1','tmp2']].max(1)
    df['dbm'] = (df['tmp1']<0).astype(int) * df[['tmp1','tmp3']].max(1)
    df['sum1'] = df.groupby('ticker')['dtm'].rolling(20,min_periods=16).mean().values * 20
    df['sum2'] = df.groupby('ticker')['dbm'].rolling(20,min_periods=16).mean().values * 20
    df['GT_69'] = (df['sum1'] - df['sum2']) / df[['sum1','sum2']].max(1)
    df['GT_69'] *= df['valid']
    return df[['ticker','date','GT_69']]
def GT_70(input_df):
    '''STD(AMOUNT,6) '''
    df = input_df.copy()
    df['GT_70'] = df.groupby('ticker')['amount'].rolling(6,min_periods=5).std().values
    df['GT_70'] *= df['valid']
    return df[['ticker','date','GT_70']]
def GT_71(input_df):
    '''(CLOSE-MEAN(CLOSE,24))/MEAN(CLOSE,24)*100 '''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].rolling(24,min_periods=20).mean().values
    df['GT_71'] =  df['close'] / df['tmp'] - 1
    df['GT_71'] *= df['valid']
    return df[['ticker','date','GT_71']]
def GT_72(input_df):
    '''SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,15,1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(6,min_periods=5).max().values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(6,min_periods=5).min().values
    df['tmp3'] = (df['tmp1'] -df['close']) / (df['tmp1']-df['tmp2']) * 100
    df['GT_72'] = qk_ewma(df,'tmp3',1/15)
    df['GT_72'] *= df['valid']
    return df[['ticker','date','GT_72']]
def GT_73(input_df):
    '''((TSRANK(DECAYLINEAR(DECAYLINEAR(CORR((CLOSE), VOLUME, 10), 16), 4), 5) - RANK(DECAYLINEAR(CORR(VWAP, MEAN(VOLUME,30), 4),3))) * -1)'''
    df = input_df.copy()
    df['corr1'] = qk_corr(df,'close','volume',10)
    df['tmp1'] = qk_decay_linear(df,'corr1',16)
    df['tmp2'] = qk_decay_linear(df,'tmp1',4)
    df['tmp3'] = qk_ts_rank(df,'tmp2',5)
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(30,min_periods=24).mean().values
    df['corr2'] = qk_corr(df,'vwap','tmp4',4)
    df['tmp5'] = qk_decay_linear(df,'corr2',3)
    df['rank1'] = (df['tmp5'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_73'] = df['rank1'] - df['tmp3']
    df['GT_73'] *= df['valid']
    return df[['ticker','date','GT_73']]
def GT_74(input_df):
    '''(RANK(CORR(SUM(((LOW * 0.35) + (VWAP * 0.65)), 20), SUM(MEAN(VOLUME,40), 20), 7)) + RANK(CORR(RANK(VWAP), RANK(VOLUME), 6)))'''
    df = input_df.copy()
    df['tmp1'] = df['low'] * 0.35 + df['vwap'] * 0.65
    df['sum1'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values * 20
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(40,min_periods=32).mean().values
    df['sum2'] = df.groupby('ticker')['tmp2'].rolling(20,min_periods=16).mean().values * 20
    df['corr1'] = qk_corr(df,'sum1','sum2',7)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['vwap'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['volume'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'rank2','rank3',6)
    df['rank4'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_74'] = df['rank1'] + df['rank4']
    df['GT_74'] *= df['valid']
    return df[['ticker','date','GT_74']]
def GT_75(input_df):
    '''COUNT(CLOSE>OPEN & BANCHMARKINDEXCLOSE<BANCHMARKINDEXOPEN,50)/COUNT(BANCHMARKINDEXCLOSE<BANCHMARKINDEXOPEN,50)'''
    df = input_df.copy()
    n = 50
    min_date = df['date'].min()
    bm_df = pd.read_sql("select date, open as bm_open, close as bm_close from daily_index where ticker='sh000300' and date >= '%s'"%min_date,conn)
    df = df.merge(bm_df,on='date').sort_values(['ticker','date']).reset_index()
    df['tmp1'] = ((df['close'] > df['open']) & (df['bm_close'] < df['bm_open'])).astype(int)
    df['tmp2'] = (df['bm_close'] < df['bm_open']).astype(int)
    df['count1'] = df.groupby('ticker')['tmp1'].rolling(n,min_periods=40).mean().values * n
    df['count2'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=40).mean().values * n
    df['GT_75'] = df['count1'] / df['count2']
    df['GT_75'] *= df['valid']
    return df[['ticker','date','GT_75']]
def GT_76(input_df):
    '''STD(ABS((CLOSE/DELAY(CLOSE,1)-1))/VOLUME,20)/MEAN(ABS((CLOSE/DELAY(CLOSE,1)-1))/VOLUME,20)'''
    df = input_df.copy()
    df['tmp1'] = (df['close'] / df.groupby('ticker')['close'].shift() - 1).abs() / df['volume']
    df['std1'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).std().values
    df['mean1'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values
    df['GT_76'] = df['std1'] / df['mean1']
    df['GT_76'] *= df['valid']
    return df[['ticker','date','GT_76']]
def GT_77(input_df):
    '''MIN(RANK(DECAYLINEAR(((((HIGH + LOW) / 2) + HIGH) - (VWAP + HIGH)), 20)), RANK(DECAYLINEAR(CORR(((HIGH + LOW) / 2), MEAN(VOLUME,40), 3), 6)))'''
    df = input_df.copy()
    df['tmp'] = df[['high','low']].mean(1)
    df['tmp1'] = df['tmp']- df['vwap']
    df['tmp2'] = qk_decay_linear(df,'tmp1',20)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(40,min_periods=32).mean().values
    df['corr1'] = qk_corr(df,'tmp','tmp3',3)
    df['tmp4'] = qk_decay_linear(df,'corr1',6)
    df['rank2'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_77'] = df[['rank1','rank2']].min(1)
    df['GT_77'] *= df['valid']
    return df[['ticker','date','GT_77']]
def GT_78(input_df):
    '''((HIGH+LOW+CLOSE)/3-MA((HIGH+LOW+CLOSE)/3,12))/(0.015*MEAN(ABS(CLOSE-MEAN((HIGH+LOW+CLOSE)/3,12)),12)) '''
    df = input_df.copy()
    df['tmp1'] = df[['high','low','close']].mean(1)
    df['ma1'] = df.groupby('ticker')['tmp1'].rolling(12,min_periods=10).mean().values
    df['tmp2'] = (df['close'] - df['ma1']).abs()
    df['ma2'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).mean().values
    df['GT_78'] = (df['tmp1'] - df['ma1']) / (0.015 * df['ma2'])
    df['GT_78'] *= df['valid']
    return df[['ticker','date','GT_78']]
def GT_79(input_df):
    '''SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100'''
    df = input_df.copy()
    df['tmp'] = df.groupby('ticker')['close'].diff()
    df['tmp1'] = df['tmp'].apply(max,args=(0,))
    df['tmp2'] = df['tmp'].abs()
    df['tmp3'] = qk_ewma(df,'tmp1',1/12)
    df['tmp4'] = qk_ewma(df,'tmp2',1/12)
    df['GT_79'] = df['tmp3'] / df['tmp4'] * 100
    df['GT_79'] *= df['valid']
    return df[['ticker','date','GT_79']]
def GT_80(input_df):
    '''(VOLUME-DELAY(VOLUME,5))/DELAY(VOLUME,5)*100 '''
    df = input_df.copy()
    df['tmp'] = df['volume'] / df.groupby('ticker')['volume'].shift(5) - 1
    df['GT_80'] = df['tmp'] * 100
    df['GT_80'] *= df['valid']
    return df[['ticker','date','GT_80']]
def GT_81(input_df):
    '''SMA(VOLUME,21,2) '''
    df = input_df.copy()
    df['GT_81'] = qk_ewma(df,'volume',2/21) * df['vol_fact']
    df['GT_81'] *= df['valid']
    return df[['ticker','date','GT_81']]
def GT_82(input_df):
    '''SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,20,1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(6,min_periods=5).max().values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(6,min_periods=5).min().values
    df['tmp3'] = (df['tmp1'] - df['close']) / (df['tmp1'] - df['tmp2']) * 100
    df['GT_82'] = qk_ewma(df,'tmp3',1/20)
    df['GT_82'] *= df['valid']
    return df[['ticker','date','GT_82']]
def GT_83(input_df):
    '''(-1 * RANK(COVIANCE(RANK(HIGH), RANK(VOLUME), 5))) '''
    df = input_df.copy()
    df['rank1'] = (df['high'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['cov1'] = qk_cov(df,'rank1','rank2',5)
    df['GT_83'] = -(df['cov1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_83'] *= df['valid']
    return df[['ticker','date','GT_83']]
def GT_84(input_df):
    '''SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),20) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff().apply(np.sign) * df['volume']
    df['GT_84'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values * 20 * df['vol_fact']
    df['GT_84'] *= df['valid']
    return df[['ticker','date','GT_84']]
def GT_85(input_df):
    '''(TSRANK((VOLUME / MEAN(VOLUME,20)), 20) * TSRANK((-1 * DELTA(CLOSE, 7)), 8))'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['tmp2'] = df['volume'] / df['tmp1']
    df['tmp3'] = qk_ts_rank(df,'tmp2',20)
    df['tmp4'] = -df.groupby('ticker')['close'].diff(7)
    df['tmp5'] = qk_ts_rank(df,'tmp4',8)
    df['GT_85'] = df['tmp3'] * df['tmp5']
    df['GT_85'] *= df['valid']
    return df[['ticker','date','GT_85']]
def GT_86(input_df):
    '''((0.25 < (((DELAY(CLOSE, 20) - DELAY(CLOSE, 10)) / 10) - ((DELAY(CLOSE, 10) - CLOSE) / 10))) ?
    (-1 * 1) : (((((DELAY(CLOSE, 20) - DELAY(CLOSE, 10)) / 10) - ((DELAY(CLOSE, 10) - CLOSE) / 10)) < 0) ?
    1 : ((-1 * 1) * (CLOSE - DELAY(CLOSE, 1))))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff(10)
    df['tmp2'] = df.groupby('ticker')['tmp1'].diff(10) / 10 * df['prc_fact']
    df['tmp3'] = -df.groupby('ticker')['close'].diff()
    df.loc[df['tmp2'] > 0.25,'tmp3'] = -1
    df.loc[df['tmp2'] < 0,'tmp3'] = 1
    df['GT_86'] = df['tmp3']
    df['GT_86'] *= df['valid']
    return df[['ticker','date','GT_86']]
def GT_87(input_df):
    '''((RANK(DECAYLINEAR(DELTA(VWAP, 4), 7)) + TSRANK(DECAYLINEAR(((((LOW * 0.9) + (LOW * 0.1)) - VWAP) 
    / (OPEN - ((HIGH + LOW) / 2))), 11), 7)) * -1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['vwap'].diff(4)
    df['tmp2'] = qk_decay_linear(df,'tmp1',7)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = (df['high'] * 0.9 + df['low'] * 0.1 - df['vwap']) / (df['open'] - df[['high','low']].mean(1))
    df['tmp4'] = qk_decay_linear(df,'tmp3',11)
    df['rank2'] = qk_ts_rank(df,'tmp4',7)
    df['GT_87'] = -(df['rank1'] + df['rank2'])
    df['GT_87'] *= df['valid']
    return df[['ticker','date','GT_87']]
def GT_88(input_df):
    '''(CLOSE-DELAY(CLOSE,20))/DELAY(CLOSE,20)*100 '''
    df = input_df.copy()
    df['GT_88'] = (df['close'] / df.groupby('ticker')['close'].shift(20) -1) * 100
    df['GT_88'] *= df['valid']
    return df[['ticker','date','GT_88']]
def GT_89(input_df):
    '''2*(SMA(CLOSE,13,2)-SMA(CLOSE,27,2)-SMA(SMA(CLOSE,13,2)-SMA(CLOSE,27,2),10,2)) '''
    df = input_df.copy()
    df['tmp1'] = qk_ewma(df,'close',2/13).values
    df['tmp2'] = qk_ewma(df,'close',2/27).values
    df['tmp3'] = (df['tmp1'] - df['tmp2'])
    df['tmp4'] = qk_ewma(df,'tmp3',2/10).values
    df['GT_89'] = (df['tmp1'] - df['tmp2'] - df['tmp4']) * df['prc_fact']
    df['GT_89'] *= df['valid']
    return df[['ticker','date','GT_89']]
def GT_90(input_df):
    '''( RANK(CORR(RANK(VWAP), RANK(VOLUME), 5)) * -1) '''
    df = input_df.copy()
    df['rank1'] = (df['raw_vwap'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr1'] = qk_corr(df,'rank1','rank2',5)
    df['GT_90'] = - (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_90'] *= df['valid']
    return df[['ticker','date','GT_90']]
def GT_91(input_df):
    '''((RANK((CLOSE - MAX(CLOSE, 5)))*RANK(CORR((MEAN(VOLUME,40)), LOW, 5))) * -1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].rolling(5,min_periods=4).max().values
    df['tmp2'] = df['close'] - df['tmp1']
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(40,min_periods=32).mean().values
    df['corr1'] = qk_corr(df,'tmp3','low',5)
    df['rank2'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_91'] = - df['rank1'] * df['rank2']
    df['GT_91'] *= df['valid']
    return df[['ticker','date','GT_91']]
def GT_92(input_df):
    '''(MAX(RANK(DECAYLINEAR(DELTA(((CLOSE * 0.35) + (VWAP *0.65)), 2), 3)), 
    TSRANK(DECAYLINEAR(ABS(CORR((MEAN(VOLUME,180)), CLOSE, 13)), 5), 15)) * -1) '''
    df = input_df.copy()
    df['tmp1'] = (df['close'] * 0.35 + df['vwap'] * 0.65).groupby(df['ticker']).diff(2)
    df['tmp2'] = qk_decay_linear(df,'tmp1',3)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(180,min_periods=144).mean().values
    df['corr1'] = qk_corr(df,'tmp3','close',13).abs()
    df['tmp4'] = qk_decay_linear(df,'corr1',5)
    df['rank2'] = qk_ts_rank(df,'tmp4',15)
    df['GT_92'] = -df[['rank1','rank2']].max(1)
    df['GT_92'] *= df['valid']
    return df[['ticker','date','GT_92']]
def GT_93(input_df):
    '''SUM((OPEN>=DELAY(OPEN,1)?0:MAX((OPEN-LOW),(OPEN-DELAY(OPEN,1)))),20)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['open'].diff()
    df['tmp2'] = df['open'] - df['low']
    df['tmp3'] = df[['tmp1','tmp2']].max(1) * (df['tmp1']<0).astype(int)
    df['GT_93'] = df.groupby('ticker')['tmp3'].rolling(20,min_periods=16).mean().values * 20 * df['prc_fact']
    df['GT_93'] *= df['valid']
    return df[['ticker','date','GT_93']]
def GT_94(input_df):
    '''SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),30) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['tmp2'] = df['volume'] * df['tmp1'].apply(np.sign)
    df['GT_94'] = df.groupby('ticker')['tmp2'].rolling(30,min_periods=24).mean().values * 20 * df['vol_fact']
    df['GT_94'] *= df['valid']
    return df[['ticker','date','GT_94']]
def GT_95(input_df):
    '''STD(AMOUNT,20) '''
    df = input_df.copy()
    df['GT_95'] = df.groupby('ticker')['amount'].rolling(20,min_periods=16).std().values
    df['GT_95'] *= df['valid']
    return df[['ticker','date','GT_95']]
def GT_96(input_df):
    '''SMA(SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1),3,1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['low'].rolling(9,min_periods=8).min().values
    df['tmp2'] = df.groupby('ticker')['high'].rolling(9,min_periods=8).max().values
    df['tmp3'] = (df['close'] - df['tmp1']) / (df['tmp2'] - df['tmp1']) * 100
    df['tmp4'] = qk_ewma(df,'tmp3',1/3).values
    df['GT_96'] = qk_ewma(df,'tmp4',1/3).values
    df['GT_96'] *= df['valid']
    return df[['ticker','date','GT_96']]
def GT_97(input_df):
    '''STD(VOLUME,10) '''
    df = input_df.copy()
    df['GT_97'] = df.groupby('ticker')['volume'].rolling(10,min_periods=8).std().values * df['vol_fact']
    df['GT_97'] *= df['valid']
    return df[['ticker','date','GT_97']]
def GT_98(input_df):
    '''((((DELTA((SUM(CLOSE, 100) / 100), 100) / DELAY(CLOSE, 100)) < 0.05) || 
    ((DELTA((SUM(CLOSE, 100) / 100), 100) / DELAY(CLOSE, 100)) == 0.05)) ? 
        (-1 * (CLOSE - TSMIN(CLOSE, 100))) : (-1 * DELTA(CLOSE, 3))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].rolling(100,min_periods=80).mean().values
    df['tmp2'] = df.groupby('ticker')['tmp1'].diff(100)
    df['tmp3'] = df.groupby('ticker')['close'].shift(100)
    df['con1'] = df['tmp2'] / df['tmp3']
    df['tmp4'] = df.groupby('ticker')['close'].rolling(100,min_periods=80).min().values
    df['tmp5'] = df['tmp4'] - df['close']
    df.loc[df['con1'] > 0.05,'tmp5'] = -1 * df.groupby('ticker')['close'].diff(3)
    df['GT_98'] = df['tmp5'] * df['prc_fact']
    df['GT_98'] *= df['valid']
    return df[['ticker','date','GT_98']]
def GT_99(input_df):
    '''(-1 * RANK(COVIANCE(RANK(CLOSE), RANK(VOLUME), 5))) '''
    df = input_df.copy()
    df['rank1'] = (df['raw_close'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['cov1'] = qk_cov(df,'rank1','rank2',5)
    df['GT_99'] = -(df['cov1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_99'] *= df['valid']
    return df[['ticker','date','GT_99']]
def GT_100(input_df):
    '''STD(VOLUME,20) '''
    df = input_df.copy()
    df['GT_100'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).std().values * df['vol_fact']
    df['GT_100'] *= df['valid']
    return df[['ticker','date','GT_100']]
def GT_101(input_df):
    '''((RANK(CORR(CLOSE, SUM(MEAN(VOLUME,30), 37), 15)) < RANK(CORR(RANK(((HIGH * 0.1) + (VWAP * 0.9))), RANK(VOLUME), 11))) * -1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(30,min_periods=24).mean().values
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(37,min_periods=30).mean().values * 37
    df['corr1'] = qk_corr(df,'tmp1','tmp2',15)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = 0.1 * df['high'] + 0.9 * df['vwap']
    df['rank2'] = (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['volume'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'rank2','rank3',11)
    df['rank4'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_101'] = -1 * (df['rank1'] < df['rank4']).astype(int)
    df['GT_101'] *= df['valid']
    return df[['ticker','date','GT_101']]
def GT_102(input_df):
    '''SMA(MAX(VOLUME-DELAY(VOLUME,1),0),6,1)/SMA(ABS(VOLUME-DELAY(VOLUME,1)),6,1)*100'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].diff()
    df['tmp2'] = df['tmp1'].apply(max,args=(0,))
    df['tmp3'] = df['tmp1'].abs()
    df['sma1'] = qk_ewma(df,'tmp2',1/6).values
    df['sma2'] = qk_ewma(df,'tmp3',1/6).values
    df['GT_102'] = df['sma1'] / df['sma2'] * 100
    df['GT_102'] *= df['valid']
    return df[['ticker','date','GT_102']]
def GT_103(input_df):
    '''((20-LOWDAY(LOW,20))/20)*100 '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df.groupby('ticker')['low'].rolling(n,min_periods=16).apply(np.argmin,raw=True).values
    df['GT_103'] = df['tmp1'] / n * 100
    df['GT_103'] *= df['valid']
    return df[['ticker','date','GT_103']]
def GT_104(input_df):
    '''(-1 * (DELTA(CORR(HIGH, VOLUME, 5), 5) * RANK(STD(CLOSE, 20)))) '''
    df = input_df.copy()
    df['corr1'] = qk_corr(df,'high','volume',5)
    df['tmp1'] = df.groupby('ticker')['corr1'].diff(5)
    df['tmp2'] = df.groupby('ticker')['close'].rolling(20,min_periods=16).std().values
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_104'] = - df['tmp1'] * df['rank1']
    df['GT_104'] *= df['valid']
    return df[['ticker','date','GT_104']]
def GT_105(input_df):
    '''(-1 * CORR(RANK(OPEN), RANK(VOLUME), 10)) '''
    df = input_df.copy()
    df['rank1'] = (df['raw_open'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_105'] = - qk_corr(df,'rank1','rank2',10)
    df['GT_105'] *= df['valid']
    return df[['ticker','date','GT_105']]
def GT_106(input_df):
    '''CLOSE-DELAY(CLOSE,20)'''
    df = input_df.copy()
    df['GT_106'] = df.groupby('ticker')['close'].diff(20)
    df['GT_106'] *= df['valid']
    return df[['ticker','date','GT_106']]
def GT_107(input_df):
    '''(((-1 * RANK((OPEN - DELAY(HIGH, 1)))) * RANK((OPEN - DELAY(CLOSE, 1)))) * RANK((OPEN - DELAY(LOW, 1)))) '''
    df = input_df.copy()
    df['tmp1'] = df['open'] - df.groupby('ticker')['high'].shift()
    df['tmp2'] = df['open'] - df.groupby('ticker')['close'].shift()
    df['tmp3'] = df['open'] - df.groupby('ticker')['low'].shift()
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['tmp3'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_107'] = df['rank1'] * df['rank2'] * df['rank3']
    df['GT_107'] *= df['valid']
    return df[['ticker','date','GT_107']]
def GT_108(input_df):
    '''((RANK((HIGH - MIN(HIGH, 2)))^RANK(CORR((VWAP), (MEAN(VOLUME,120)), 6))) * -1) '''
    df = input_df.copy()
    df['tmp1'] = df['high'] - df.groupby('ticker')['high'].rolling(2).min().values
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(120,min_periods=96).mean().values
    df['corr1'] = qk_corr(df,'vwap','tmp2',6)
    df['rank2'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_108'] = -1 * df['rank1'] ** df['rank2']
    df['GT_108'] *= df['valid']
    return df[['ticker','date','GT_108']]
def GT_109(input_df):
    '''SMA(HIGH-LOW,10,2)/SMA(SMA(HIGH-LOW,10,2),10,2) '''
    df = input_df.copy()
    df['tmp1'] = df['high'] - df['low']
    df['sma1'] = qk_ewma(df,'tmp1',2/10).values
    df['sma2'] = qk_ewma(df,'sma1',2/10).values
    df['GT_109'] = df['sma1'] / df['sma2']
    df['GT_109'] *= df['valid']
    return df[['ticker','date','GT_109']]
def GT_110(input_df):
    '''SUM(MAX(0,HIGH-DELAY(CLOSE,1)),20)/SUM(MAX(0,DELAY(CLOSE,1)-LOW),20)*100'''
    df = input_df.copy()
    df['tmp1'] = (df['high'] - df.groupby('ticker')['close'].shift()).apply(max,args=(0,))
    df['tmp2'] = (df.groupby('ticker')['close'].shift() - df['low']).apply(max,args=(0,))
    df['sum1'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values * 20
    df['sum2'] = df.groupby('ticker')['tmp2'].rolling(20,min_periods=16).mean().values * 20
    df['GT_110'] = df['sum1'] / df['sum2'] * 100
    df['GT_110'] *= df['valid']
    return df[['ticker','date','GT_110']]
def GT_111(input_df):
    '''SMA(VOL*((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-LOW),11,2)-SMA(VOL*((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-L OW),4,2) '''
    df = input_df.copy()
    df['tmp1'] = df['volume'] * (df['close'] * 2 - df['high'] - df['low']) / (df['high'] - df['low'])
    df['sma1'] = qk_ewma(df,'tmp1',2/11).values
    df['sma2'] = qk_ewma(df,'tmp1',2/4).values
    df['GT_111'] = (df['sma1'] - df['sma2']) * df['vol_fact']
    df['GT_111'] *= df['valid']
    return df[['ticker','date','GT_111']]
def GT_112(input_df):
    '''(SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12)-SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12))/
    (SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12)+SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12))*100'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff().apply(max,args=(0,))
    df['tmp2'] = -df.groupby('ticker')['close'].diff().apply(min,args=(0,))
    df['sum1'] = df.groupby('ticker')['tmp1'].rolling(12,min_periods=10).mean().values * 12
    df['sum2'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).mean().values * 12
    df['GT_112'] = (df['sum1'] - df['sum2']) / (df['sum1'] + df['sum2'])
    df['GT_112'] *= df['valid']
    return df[['ticker','date','GT_112']]
def GT_113(input_df):
    '''(-1 * ((RANK((SUM(DELAY(CLOSE, 5), 20) / 20)) * CORR(CLOSE, VOLUME, 2)) * RANK(CORR(SUM(CLOSE, 5), SUM(CLOSE, 20), 2)))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].shift(5).rolling(20,min_periods=16).mean().values
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr1'] = qk_corr(df,'close','volume',2)
    df['tmp2'] = df.groupby('ticker')['close'].rolling(5,min_periods=4).mean().values * 5
    df['tmp3'] = df.groupby('ticker')['close'].rolling(20,min_periods=16).mean().values * 20
    df['corr2'] = qk_corr(df,'tmp2','tmp3',2)
    df['rank2'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_113'] = - df['rank1'] * df['corr1'] * df['rank2']
    df['GT_113'] *= df['valid']
    return df[['ticker','date','GT_113']]
def GT_114(input_df):
    '''((RANK(DELAY(((HIGH - LOW) / (SUM(CLOSE, 5) / 5)), 2)) * RANK(RANK(VOLUME))) / (((HIGH - LOW) / (SUM(CLOSE, 5) / 5)) / (VWAP - CLOSE))) '''
    df = input_df.copy()
    df['tmp1'] = df['high'] - df['low']
    df['tmp2'] = df.groupby('ticker')['close'].rolling(5,min_periods=4).mean().values
    df['tmp3'] = df['tmp1'] / df['tmp2']
    df['rank1'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp4'] = df.groupby(df['date'])['tmp3'].shift(2)
    df['rank2'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp5']  = df['tmp3'] / (df['vwap'] - df['close'])
    df['GT_114'] = df['rank1'] * df['rank2'] / df['tmp5'] / df['prc_fact']
    df['GT_114'] *= df['valid']
    return df[['ticker','date','GT_114']]
def GT_115(input_df):
    '''(RANK(CORR(((HIGH * 0.9) + (CLOSE * 0.1)), MEAN(VOLUME,30), 10))^RANK(CORR(TSRANK(((HIGH + LOW) / 2), 4), TSRANK(VOLUME, 10), 7)))'''
    df = input_df.copy()
    df['tmp1'] = 0.9 * df['high'] + 0.1 * df['low']
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(30,min_periods=24).mean().values
    df['corr1'] = qk_corr(df,'tmp1','tmp2',10)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df[['high','low']].mean(1)
    df['tmp4'] = qk_ts_rank(df,'tmp3',4)
    df['tmp5'] = qk_ts_rank(df,'volume',10)
    df['corr2'] = qk_corr(df,'tmp4','tmp5',7)
    df['rank2'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    '''mark: override ** with *'''
    df['GT_115'] = df['rank1'] * df['rank2']
    df['GT_115'] *= df['valid']
    return df[['ticker','date','GT_115']]
def GT_116(input_df):
    '''REGBETA(CLOSE,SEQUENCE,20)'''
    df = input_df.copy()
    df['GT_116'] = reg_beta(df,'close',20) * df['prc_fact']
    df['GT_116'] *= df['valid']
    return df[['ticker','date','GT_116']]
def GT_117(input_df):
    '''((TSRANK(VOLUME, 32) * (1 - TSRANK(((CLOSE + HIGH) - LOW), 16))) * (1 - TSRANK(RET, 32))) '''
    df = input_df.copy()
    df['tmp1'] = df['close'] + df['high'] - df['low'] 
    df['tmp2'] = df['close'].pct_change()
    df['tsrank1'] = qk_ts_rank(df,'volume',32)
    df['tsrank2'] = qk_ts_rank(df,'tmp1',16)
    df['tsrank3'] = qk_ts_rank(df,'tmp2',32)
    x = df.groupby('ticker')['close'].shift(32)
    unit = x/x
    df['GT_117'] = df['tsrank1'] * (1 - df['tsrank2']) * (1 - df['tsrank3']) * unit
    df['GT_117'] *= df['valid']
    return df[['ticker','date','GT_117']]
def GT_118(input_df):
    '''SUM(HIGH-OPEN,20)/SUM(OPEN-LOW,20)*100 '''
    df = input_df.copy()
    df['tmp1'] = (df['high'] - df['open']).rolling(20,min_periods=16).mean() * 20
    df['tmp2'] = (df['open'] - df['low']).rolling(20,min_periods=16).mean() * 20
    x = df.groupby('ticker')['close'].shift(19)
    unit = x/x
    df['GT_118'] = df['tmp1'] / df['tmp2'] * 100 * unit
    df['GT_118'] *= df['valid']
    return df[['ticker','date','GT_118']]
def GT_119(input_df):
    '''(RANK(DECAYLINEAR(CORR(VWAP, SUM(MEAN(VOLUME,5), 26), 5), 7)) - 
    RANK(DECAYLINEAR(TSRANK(MIN(CORR(RANK(OPEN), RANK(MEAN(VOLUME,15)), 21), 9), 7), 8)))'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(5,min_periods=4).mean().values
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(26,min_periods=21).mean().values * 26
    df['corr1'] = qk_corr(df,'vwap','tmp2',5)
    df['tmp3'] = qk_decay_linear(df,'corr1',7)
    df['rank1'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_open'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(15,min_periods=12).mean().values
    df['rank3'] = (df['tmp4'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'rank2','rank3',21)
    df['tmp5'] = df.groupby('ticker')['corr2'].rolling(9,min_periods=7).min().values
    df['ts_rank1'] = qk_ts_rank(df,'tmp5',7)
    df['tmp6'] = qk_decay_linear(df,'ts_rank1',8)
    df['rank4'] = (df['tmp6'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_119'] = df['rank1'] - df['rank4']
    df['GT_119'] *= df['valid']
    return df[['ticker','date','GT_119']]
def GT_120(input_df):
    '''(RANK((VWAP - CLOSE)) / RANK((VWAP + CLOSE))) '''
    df = input_df.copy()
    df['rank1'] = ((df['raw_vwap'] - df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = ((df['raw_vwap'] + df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_120'] = df['rank1'] / df['rank2']
    df['GT_120'] *= df['valid']
    return df[['ticker','date','GT_120']]
def GT_121(input_df):
    '''((RANK((VWAP - MIN(VWAP, 12)))^TSRANK(CORR(TSRANK(VWAP, 20), TSRANK(MEAN(VOLUME,60), 2), 18), 3)) * -1) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['vwap'].rolling(12,min_periods=10).min().values
    df['rank1'] = ((df['vwap'] - df['tmp1']) * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['ts_rank1'] = qk_ts_rank(df,'vwap',20)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(60,min_periods=48).mean().values
    df['ts_rank2'] = qk_ts_rank(df,'tmp2',2)
    df['corr1'] = qk_corr(df,'ts_rank1','ts_rank2',18)
    df['ts_rank3'] = qk_ts_rank(df,'corr1',3)
    df['GT_121'] = df['rank1'] ** df['ts_rank3'] * -1
    df['GT_121'] *= df['valid']
    return df[['ticker','date','GT_121']]
def GT_122(input_df):
    '''(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2)-DELAY(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2),1))/DELAY(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2),1) '''
    df = input_df.copy()
    df['tmp'] = df['close'].apply(np.log)
    df['sma1'] = qk_ewma(df,'tmp',2/13).values
    df['sma2'] = qk_ewma(df,'sma1',2/13).values
    df['sma3'] = qk_ewma(df,'sma2',2/13).values
    df['GT_122'] = df['sma3'] / df.groupby('ticker')['sma3'].shift() -1
    df['GT_122'] *= df['valid']
    return df[['ticker','date','GT_122']]
def GT_123(input_df):
    '''((RANK(CORR(SUM(((HIGH + LOW) / 2), 20), SUM(MEAN(VOLUME,60), 20), 9)) < RANK(CORR(LOW, VOLUME, 6))) * -1)'''
    df = input_df.copy()
    df['tmp1'] = df[['high','low']].mean(1)
    df['sum1'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values * 20
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(60,min_periods=48).mean().values
    df['sum2'] = df.groupby('ticker')['tmp2'].rolling(20,min_periods=16).mean().values * 20
    df['corr1'] = qk_corr(df,'sum1','sum2',9)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'low','volume',6)
    df['rank2'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_123'] = (df['rank1'] < df['rank2']).astype(int) * -1
    df['GT_123'] *= df['valid']
    return df[['ticker','date','GT_123']]     
def GT_124(input_df):
    '''(CLOSE - VWAP) / DECAYLINEAR(RANK(TSMAX(CLOSE, 30)),2) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].rolling(30,min_periods=24).max().values
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = qk_decay_linear(df,'rank1',2)
    df['GT_124'] = (df['close'] - df['vwap']) / df['tmp2'] * df['prc_fact']
    df['GT_124'] *= df['valid']
    return df[['ticker','date','GT_124']]
def GT_125(input_df):
    '''(RANK(DECAYLINEAR(CORR((VWAP), MEAN(VOLUME,80),17), 20)) / RANK(DECAYLINEAR(DELTA(((CLOSE * 0.5) + (VWAP * 0.5)), 3), 16))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(80,min_periods=64).mean().values
    df['corr1'] = qk_corr(df,'vwap','tmp1',17)
    df['tmp2'] = qk_decay_linear(df, 'corr1',20)
    df['rank1'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df[['close','vwap']].mean(1).groupby(df['ticker']).diff(3)
    df['tmp4'] = qk_decay_linear(df, 'tmp3',16)
    df['rank2'] = (df['tmp4'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_125'] = df['rank1'] / df['rank2']
    df['GT_125'] *= df['valid']
    return df[['ticker','date','GT_125']]
def GT_126(input_df):
    '''(CLOSE+HIGH+LOW)/3 '''
    df = input_df.copy()
    df['GT_126'] = df[['raw_high','raw_low','raw_close']].mean(1)
    df['GT_126'] *= df['valid']
    return df[['ticker','date','GT_126']]
def GT_127(input_df):
    '''(MEAN((100*(CLOSE-MAX(CLOSE,12))/(MAX(CLOSE,12)))^2,12))^(1/2)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].rolling(12,min_periods=10).max().values
    df['tmp2'] = ((df['close'] - df['tmp1']) / df['tmp1']) ** 2
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).mean().values
    df['GT_127'] = df['tmp3'] ** 0.5 * 100
    df['GT_127'] *= df['valid']
    return df[['ticker','date','GT_127']]
def GT_128(input_df):
    '''100-(100/(1+SUM(((HIGH+LOW+CLOSE)/3>DELAY((HIGH+LOW+CLOSE)/3,1)?(HIGH+LOW+CLOSE)/3*VOLUME:0),14)
    /SUM(((HIGH+LOW+CLOSE)/3<DELAY((HIGH+LOW+CLOSE)/3,1)?(HIGH+LOW+CLOSE)/3*VOLUME:0), 14))) '''
    df = input_df.copy()
    df['tmp1'] = df[['high','low','close']].mean(1)   
    df['tmp2'] = df['tmp1'] * df['volume'] * (df.groupby('ticker')['tmp1'].diff().dropna()>0).astype(int)
    df['tmp3'] = df['tmp1'] * df['volume'] * (df.groupby('ticker')['tmp1'].diff().dropna()<0).astype(int)
    df['sum1'] = df.groupby('ticker')['tmp2'].rolling(14,min_periods=11).mean().values * 14
    df['sum2'] = df.groupby('ticker')['tmp3'].rolling(14,min_periods=11).mean().values * 14
    df['GT_128'] = (1-1/(1+ df['sum1'] / df['sum2'])) * 100
    df['GT_128'] *= df['valid']
    return df[['ticker','date','GT_128']]
def GT_129(input_df):
    '''SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12)'''
    df = input_df.copy()
    df['tmp'] = -1 * df.groupby('ticker')['close'].diff().apply(min,args=(0,))
    df['GT_129'] = df.groupby('ticker')['tmp'].rolling(12,min_periods=10).mean().values * 12 * df['prc_fact']
    df['GT_129'] *= df['valid']
    return df[['ticker','date','GT_129']]
def GT_130(input_df):
    '''(RANK(DECAYLINEAR(CORR(((HIGH + LOW) / 2), MEAN(VOLUME,40), 9), 10)) / RANK(DECAYLINEAR(CORR(RANK(VWAP), RANK(VOLUME), 7),3)))'''
    df = input_df.copy()
    df['tmp1'] = df[['high','low']].mean(1)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(40,min_periods=32).mean().values
    df['corr1'] = qk_corr(df,'tmp1','tmp2',9)
    df['tmp3'] = qk_decay_linear(df, 'corr1',10)
    df['rank1'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['vwap'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['volume'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'rank2','rank3',7)
    df['tmp4'] = qk_decay_linear(df, 'corr2',3)
    df['rank4'] = (df['tmp4'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_130'] = df['rank1'] / df['rank4']
    df['GT_130'] *= df['valid']
    return df[['ticker','date','GT_130']]
def GT_131(input_df):  
    '''(RANK(DELAT(VWAP, 1))^TSRANK(CORR(CLOSE,MEAN(VOLUME,50), 18), 18))'''
    df = input_df.copy()
    df['rank1'] = (df['vwap'].groupby(df['ticker']).diff() * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(50,min_periods=40).mean().values
    df['corr1'] = qk_corr(df,'close','tmp1',18)
    df['ts_rank1'] = qk_ts_rank(df,'corr1',18)
    df['GT_131'] = df['rank1'] ** df['ts_rank1']
    df['GT_131'] *= df['valid']
    return df[['ticker','date','GT_131']]
def GT_132(input_df):  
    '''MEAN(AMOUNT,20) '''
    df = input_df.copy()
    df['GT_132'] = df.groupby('ticker')['amount'].rolling(20,min_periods=16).mean().values
    df['GT_132'] *= df['valid']
    return df[['ticker','date','GT_132']]
def GT_133(input_df):  
    '''((20-HIGHDAY(HIGH,20))/20)*100-((20-LOWDAY(LOW,20))/20)*100'''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df.groupby('ticker')['high'].rolling(n,min_periods=16).apply(np.argmax,raw=True).values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(n,min_periods=16).apply(np.argmin,raw=True).values
    df['GT_133'] = (df['tmp1'] - df['tmp2']) / n * 100
    df['GT_133'] *= df['valid']
    return df[['ticker','date','GT_133']]
def GT_134(input_df):
    '''(CLOSE-DELAY(CLOSE,12))/DELAY(CLOSE,12)*VOLUME '''
    df = input_df.copy()
    n = 12
    df['GT_134'] = (df['close'] / df.groupby('ticker')['close'].shift(n) - 1) * df['raw_volume']
    df['GT_134'] *= df['valid']
    return df[['ticker','date','GT_134']]
def GT_135(input_df):
    '''SMA(DELAY(CLOSE/DELAY(CLOSE,20),1),20,1)'''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df['close'] / df.groupby('ticker')['close'].shift(n)
    df['tmp2'] = df.groupby('ticker')['tmp1'].shift(1)
    df['GT_135'] = qk_ewma(df,'tmp2',1/n).values
    df['GT_135'] *= df['valid']
    return df[['ticker','date','GT_135']]
def GT_136(input_df):
    '''((-1 * RANK(DELTA(RET, 3))) * CORR(OPEN, VOLUME, 10))'''
    df = input_df.copy()
    df['tmp1'] = df['close'] / df.groupby('ticker')['close'].shift() - 1
    df['tmp2'] = df.groupby('ticker')['tmp1'].shift(3)
    df['rank1'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr1'] = qk_corr(df,'open','volume',10)
    df['GT_136'] = - df['rank1'] * df['corr1']
    df['GT_136'] *= df['valid']
    return df[['ticker','date','GT_136']]
def GT_137(input_df):
    '''16*(CLOSE-DELAY(CLOSE,1)+(CLOSE-OPEN)/2+DELAY(CLOSE,1)-DELAY(OPEN,1))/
    ((ABS(HIGH-DELAY(CLOSE, 1))>ABS(LOW-DELAY(CLOSE,1)) & ABS(HIGH-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1))?
    ABS(HIGH-DELAY(CLOSE,1))+ABS(LOW-DELAY(CLOS E,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
    (ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) & ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(CLOSE,1))?
    ABS(LOW-DELAY(CLOSE,1))+ABS(HIGH-DELAY(CLO SE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
    ABS(HIGH-DELAY(LOW,1))+ABS(DELAY(CLOSE,1)-DELAY(OP EN,1))/4)))
    *MAX(ABS(HIGH-DELAY(CLOSE,1)),ABS(LOW-DELAY(CLOSE,1)))'''
    df = input_df.copy()
    df['tmp1'] = (df['high'] - df.groupby('ticker')['close'].shift()).abs()
    df['tmp2'] = (df['low'] - df.groupby('ticker')['close'].shift()).abs()
    df['tmp3'] = (df['high'] - df.groupby('ticker')['low'].shift()).abs()
    df['tmp4'] = (df['close'] - df['open']).abs().groupby(df['ticker']).shift()
    df['cond1'] = ((df['tmp1'] > df['tmp2']) & (df['tmp1'] > df['tmp3'])).astype(int)
    df['op1'] = df['tmp1'] +df['tmp2']/2 + df['tmp4']/4
    df['cond2'] = ((df['tmp2'] > df['tmp3']) & (df['tmp2'] > df['tmp1'])).astype(int)
    df['op2'] = df['tmp2'] +df['tmp1']/2 + df['tmp4']/4
    df['cond3'] = 1-(df['cond1']+df['cond2'])
    df['op3'] = df['tmp3'] + df['tmp4']/4
    df['tmp5'] = df['cond1'] * df['op1'] + df['cond2'] * df['op2'] + df['cond3'] * df['op3']
    df['GT_137'] = df[['tmp1','tmp2']].max(1) * 16 * (df['close'] + (df['close'] - df['open']) / 2 - df.groupby('ticker')['open'].shift()) / df['tmp5'] * df['prc_fact']
    df['GT_137'] *= df['valid']
    return df[['ticker','date','GT_137']]
def GT_138(input_df):
    '''((RANK(DECAYLINEAR(DELTA((((LOW * 0.7) + (VWAP *0.3))), 3), 20)) - 
    TSRANK(DECAYLINEAR(TSRANK(CORR(TSRANK(LOW, 8), TSRANK(MEAN(VOLUME,60), 17), 5), 19), 16), 7)) * -1)'''
    df = input_df.copy()
    df['tmp1'] = df['low'] * 0.7 + df['vwap'] * 0.3
    df['tmp2'] = df.groupby('ticker')['tmp1'].diff(3)
    df['tmp3'] = qk_decay_linear(df,'tmp2',20)
    df['rank1'] = (df['tmp3']* df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['ts_rank1'] = qk_ts_rank(df,'low',8)
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(60,min_periods=48).mean().values
    df['ts_rank2'] = qk_ts_rank(df,'tmp4',17)
    df['corr1'] = qk_corr(df,'ts_rank1','ts_rank2',5)
    df['ts_rank3'] = qk_ts_rank(df,'corr1',19)
    df['tmp5'] = qk_decay_linear(df,'ts_rank3',16)
    df['ts_rank4'] = qk_ts_rank(df,'tmp5',7)
    df['GT_138'] = df['ts_rank4'] - df['rank1']
    df['GT_138'] *= df['valid']
    return df[['ticker','date','GT_138']]
def GT_139(input_df):
    '''(-1 * CORR(OPEN, VOLUME, 10)) '''
    df = input_df.copy()
    df['GT_139'] = -qk_corr(df,'open','volume',10)
    df['GT_139'] *= df['valid']
    return df[['ticker','date','GT_139']]
def GT_140(input_df):
    '''MIN(RANK(DECAYLINEAR(((RANK(OPEN) + RANK(LOW)) - (RANK(HIGH) + RANK(CLOSE))), 8)), 
    TSRANK(DECAYLINEAR(CORR(TSRANK(CLOSE, 8), TSRANK(MEAN(VOLUME,60), 20), 8), 7), 3))'''
    df = input_df.copy()    
    df['rank1'] = (df['raw_open'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_low'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['raw_high'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank4'] = (df['raw_close'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp1'] = df['rank1'] + df['rank2'] - df['rank3'] - df['rank4']
    df['tmp2'] = qk_decay_linear(df,'tmp1',8)
    df['rank5'] = (df['tmp2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['ts_rank1'] = qk_ts_rank(df,'close',8)
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(60,min_periods=48).mean().values
    df['ts_rank2'] = qk_ts_rank(df,'tmp3',20)
    df['corr1'] = qk_corr(df,'ts_rank1','ts_rank2',8)
    df['tmp4'] = qk_decay_linear(df,'corr1',7)
    df['ts_rank3'] = qk_ts_rank(df,'tmp4',3)
    df['GT_140'] = df[['rank5','ts_rank3']].min(1)
    df['GT_140'] *= df['valid']
    return df[['ticker','date','GT_140']]
def GT_141(input_df):
    '''(RANK(CORR(RANK(HIGH), RANK(MEAN(VOLUME,15)), 9))* -1)'''
    df = input_df.copy()
    df['rank1'] = (df['raw_high'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(15,min_periods=12).mean().values
    df['rank2'] = (df['tmp1'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr1'] = qk_corr(df,'rank1','rank2',9)
    df['GT_141'] = -(df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_141'] *= df['valid']
    return df[['ticker','date','GT_141']]
def GT_142(input_df):
    '''(((-1 * RANK(TSRANK(CLOSE, 10))) * RANK(DELTA(DELTA(CLOSE, 1), 1))) 
    * RANK(TSRANK((VOLUME /MEAN(VOLUME,20)), 5))) '''
    df = input_df.copy()
    df['ts_rank1'] = qk_ts_rank(df,'close',10)
    df['rank1'] = (df['ts_rank1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp1'] = df['close'].groupby(df['ticker']).diff().groupby(df['ticker']).diff()
    df['rank2'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['tmp3'] = df['volume'] / df['tmp2']
    df['ts_rank2'] = qk_ts_rank(df,'tmp3',5)
    df['rank3'] = (df['ts_rank2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_142'] = -df['rank1'] * df['rank2'] * df['rank3']
    df['GT_142'] *= df['valid']
    return df[['ticker','date','GT_142']]
def GT_143(input_df):
    '''CLOSE>DELAY(CLOSE,1)?(CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*SELF:SELF'''
    '''hold on'''
    df = input_df.copy()
    df['GT_143'] = 0
    df['GT_143'] *= df['valid']
    return df[['ticker','date','GT_143']]
def GT_144(input_df):
    '''SUMIF(ABS(CLOSE/DELAY(CLOSE,1)-1)/AMOUNT,20,CLOSE<DELAY(CLOSE,1))/COUNT(CLOSE<DELAY(CLOSE, 1),20) '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = (df.groupby('ticker')['close'].shift() > df['close']).astype(int)
    df['tmp2'] = (df['close'] / df.groupby('ticker')['close'].shift() - 1).abs() / df['amount'] * df['tmp1']
    df['sum1'] = df.groupby('ticker')['tmp1'].rolling(n,min_periods=16).mean().values * 20
    df['sum2'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=16).mean().values * 20
    df['GT_144'] = df['sum2'] / df['sum1']
    df['GT_144'] *= df['valid']
    return df[['ticker','date','GT_144']]
def GT_145(input_df):
    '''(MEAN(VOLUME,9)-MEAN(VOLUME,26))/MEAN(VOLUME,12)*100'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(9,min_periods=7).mean().values
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(26,min_periods=21).mean().values
    df['tmp3'] = df.groupby('ticker')['volume'].rolling(12,min_periods=10).mean().values
    df['GT_145'] = (df['tmp1'] - df['tmp2']) / df['tmp3'] * 100
    df['GT_145'] *= df['valid']
    return df[['ticker','date','GT_145']]
def GT_146(input_df):
    '''MEAN((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-SMA((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1),61,2),20)*
    (( CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-SMA((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1),61,2))/
    SMA(((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-SMA((CLOSE-DELAY(CLOSE, 1))/DELAY(CLOSE,1),61,2)))^2,60,2)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff() / df.groupby('ticker')['close'].shift()
    df['sma1'] = qk_ewma(df,'tmp1',2/61)
    df['tmp2'] = df['tmp1'] - df['sma1']
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(20,min_periods=16).mean().values
    df['tmp4'] = df['tmp2'] ** 2
    df['sma2'] = qk_ewma(df,'tmp4',2/60)
    df['GT_146'] = df['tmp3'] * df['tmp2'] / df['sma2']
    df['GT_146'] *= df['valid']
    return df[['ticker','date','GT_146']]
def GT_147(input_df):
    '''REGBETA(MEAN(CLOSE,12),SEQUENCE(12)) '''
    df = input_df.copy()
    n = 12
    df['tmp1'] = df.groupby('ticker')['close'].rolling(n,min_periods=10).mean().values
    df['GT_147'] = reg_beta(df,'tmp1',n) * df['prc_fact']
    df['GT_147'] *= df['valid']
    return df[['ticker','date','GT_147']]
def GT_148(input_df):
    '''((RANK(CORR((OPEN), SUM(MEAN(VOLUME,60), 9), 6)) < RANK((OPEN - TSMIN(OPEN, 14)))) * -1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(60,min_periods=48).mean().values
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(9,min_periods=7).mean().values * 9
    df['corr1'] = qk_corr(df,'open','tmp2',6)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['open'].rolling(14,min_periods=12).min().values
    df['tmp4'] = df['open'] - df['tmp3']
    df['rank2'] = (df['tmp4'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_148'] = (df['rank1'] < df['rank2']).astype(int) * -1
    df['GT_148'] *= df['valid']
    return df[['ticker','date','GT_148']]
def GT_149(input_df):
    '''REGBETA(FILTER(CLOSE/DELAY(CLOSE,1)-1,BANCHMARKINDEXCLOSE<DELAY(BANCHMARKINDEXCLOSE,1) ),
    FILTER(BANCHMARKINDEXCLOSE/DELAY(BANCHMARKINDEXCLOSE,1)-1,BANCHMARKINDEXCLOSE<DELAY(BANCHMARKINDEXCLOSE,1)),252)'''
    df = input_df.copy()
    min_date = df['date'].min()
    bm_df = pd.read_sql("select date, close as bm_close from daily_index where ticker='sh000300' and date >= '%s'"%min_date,conn)
    df = df.merge(bm_df,on='date').sort_values(['ticker','date']).reset_index()
    df['tmp1'] = np.nan
    df['tmp1'].loc[df['bm_close']<df.groupby('ticker')['bm_close'].shift()] = df['close'] / df.groupby('ticker')['close'].shift() - 1
    df['tmp2'] = np.nan
    df['tmp2'].loc[df['bm_close']<df.groupby('ticker')['bm_close'].shift()] = df['bm_close'] / df.groupby('ticker')['bm_close'].shift() - 1
    df['GT_149'] = sample_beta(df,'tmp1','tmp2',252)
    df['GT_149'] *= df['valid']
    return df[['ticker','date','GT_149']]
def GT_150(input_df):
    '''(CLOSE+HIGH+LOW)/3*VOLUME'''
    df = input_df.copy()
    df['GT_150'] = df[['raw_close','raw_high','raw_low']].mean(1) / df['raw_volume']
    df['GT_150'] *= df['valid']
    return df[['ticker','date','GT_150']]
def GT_151(input_df):
    '''SMA(CLOSE-DELAY(CLOSE,20),20,1)'''
    df = input_df.copy()
    n = 20
    df['tmp'] = df.groupby('ticker')['close'].diff(n)
    df['GT_151'] = qk_ewma(df,'tmp',1/n) * df['prc_fact']
    df['GT_151'] *= df['valid']
    return df[['ticker','date','GT_151']]
def GT_152(input_df):
    '''SMA(MEAN(DELAY(SMA(DELAY(CLOSE/DELAY(CLOSE,9),1),9,1),1),12)-MEAN(DELAY(SMA(DELAY(CLOSE/DELAY (CLOSE,9),1),9,1),1),26),9,1) '''
    df = input_df.copy()
    n = 9
    df['tmp1'] = (df['close'] / df.groupby('ticker')['close'].shift(n)).groupby(df['ticker']).shift()
    df['sma1'] = qk_ewma(df,'tmp1',1/n)
    df['tmp2'] = df.groupby('ticker')['sma1'].shift()
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).mean().values
    df['tmp4'] = df.groupby('ticker')['tmp2'].rolling(26,min_periods=21).mean().values
    df['tmp5'] = df['tmp3'] - df['tmp4']
    df['GT_152'] = qk_ewma(df,'tmp5',1/n)
    df['GT_152'] *= df['valid']
    return df[['ticker','date','GT_152']]
def GT_153(input_df):
    '''(MEAN(CLOSE,3)+MEAN(CLOSE,6)+MEAN(CLOSE,12)+MEAN(CLOSE,24))/4''' 
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].rolling(3).mean().values
    df['tmp2'] = df.groupby('ticker')['close'].rolling(6,min_periods=5).mean().values
    df['tmp3'] = df.groupby('ticker')['close'].rolling(12,min_periods=10).mean().values
    df['tmp4'] = df.groupby('ticker')['close'].rolling(24,min_periods=20).mean().values
    df['GT_153'] = df[['tmp1','tmp2','tmp3','tmp4']].mean(1) * df['prc_fact']
    df['GT_153'] *= df['valid']
    return df[['ticker','date','GT_153']]
def GT_154(input_df):
    '''(((VWAP - MIN(VWAP, 16))) < (CORR(VWAP, MEAN(VOLUME,180), 18))) ''' 
    df = input_df.copy()
    df['tmp1'] = df['vwap'] - df.groupby('ticker')['vwap'].rolling(16,min_periods=14).min().values
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(180,min_periods=144).mean().values
    df['corr1'] = qk_corr(df,'vwap','tmp2',18)
    df['GT_154'] = ((df['tmp1'] * df['prc_fact']) < df['corr1']).astype(int)
    df['GT_154'] *= df['valid']
    return df[['ticker','date','GT_154']]
def GT_155(input_df):
    '''SMA(VOLUME,13,2)-SMA(VOLUME,27,2)-SMA(SMA(VOLUME,13,2)-SMA(VOLUME,27,2),10,2)'''
    df = input_df.copy()
    df['tmp1'] = qk_ewma(df,'volume',2/13)
    df['tmp2'] = qk_ewma(df,'volume',2/27)
    df['tmp3'] = df['tmp1'] - df['tmp2']
    df['tmp4'] = qk_ewma(df,'tmp3',2/10)
    df['GT_155'] = (df['tmp3'] - df['tmp4'])  * df['vol_fact']
    df['GT_155'] *= df['valid']
    return df[['ticker','date','GT_155']]
def GT_156(input_df):
    '''(MAX(RANK(DECAYLINEAR(DELTA(VWAP, 5), 3)), RANK(DECAYLINEAR(((DELTA(((OPEN * 0.15) + (LOW *0.85)), 2) / ((OPEN * 0.15) + (LOW * 0.85))) * -1), 3))) * -1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['vwap'].diff(5)
    df['tmp2'] = qk_decay_linear(df,'tmp1',3)
    df['rank1'] = (df['tmp2'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df['open'] * 0.15 + df['close'] * 0.85
    df['tmp4'] = df.groupby('ticker')['tmp3'].diff(2)
    df['tmp5'] = -df['tmp4'] / df['tmp3']
    df['tmp6'] = qk_decay_linear(df,'tmp5',3)
    df['rank2'] = (df['tmp6'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_156'] = df[['rank1','rank2']].max(1)
    df['GT_156'] *= df['valid']
    return df[['ticker','date','GT_156']]
def GT_157(input_df):
    '''(MIN(PROD(RANK(RANK(LOG(SUM(TSMIN(RANK(RANK((-1 * RANK(DELTA((CLOSE - 1), 5))))), 2), 1)))), 1), 5) + TSRANK(DELAY((-1 * RET), 6), 5))'''
    df = input_df.copy()
    df['tmp1'] = -df.groupby('ticker')['close'].diff(5)
    df['rank1'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df.groupby('ticker')['rank1'].rolling(2).min().values
    df['tmp3'] = df['tmp2'].apply(np.log)
    df['rank2'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp4'] = df.groupby('ticker')['rank2'].rolling(5,min_periods=4).min().values
    df['tmp5'] = -(df['close'] / df.groupby('ticker')['close'].shift()).groupby(df['ticker']).shift(6)
    df['ts_rank1'] = qk_ts_rank(df,'tmp5',5)
    df['GT_157'] = df['tmp4'] + df['ts_rank1']
    df['GT_157'] *= df['valid']
    return df[['ticker','date','GT_157']]
def GT_158(input_df):
    '''((HIGH-SMA(CLOSE,15,2))-(LOW-SMA(CLOSE,15,2)))/CLOSE '''
    df = input_df.copy()
    df['GT_158'] = (df['high'] -df['low']) / df['close']
    df['GT_158'] *= df['valid']
    return df[['ticker','date','GT_158']]
def GT_159(input_df):
    '''((CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),6))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),6) *12*24+
    (CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),12))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),12)*6*24+
    (CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),24))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),24)*12*24)*100/(6*12+6*24+12*24) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].shift()
    df['tmp2'] = df[['low','tmp1']].min(1)
    df['tmp3'] = df[['high','tmp1']].max(1) - df['tmp2']
    df['sum1'] = df.groupby('ticker')['tmp2'].rolling(6,min_periods=5).mean().values * 6
    df['sum2'] = df.groupby('ticker')['tmp3'].rolling(6,min_periods=5).mean().values * 6
    df['sum3'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).mean().values * 12
    df['sum4'] = df.groupby('ticker')['tmp3'].rolling(12,min_periods=10).mean().values * 12
    df['sum5'] = df.groupby('ticker')['tmp2'].rolling(24,min_periods=20).mean().values * 24
    df['sum6'] = df.groupby('ticker')['tmp3'].rolling(24,min_periods=20).mean().values * 24
    df['tmp4'] = (df['close'] - df['sum1']) / df['sum2'] * 12* 24 + (df['close'] - df['sum3']) / df['sum4'] * 6 * 24 + (df['close'] - df['sum5']) / df['sum6'] * 6 * 12
    df['GT_159'] = df['tmp4'] *100/(6*12+6*24+12*24)
    df['GT_159'] *= df['valid']
    return df[['ticker','date','GT_159']]
def GT_160(input_df):
    '''SMA((CLOSE<=DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1) '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = (df.groupby('ticker')['close'].diff()<=0).astype(int)
    df['tmp2'] = df.groupby('ticker')['close'].rolling(n,min_periods=16).std().values
    df['tmp3'] = df['tmp1'] * df['tmp2']
    df['GT_160'] = qk_ewma(df,'tmp3',1/n) * df['prc_fact']
    df['GT_160'] *= df['valid']
    return df[['ticker','date','GT_160']]
def GT_161(input_df):
    '''MEAN(MAX(MAX((HIGH-LOW),ABS(DELAY(CLOSE,1)-HIGH)),ABS(DELAY(CLOSE,1)-LOW)),12) '''
    df = input_df.copy()
    df['tmp1'] = df['high'] - df['low']
    df['tmp2'] = (df.groupby('ticker')['close'].shift() - df['high']).abs()
    df['tmp3'] = (df.groupby('ticker')['close'].shift() - df['low']).abs()
    df['max1'] = df[['tmp1','tmp2','tmp3']].max(1)
    df['GT_161'] = df.groupby('ticker')['max1'].rolling(12,min_periods=10).mean().values * df['prc_fact']
    df['GT_161'] *= df['valid']
    return df[['ticker','date','GT_161']]
def GT_162(input_df):
    '''(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100
    - MIN(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12))
    /(MAX(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12)
    -MIN(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12, 1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12)) '''
    df = input_df.copy()
    n = 12
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['tmp2'] = df['tmp1'].apply(max,args=(0,))
    df['tmp3'] = df['tmp1'].abs()
    df['sma1'] = qk_ewma(df,'tmp2',1/n)
    df['sma2'] = qk_ewma(df,'tmp3',1/n)
    df['tmp4'] = df['sma1'] - df['sma2']
    df['tmp5'] = df.groupby('ticker')['tmp4'].rolling(n,min_periods=10).min().values
    df['tmp6'] = df.groupby('ticker')['tmp4'].rolling(n,min_periods=10).max().values
    df['GT_162'] = (df['tmp4'] - df['tmp5']) / (df['tmp6'] - df['tmp5'])
    df['GT_162'] *= df['valid']
    return df[['ticker','date','GT_162']]
def GT_163(input_df):
    '''RANK(((((-1 * RET) * MEAN(VOLUME,20)) * VWAP) * (HIGH - CLOSE))) '''
    df = input_df.copy()
    df['tmp1'] = -df.groupby('ticker')['close'].diff() / df.groupby('ticker')['close'].shift()
    df['tmp2'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['tmp3'] = df['tmp1'] * df['tmp2'] * df['raw_vwap'] * (df['raw_high'] - df['raw_low'])
    df['GT_163'] = (df['tmp3'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_163'] *= df['valid']
    return df[['ticker','date','GT_163']]
def GT_164(input_df):
    '''SMA((((CLOSE>DELAY(CLOSE,1))?1/(CLOSE-DELAY(CLOSE,1)):1)
    -MIN(((CLOSE>DELAY(CLOSE,1))?1/(CLOSE-DELAY(CLOSE,1)):1),12))
        /(HIGH-LOW)*100,13,2) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['tmp2'] = 1
    df['tmp2'].loc[df['tmp1']>0] /= df['tmp1']
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).min().values
    df['tmp4'] = (df['tmp2'] - df['tmp3']) / (df['high'] - df['low'] + 0.0001) * 100
    df['GT_164'] = qk_ewma(df,'tmp4',2/13)
    df['GT_164'] *= df['valid']
    return df[['ticker','date','GT_164']]
def GT_165(input_df):
    '''MAX(SUMAC(CLOSE-MEAN(CLOSE,48)))-MIN(SUMAC(CLOSE-MEAN(CLOSE,48)))/STD(CLOSE,48)'''
    df = input_df.copy()
    n = 48
    df['tmp1'] = df.groupby('ticker')['close'].rolling(n,min_periods=39).mean().values
    df['tmp2'] = df['close'] - df['tmp1']
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=39).mean().values * 48
    df['tmp4'] = df.groupby('ticker')['tmp3'].rolling(n,min_periods=39).max().values
    df['tmp5'] = df.groupby('ticker')['tmp3'].rolling(n,min_periods=39).min().values
    df['tmp6'] = df.groupby('ticker')['close'].rolling(n,min_periods=39).std().values
    df['GT_165'] = (df['tmp4'] - df['tmp5']) / df['tmp6']
    df['GT_165'] *= df['valid']
    return df[['ticker','date','GT_165']]
def GT_166(input_df):
    '''-20*(20-1)^1.5*SUM(CLOSE/DELAY(CLOSE,1)-1-MEAN(CLOSE/DELAY(CLOSE,1)-1,20),20)
    /((20-1)*(20-2)(SUM((CLOSE/DELAY(CLOSE,1),20)^2,20))^1.5) '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df.groupby('ticker')['close'].diff() / df.groupby('ticker')['close'].shift()
    df['tmp2'] = df['tmp1'] - df.groupby('ticker')['tmp1'].rolling(n,min_periods=16).mean().values
    df['sum1'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=16).mean().values * 20
    '''mark: guess'''
    df['tmp3'] = (df['close'] / df.groupby('ticker')['close'].shift()).groupby(df['ticker']).rolling(n,min_periods=16).mean().values
    df['sum2'] = (df['tmp3'] ** 2).groupby(df['ticker']).rolling(n,min_periods=16).mean().values * 20
    df['GT_166'] = (-n * (n-1) ** 1.5 * df['sum1']) / ((n-1) * (n-2) * df['sum2'] ** 1.5)
    df['GT_166'] *= df['valid']
    return df[['ticker','date','GT_166']]
def GT_167(input_df):
    '''SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff().apply(max,args=(0,))
    df['GT_167'] = df.groupby('ticker')['tmp1'].rolling(12,min_periods=10).mean().values * 12 * df['prc_fact']
    df['GT_167'] *= df['valid']
    return df[['ticker','date','GT_167']]
def GT_168(input_df):
    '''(-1*VOLUME/MEAN(VOLUME,20)) '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(n,min_periods=16).mean().values
    df['GT_168'] = -df['volume'] / df['tmp1']
    df['GT_168'] *= df['valid']
    return df[['ticker','date','GT_168']]
def GT_169(input_df):
    '''SMA(MEAN(DELAY(SMA(CLOSE-DELAY(CLOSE,1),9,1),1),12)-MEAN(DELAY(SMA(CLOSE-DELAY(CLOSE,1),9,1),1), 26),10,1)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff()
    df['sma1'] = qk_ewma(df,'tmp1',1/9)
    df['tmp2'] = df.groupby('ticker')['sma1'].shift()
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(12,min_periods=10).mean().values
    df['tmp4'] = df.groupby('ticker')['tmp2'].rolling(26,min_periods=21).mean().values
    df['tmp5'] = df['tmp3'] - df['tmp4']
    df['GT_169'] = qk_ewma(df,'tmp5',1/10) * df['prc_fact']
    df['GT_169'] *= df['valid']
    return df[['ticker','date','GT_169']]
def GT_170(input_df):
    '''((((RANK((1 / CLOSE)) * VOLUME) / MEAN(VOLUME,20)) * ((HIGH * RANK((HIGH - CLOSE))) / (SUM(HIGH, 5) / 5))) - RANK((VWAP - DELAY(VWAP, 5)))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['rank1'] = ((1 / df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp2'] = df['rank1'] * df['volume'] / df['tmp1']
    df['rank2'] = ((df['raw_high'] - df['raw_close']) * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp3'] = df.groupby('ticker')['high'].rolling(5,min_periods=4).mean().values
    df['tmp4'] = df['high'] * df['rank2'] / df['tmp3']
    df['rank3'] = (df['vwap'].groupby(df['ticker']).diff(5) * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_170'] = df['tmp2'] * df['tmp4'] - df['rank3']
    df['GT_170'] *= df['valid']
    return df[['ticker','date','GT_170']]
def GT_171(input_df):
    '''((-1 * ((LOW - CLOSE) * (OPEN^5))) / ((CLOSE - HIGH) * (CLOSE^5))) '''
    df = input_df.copy()
    df['GT_171'] = -(df['low'] - df['close']) * df['open'] ** 5 / (df['close'] - df['high']) / df['close'] ** 5
    df['GT_171'] *= df['valid']
    return df[['ticker','date','GT_171']]
def GT_172(input_df):
    '''MEAN(ABS(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)-SUM((HD>0 & HD>LD)?HD:0,14)*100
    /SUM(TR,14))/(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)+SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))*100,6) '''
    df = input_df.copy()
    n=14
    df['hd'] = df.groupby('ticker')['high'].diff()
    df['ld'] = df.groupby('ticker')['low'].diff()
    df['tmp1'] = df['high'] -  df['low']
    df['tr'] = df[['tmp1','hd','ld']].max(1)
    df['tmp2'] = df['ld'] * ((df['ld']>0)&(df['ld']>df['hd'])).astype(int)
    df['tmp3'] = df['hd'] * ((df['hd']>0)&(df['hd']>df['ld'])).astype(int)
    df['sum1'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=11).mean().values * 14
    df['sum2'] = df.groupby('ticker')['tmp3'].rolling(n,min_periods=11).mean().values * 14
    df['sum3'] = df.groupby('ticker')['tr'].rolling(n,min_periods=11).mean().values * 14
    df['tmp4'] = ((df['sum1'] - df['sum2']) / df['sum3']).abs()
    df['tmp5'] = (df['sum1'] + df['sum2']) / df['sum3']
    df['GT_172'] = (df['tmp4'] / df['tmp5']).groupby(df['ticker']).rolling(6,min_periods=5).mean().values
    df['GT_172'] *= df['valid']
    return df[['ticker','date','GT_172']]
def GT_173(input_df):
    '''3*SMA(CLOSE,13,2)-2*SMA(SMA(CLOSE,13,2),13,2)+SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2)'''
    df = input_df.copy()
    alp = 2/13
    df['sma1'] = qk_ewma(df,'close',alp)
    df['sma2'] = qk_ewma(df,'sma1',alp)
    df['log_close'] = df['close'].apply(np.log)
    df['sma3'] = qk_ewma(df,'log_close',alp)
    df['sma4'] = qk_ewma(df,'sma3',alp)
    df['sma5'] = qk_ewma(df,'sma4',alp)
    df['GT_173'] = (3 * df['sma1']  -2*df['sma2']) * df['prc_fact'] + df['sma5'] + df['prc_fact'].apply(np.log)
    df['GT_173'] *= df['valid']
    return df[['ticker','date','GT_173']]
def GT_174(input_df):
    '''SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1) '''
    df = input_df.copy()
    n=20
    df['tmp1'] = (df.groupby('ticker')['close'].diff() > 0).astype(int)
    df['tmp2'] = df.groupby('ticker')['close'].rolling(20,min_periods=16).std().values
    df['tmp3'] = df['tmp1'] * df['tmp2']
    df['GT_174'] = qk_ewma(df,'tmp3',1/n) * df['prc_fact']
    df['GT_174'] *= df['valid']
    return df[['ticker','date','GT_174']]
def GT_175(input_df):
    '''MEAN(MAX(MAX((HIGH-LOW),ABS(DELAY(CLOSE,1)-HIGH)),ABS(DELAY(CLOSE,1)-LOW)),6)'''
    df = input_df.copy()
    df['tmp1'] = df['high'] -  df['low']
    df['tmp2'] = (df.groupby('ticker')['close'].shift() - df['high']).abs()
    df['tmp3'] = (df.groupby('ticker')['close'].shift() - df['low']).abs()
    df['max1'] = df[['tmp1','tmp2','tmp3']].max(1)
    df['GT_175'] = df.groupby('ticker')['max1'].rolling(6,min_periods=5).mean().values * df['prc_fact']
    df['GT_175'] *= df['valid']
    return df[['ticker','date','GT_175']]
def GT_176(input_df):
    '''CORR(RANK(((CLOSE - TSMIN(LOW, 12)) / (TSMAX(HIGH, 12) - TSMIN(LOW,12)))), RANK(VOLUME), 6) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['high'].rolling(12,min_periods=10).max().values
    df['tmp2'] = df.groupby('ticker')['low'].rolling(12,min_periods=10).min().values
    df['tmp3'] = (df['close'] - df['tmp2']) / (df['tmp1'] - df['tmp2'])
    df['rank1'] = (df['tmp3'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['raw_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_176'] = qk_corr(df,'rank1','rank2',6)
    df['GT_176'] *= df['valid']
    return df[['ticker','date','GT_176']]
def GT_177(input_df):
    '''((20-HIGHDAY(HIGH,20))/20)*100 '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df.groupby('ticker')['high'].rolling(n,min_periods=16).apply(np.argmax,raw=True).values
    df['GT_177'] = df['tmp1'] / n * 100
    df['GT_177'] *= df['valid']
    return df[['ticker','date','GT_177']]
def GT_178(input_df):
    '''(CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*VOLUME'''
    df = input_df.copy()
    df['GT_178'] = df.groupby('ticker')['close'].diff() / df.groupby('ticker')['close'].shift() * df['raw_volume']
    df['GT_178'] *= df['valid']
    return df[['ticker','date','GT_178']]
def GT_179(input_df):
    '''(RANK(CORR(VWAP, VOLUME, 4)) *RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME,50)), 12)))'''
    df = input_df.copy()
    df['corr1'] = qk_corr(df,'vwap','volume',4)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(50,min_periods=40).mean().values
    df['rank2'] = (df['raw_low'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank3'] = (df['tmp1'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['corr2'] = qk_corr(df,'rank2','rank3',12)
    df['rank4'] = (df['corr2'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_179'] = df['rank1'] * df['rank4']
    df['GT_179'] *= df['valid']
    return df[['ticker','date','GT_179']]
def GT_180(input_df):
    '''((MEAN(VOLUME,20) < VOLUME) ? ((-1 * TSRANK(ABS(DELTA(CLOSE, 7)), 60)) * SIGN(DELTA(CLOSE, 7)) : (-1 *  VOLUME))) '''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['close'].diff(7)
    df['tmp2'] = df['tmp1'].abs()
    df['ts_rank1'] = qk_ts_rank(df,'tmp2',60)
    df['tmp3'] = - df['ts_rank1'] * df['tmp1'].apply(np.sign)
    df['tmp4'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['GT_180'] = 0
    df['GT_180'].loc[df['tmp4']<df['volume']] = df['tmp3']
    df['GT_180'].loc[df['tmp4']>=df['volume']] = -df['raw_volume']
    df['GT_180'] *= df['valid']
    return df[['ticker','date','GT_180']]
def GT_181(input_df):
    '''SUM(((CLOSE/DELAY(CLOSE,1)-1)-MEAN((CLOSE/DELAY(CLOSE,1)-1),20))
    -(BANCHMARKINDEXCLOSE-MEAN(BANCHMARKINDEXCLOSE,20))^2,20)
    /SUM((BANCHMARKINDEXCLOSE-MEAN(BANCHMARKINDEXCLOSE,20))^3) '''
    df = input_df.copy()
    n=20
    min_date = df['date'].min()
    bm_df = pd.read_sql("select date, open as bm_open, close as bm_close from daily_index where ticker='sh000300' and date >= '%s'"%min_date,conn)
    df = df.merge(bm_df,on='date').sort_values(['ticker','date']).reset_index()
    df['tmp1'] = df.groupby('ticker')['close'].diff() / df.groupby('ticker')['close'].shift()
    df['tmp2'] = df.groupby('ticker')['tmp1'].rolling(n,min_periods=16).mean().values
    df['tmp3'] = df['bm_close'] - df.groupby('ticker')['bm_close'].rolling(n,min_periods=16).mean().values
    df['tmp4'] = (df['tmp1'] - df['tmp2'] - df['tmp3'] ** 2).groupby(df['ticker']).rolling(n,min_periods=16).mean().values * 20
    df['tmp5'] = (df['tmp3'] ** 3).groupby(df['ticker']).rolling(n,min_periods=16).mean().values * 20
    df['GT_181'] = df['tmp4'] / df['tmp5']
    df['GT_181'] *= df['valid']
    return df[['ticker','date','GT_181']]
def GT_182(input_df):
    '''COUNT((CLOSE>OPEN & BANCHMARKINDEXCLOSE>BANCHMARKINDEXOPEN)OR(CLOSE<OPEN & BANCHMARKINDEXCLOSE<BANCHMARKINDEXOPEN),20)/20'''
    df = input_df.copy()
    min_date = df['date'].min()
    bm_df = pd.read_sql("select date, open as bm_open, close as bm_close from daily_index where ticker='sh000300' and date >= '%s'"%min_date,conn)
    df = df.merge(bm_df,on='date').sort_values(['ticker','date']).reset_index()
    df['tmp1'] = (((df['close'] - df['open']) * (df['bm_close'] - df['bm_open'])) > 0).astype(int)
    df['GT_182'] = df.groupby('ticker')['tmp1'].rolling(20,min_periods=16).mean().values
    df['GT_182'] *= df['valid']
    return df[['ticker','date','GT_182']]
def GT_183(input_df):
    '''MAX(SUMAC(CLOSE-MEAN(CLOSE,24)))-MIN(SUMAC(CLOSE-MEAN(CLOSE,24)))/STD(CLOSE,24)'''
    df = input_df.copy()
    n = 24
    df['tmp1'] = df.groupby('ticker')['close'].rolling(n,min_periods=19).mean().values
    df['tmp2'] = df['close'] - df['tmp1']
    df['tmp3'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=19).mean().values * 24
    df['tmp4'] = df.groupby('ticker')['tmp3'].rolling(n,min_periods=19).max().values
    df['tmp5'] = df.groupby('ticker')['tmp3'].rolling(n,min_periods=19).min().values
    df['tmp6'] = df.groupby('ticker')['close'].rolling(n,min_periods=19).std().values
    df['GT_183'] = (df['tmp4'] - df['tmp5']) / df['tmp6']
    df['GT_183'] *= df['valid']
    return df[['ticker','date','GT_183']]
def GT_184(input_df):
    '''(RANK(CORR(DELAY((OPEN - CLOSE), 1), CLOSE, 200)) + RANK((OPEN - CLOSE)))'''
    df = input_df.copy()
    df['tmp1'] = df['open'] - df['close']
    df['tmp2'] = df.groupby('ticker')['tmp1'].shift()
    df['corr1'] = qk_corr(df,'close','tmp1',200)
    df['rank1'] = (df['corr1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['rank2'] = (df['tmp1'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_184'] = df['rank1'] + df['rank2']
    df['GT_184'] *= df['valid']
    return df[['ticker','date','GT_184']]
def GT_185(input_df):
    '''RANK((-1 * ((1 - (OPEN / CLOSE))^2)))'''
    df = input_df.copy()
    df['tmp1'] = - 1 * (1 - df['open'] / df['close']) ** 2
    df['GT_185'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
    df['GT_185'] *= df['valid']
    return df[['ticker','date','GT_185']]
def GT_186(input_df):
    '''(MEAN(ABS(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)-SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))
        /(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)+SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))*100,6)
        +DELAY(MEAN(ABS(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)-SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))
        /(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)+SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))*100,6),6))/2'''
    df = input_df.copy()
    n = 14
    df['hd'] = df.groupby('ticker')['high'].diff()
    df['ld'] = df.groupby('ticker')['low'].diff()
    df['tmp1'] = df['high'] -  df['low']
    df['tr'] = df[['tmp1','hd','ld']].max(1)
    df['tmp2'] = df['ld'] * ((df['ld']>0)&(df['ld']>df['hd'])).astype(int)
    df['tmp3'] = df['hd'] * ((df['hd']>0)&(df['hd']>df['ld'])).astype(int)
    df['sum1'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=11).mean().values * 14
    df['sum2'] = df.groupby('ticker')['tmp3'].rolling(n,min_periods=11).mean().values * 14
    df['sum3'] = df.groupby('ticker')['tr'].rolling(n,min_periods=11).mean().values * 14
    df['tmp4'] = ((df['sum1'] - df['sum2']) / df['sum3']).abs()
    df['tmp5'] = (df['sum1'] + df['sum2']) / df['sum3']
    df['tmp6'] = (df['tmp4'] / df['tmp5']).groupby(df['ticker']).rolling(6,min_periods=5).mean().values
    df['GT_186'] = (df['tmp6'] + df.groupby('ticker')['tmp6'].shift(6)) / 2
    df['GT_186'] *= df['valid']
    return df[['ticker','date','GT_186']]
def GT_187(input_df):
    '''SUM((OPEN<=DELAY(OPEN,1)?0:MAX((HIGH-OPEN),(OPEN-DELAY(OPEN,1)))),20)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['open'].diff()
    df['tmp2'] = df['high'] - df['open']
    df['tmp3'] = df[['tmp1','tmp2']].max(1) * (df['tmp1'] <= 0).astype(int)
    df['GT_187'] = df.groupby('ticker')['tmp3'].rolling(20,min_periods=16).mean().values * 20 * df['prc_fact']
    df['GT_187'] *= df['valid']
    return df[['ticker','date','GT_187']]
def GT_188(input_df):
    '''((HIGH-LOW–SMA(HIGH-LOW,11,2))/SMA(HIGH-LOW,11,2))*100'''
    df = input_df.copy()
    df['tmp1'] = df['high'] - df['low']
    df['tmp2'] = qk_ewma(df,'tmp1',2/11)
    df['GT_188'] = (df['tmp1'] - df['tmp2']) / df['tmp2'] * 100
    df['GT_188'] *= df['valid']
    return df[['ticker','date','GT_188']]
def GT_189(input_df):
    '''MEAN(ABS(CLOSE-MEAN(CLOSE,6)),6) '''
    df = input_df.copy()
    n = 6
    df['tmp1'] = df.groupby('ticker')['close'].rolling(n,min_periods=5).mean().values
    df['tmp2'] = (df['close'] - df['tmp1']).abs()
    df['GT_189'] = df.groupby('ticker')['tmp2'].rolling(n,min_periods=5).mean().values * df['prc_fact']
    df['GT_189'] *= df['valid']
    return df[['ticker','date','GT_189']]
def GT_190(input_df):
    '''LOG((COUNT(CLOSE/DELAY(CLOSE)-1>((CLOSE/DELAY(CLOSE,19))^(1/20)-1),20)-1)*
    (SUMIF(((CLOSE/DELAY(CLOSE)-1-(CLOSE/DELAY(CLOSE,19))^(1/20)-1))^2,20,CLOSE/DELAY(CLOSE)-1<(CLOSE/DELAY(CLOSE,19))^(1/20)-1))
    /((COUNT((CLOSE/DELAY(CLOSE)-1<(CLOSE/DELAY(CLOSE,19))^(1/20)-1),20))*
    (SUMIF((CLOSE/DELAY(CLOSE)-1-((CLOSE/DELAY(CLOSE,19))^(1/20)-1))^2,20,CLOSE/DELAY(CLOSE)-1>(CLOSE/DELAY(CLOSE,19))^(1/20)-1)))) '''
    df = input_df.copy()
    n = 20
    df['tmp1'] = df['close'] / df.groupby('ticker')['close'].shift() - 1
    df['tmp2'] = (df['close'] / df.groupby('ticker')['close'].shift(n-1)) ** (1/n) - 1
    df['tmp3'] = (df['tmp1'] - df['tmp2']) ** 2
    df['con1'] = (df['tmp1'] > df['tmp2']).astype(int)
    df['con2'] = (df['tmp1'] < df['tmp2']).astype(int)
    df['tmp4'] = df.groupby('ticker')['con1'].rolling(n,min_periods=16).mean().values * 20
    df['tmp5'] = df.groupby('ticker')['con2'].rolling(n,min_periods=16).mean().values * 20
    df['tmp6'] = df['tmp4'] * (df['tmp3'] * df['con2']).groupby(df['ticker']).rolling(n,min_periods=16).mean().values * 20
    df['tmp7'] = df['tmp5'] * (df['tmp3'] * df['con1']).groupby(df['ticker']).rolling(n,min_periods=16).mean().values * 20
    df['GT_190'] = (df['tmp6'] / df['tmp7']).apply(np.log)
    df['GT_190'] *= df['valid']
    return df[['ticker','date','GT_190']]
def GT_191(input_df):
    '''((CORR(MEAN(VOLUME,20), LOW, 5) + ((HIGH + LOW) / 2)) - CLOSE)'''
    df = input_df.copy()
    df['tmp1'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
    df['corr1'] = qk_corr(df,'tmp1','low',5)
    df['GT_191'] = df['corr1'] + ((df['high'] + df['low']) / 2 - df['close']) * df['prc_fact']
    df['GT_191'] *= df['valid']
    return df[['ticker','date','GT_191']]
