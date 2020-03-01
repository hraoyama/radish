from paprika.alpha.base import *
from paprika.alpha.utils import *
from paprika.data.data_processor import DataProcessor


# def alpha_gt_1(dp: DataProcessor, period=6):
#     '''(-1 * CORR(RANK(DELTA(LOG(VOLUME), 1)), RANK(((CLOSE - OPEN) / OPEN)), 6)) '''
#     # df = input_df.copy()
#     # df['volume'] = df['volume'].apply(np.log)
#     # df['d_volume'] = df.groupby('ticker')['volume'].diff()
#     # df['rank1'] = (df['d_volume'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
#     # df['tmp'] = (df['close']-df['open']) /df['open']
#     # df['rank2'] = (df['tmp'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
#     # df['GT_1'] = qk_corr(df,'rank1','rank2',6) * -1
#     # df['GT_1'] *= df['valid']
#     # return df[['ticker','date','GT_1']]
#     # -1 * CORR(RANK(DELTA(LOG(VOLUME), 1)), RANK(((CLOSE - OPEN) / OPEN)), 6)
#     r1 = rank(delta(dp.volume.apply(np.log), 1))
#     r2 = rank((dp.close - dp.open) / dp.open)
#     return - correlation(r1, r2, period)
#
#
# def alpha_gt_2(dp: DataProcessor, period=10):
#     '''(-1 * DELTA((((CLOSE - LOW) - (HIGH - CLOSE)) / (HIGH - LOW)), 1)) '''
#     # df = input_df.copy()
#     # df['tmp'] = (df['close'] * 2 - df['high'] - df['low'])/(df['high'] - df['low'])
#     # df['GT_2'] = df.groupby('ticker')['tmp'].diff() * -1
#     # df['GT_2'] *= df['valid']
#
#     tmp = (dp.close * 2 - dp.high - dp.low) / (dp.high - dp.low)
#     return - delta(tmp, 1)
#     # return df[['ticker','date','GT_2']]


# def alpha_gt_3(input_df):
#     '''SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),6) '''
#     df = input_df.copy()
#     df['delay'] = df.groupby('ticker')['close'].shift().values
#
#     df['tmp'] = ((df['close']-df[['low','delay']].min(1))*(df['close']>df['delay']).astype(int)+(df['close']-df[['high','delay']].max(1))*(df['close']<df['delay']).astype(int)).values
#     df['GT_3'] = df.groupby('ticker')['tmp'].rolling(6,min_periods=5).mean().values * 6 * df['prc_fact']
#     df['GT_3'] *= df['valid']
#
#     return df[['ticker','date','GT_3']]
#     SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),6)
#
#     if dp.close == delay(dp.close, 1):
#         return 0
#     else:
#         if dp.close > delay(dp.close, 1):
#             return
#
# def GT_4(input_df):
#     '''((((SUM(CLOSE, 8) / 8) + STD(CLOSE, 8)) < (SUM(CLOSE, 2) / 2)) ? (-1 * 1) :
#         (((SUM(CLOSE, 2) / 2) < ((SUM(CLOSE, 8) / 8) - STD(CLOSE, 8))) ? 1 :
#             (((1 < (VOLUME / MEAN(VOLUME,20))) || ((VOLUME / MEAN(VOLUME,20)) == 1)) ? 1 : (-1 * 1)))) '''
#     df = input_df.copy()
#     df['mean1'] = df.groupby('ticker')['close'].rolling(8,min_periods=6).mean().values
#     df['mean2'] = df.groupby('ticker')['close'].rolling(2).mean().values
#     df['std1'] = df.groupby('ticker')['close'].rolling(8,min_periods=6).std().values
#     df['mean_vol'] = df.groupby('ticker')['volume'].rolling(20,min_periods=16).mean().values
#     df['tmp1'] = df['close'] * 0
#     df['tmp1'] += (df['mean2']>df['mean1']+df['std1']).astype(int)*-1
#     df['tmp1'] += (df['mean2']<df['mean1']-df['std1']).astype(int)
#     sub_df = df[df['tmp1']==0]
#     df['tmp2'] = ((sub_df['volume'] >= sub_df['mean_vol']).astype(int)*2-1)
#     df['tmp2'] =df['tmp2'].fillna(0)
#     df['GT_4'] = df['tmp1']+df['tmp2']
#     df['GT_4'] *= df['valid']
#     return df[['ticker','date','GT_4']]
# def GT_5(input_df):
#     '''(-1 * TSMAX(CORR(TSRANK(VOLUME, 5), TSRANK(HIGH, 5), 5), 3)) '''
#     df = input_df.copy()
#     n = 5
#     df['rank1'] = qk_ts_rank(df,'volume',n)
#     df['rank2'] = qk_ts_rank(df,'high',n)
#     df['corr'] = qk_corr(df,'rank1','rank2',n)
#     df['GT_5'] = df.groupby('ticker')['corr'].rolling(3).max().values * -1
#     df['GT_5'] *= df['valid']
#     return df[['ticker','date','GT_5']]
# def GT_6(input_df):
#     '''(RANK(SIGN(DELTA((((OPEN * 0.85) + (HIGH * 0.15))), 4)))* -1) '''
#     df = input_df.copy()
#     rho=0.85
#     df['tmp'] = df['open'] * rho + df['high'] * (1-rho)
#     df['sign'] = df.groupby('ticker')['tmp'].diff(4).apply(np.sign)
#     df['GT_6']= (df['sign'] * df['valid']).dropna().groupby(df['date']).rank(pct=True) * -1
#     df['GT_6'] *= df['valid']
#     return df[['ticker','date','GT_6']]
# def GT_7(input_df):
#     '''((RANK(MAX((VWAP - CLOSE), 3)) + RANK(MIN((VWAP - CLOSE), 3))) * RANK(DELTA(VOLUME, 3))'''
#     df = input_df.copy()
#     n = 3
#     df['tmp'] = df['vwap']-df['close']
#     df['max_tmp'] = df.groupby('ticker')['tmp'].rolling(n).max().values
#     df['min_tmp'] = df.groupby('ticker')['tmp'].rolling(n).min().values
#     df['rank1'] = (df['max_tmp'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
#     df['rank2'] = (df['min_tmp'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
#     df['d_volume'] = df.groupby('ticker')['volume'].diff(n)
#     df['rank3'] = (df['d_volume'] * df['vol_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
#     df['GT_7'] = (df['rank1']+df['rank2'])*df['rank3']
#     df['GT_7'] *= df['valid']
#     return df[['ticker','date','GT_7']]
# def GT_8(input_df):
#     '''RANK(DELTA(((((HIGH + LOW) / 2) * 0.2) + (VWAP * 0.8)), 4) * -1) '''
#     df = input_df.copy()
#     rho=0.8
#     df['tmp'] = (df['high']+df['low'])/2 * (1-rho) + df['vwap']*rho
#     df['diff'] = df.groupby('ticker')['tmp'].diff(4)*-1
#     df['GT_8'] = (df['diff'] * df['prc_fact'] * df['valid']).dropna().groupby(df['date']).rank(pct=True)
#     df['GT_8'] *= df['valid']
#     return df[['ticker','date','GT_8']]
# def GT_9(input_df):
#     '''SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,7,2)'''
#     df = input_df.copy()
#     df['tmp'] = (df['high']+df['low'])/2
#     df['diff'] = df.groupby('ticker')['tmp'].diff(1)
#     df['tmp1'] = df['diff'] * (df['high']-df['low'])/df['volume']
#     df['GT_9'] = qk_ewma(df,'tmp1',2/7)*1000 * df['prc_fact'] ** 2 / df['vol_fact']
#     df['GT_9'] *= df['valid']
#     return df[['ticker','date','GT_9']]
# def GT_10(input_df):
#     '''(RANK(MAX(((RET < 0) ? STD(RET, 20) : CLOSE)^2),5))'''
#     df = input_df.copy()
#     df['log_close'] = df['close'].apply(np.log)
#     df['return'] = df.groupby('ticker')['log_close'].diff()
#     df['std'] =df.groupby('ticker')['return'].rolling(20,min_periods=16).std().values
#     df['tmp'] = (df['std']*(df['return']<0).astype(int)+df['close']*(df['return']>=0).astype(int)) ** 2
#     df['tmp1'] = df.groupby('ticker')['tmp'].rolling(5).apply(np.argmax,raw=True).values
#     df['GT_10'] = (df['tmp1'] * df['valid']).dropna().groupby(df['date']).rank(pct=True) - 0.5
#     df['GT_10'] *= df['valid']
#     return df[['ticker','date','GT_10']]
