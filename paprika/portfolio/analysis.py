from math import ceil
from typing import Dict, List, NamedTuple, Tuple
from datetime import timedelta
import numpy as np
import pandas as pd

from paprika.portfolio.account_type import AccountType
from paprika.utils.time import datetime_to_micros, micros_for_frequency
from paprika.portfolio.portfolio import Portfolio


class AnalysisResult(NamedTuple):
    cagr: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    daily_turnover: float


# class PortfolioRecorder:
#     def __init__(self):
#         # (source, account_type) -> list(timestamp), list(portfolio)
#         self.records: \
#             Dict[Tuple[str, AccountType], Tuple[List[int], List]] = {}
#
#     def record_for_analysis(
#             self,
#             portfolio_by_source: Dict[Tuple[str, AccountType], Portfolio]):
#
#         for (source, account_type), portfolio in \
#                 portfolio_by_source.items():
#             timestamps, portfolios = \
#                 self.records.setdefault((source, account_type), ([], []))
#
#             if not portfolios or portfolio is not portfolios[-1]:
#                 timestamp = get_current_frame_timestamp()
#                 timestamps.append(timestamp)
#                 portfolios.append(portfolio)


class PortfolioAnalyser:
    @classmethod
    def analyse(cls, portfolio: Portfolio):
        return cls._analyse_portfolio_series(portfolio)

    @classmethod
    def _analyse_portfolio_series(cls, portfolio: Portfolio):
        market_values = cls._get_eod_values(portfolio)

        result = AnalysisResult(
            cagr=cls._calculate_cagr(market_values),
            sharpe_ratio=cls._calculate_sharpe_ratio(market_values),
            sortino_ratio=cls._calculate_sortino_ratio(market_values),
            max_drawdown=cls._calculate_max_drawdown(market_values),
            daily_turnover=cls._calculate_turnover(market_values))

        return result

    @classmethod
    def _get_eod_values(cls, portfolio: Portfolio):
        return portfolio.total_portfolio_records.resample('1D').last().fillna(method='ffill').portfolio_value

    @classmethod
    def _calculate_sharpe_ratio(cls, market_values):
        returns = market_values.pct_change().dropna()
        mean = np.mean(returns)
        std = np.std(returns)
        if np.isclose(std, 0.0):
            return np.nan
        else:
            return np.sqrt(365) * (mean / std)

    @classmethod
    def _calculate_sortino_ratio(cls, market_values):
        returns = market_values.pct_change().dropna()
        mean = np.mean(returns)
        downside_deviation = np.sqrt(
            sum(np.where(returns > 0, 0, (returns - mean)**2)) / len(returns))
        if np.isclose(downside_deviation, 0.0):
            return np.nan
        else:
            return np.sqrt(365) * (mean / downside_deviation)

    @classmethod
    def _calculate_max_drawdown(cls, market_values):
        max_drawdown = 0
        highest = None
        for value in market_values:
            if highest is None or highest < value:
                highest = value
            elif value < highest:
                ratio = (highest - value) / highest
                if max_drawdown < ratio:
                    max_drawdown = ratio
        return max_drawdown

    @classmethod
    def _calculate_cagr(cls, market_values):
        timestamps = market_values.index
        growth = market_values.iloc[-1] / market_values.iloc[0]
        time_diff = timestamps[-1] - timestamps[0]
        if isinstance(time_diff, int):
            num_years = (timestamps[-1] - timestamps[0]) / 365.0 / 86400.0 / 1000.0
        elif isinstance(time_diff, timedelta):
            num_years = time_diff.days / 365
        else:
            raise NotImplementedError(f'Do not handle {type(time_diff)} type of time for now.')

        return pow(growth, 1.0 / num_years) - 1.0

    @classmethod
    def _calculate_turnover(cls, market_values):
        total_turnover = 0
        last_portfolio_value = None
        for timestamp, portfolio_value in market_values.iteritems():
            if last_portfolio_value is not None:
                diffs_value = last_portfolio_value - portfolio_value
                total_turnover += (diffs_value / portfolio_value / 2.0)

            last_portfolio_value = portfolio_value

        num_days = PortfolioAnalyser.time_range(market_values.index, by='D')

        if np.isclose(num_days, 0):
            return 0
        else:
            return total_turnover / num_days

    @staticmethod
    def time_range(timestamps, by='Y') -> float:
        time_diff = timestamps[-1] - timestamps[0]
        if isinstance(time_diff, int):
            if by == 'Y':
                return (timestamps[-1] - timestamps[0]) / 365.0 / 86400.0 / 1000.0
            elif by == 'D':
                return (timestamps[-1] - timestamps[0]) / 86400.0 / 1000.0
        elif isinstance(time_diff, timedelta):
            if by == 'Y':
                return  time_diff.days / 365
            elif by == 'D':
                return time_diff.days
        else:
            raise NotImplementedError(f'Do not handle {type(time_diff)} type of time for now.')



# class PortfolioAnalyser:
#     _context = get_context()
#
#     @classmethod
#     def analyse(cls, recorder: PortfolioRecorder):
#
#         if len(recorder.records) > 1:
#             raise Exception('Currently only support analyzing single source.')
#
#         (source, _), (timestamps, portfolios) = next(
#             iter(recorder.records.items()))
#         portfolio_series = pd.Series(index=timestamps, data=portfolios)
#
#         return cls._analyse_portfolio_series(source, portfolio_series)
#
#     @classmethod
#     def _analyse_portfolio_series(cls, source, portfolio_series):
#         market_values = \
#             cls._get_eod_values(source, portfolio_series)
#
#         result = AnalysisResult(
#             cagr=cls._calculate_cagr(market_values),
#             sharpe_ratio=cls._calculate_sharpe_ratio(market_values),
#             sortino_ratio=cls._calculate_sortino_ratio(market_values),
#             max_drawdown=cls._calculate_max_drawdown(market_values),
#             daily_turnover=cls._calculate_turnover(source, portfolio_series))
#
#         portfolio_series.index = \
#             pd.to_datetime(portfolio_series.index, unit='us')
#         market_values.index = pd.to_datetime(market_values.index, unit='us')
#
#         return result, portfolio_series, market_values
#
#     @classmethod
#     def _get_eod_values(cls, source, portfolio_series):
#         eod_timestamps = []
#         eod_market_values = []
#
#         start = datetime_to_micros(cls._context.config.start_datetime)
#         end = datetime_to_micros(cls._context.config.end_datetime)
#         step = micros_for_frequency('1d')
#
#         eod_timestamp = ceil(start / step) * step
#
#         while eod_timestamp <= end:
#             eod_portfolio = portfolio_series.asof(eod_timestamp)
#             eod_market_value = cls._context.portfolio_manager.get_market_value(
#                 source, eod_portfolio, eod_timestamp)
#
#             eod_timestamps.append(eod_timestamp)
#             eod_market_values.append(eod_market_value)
#
#             eod_timestamp += step
#
#         return pd.Series(index=eod_timestamps, data=eod_market_values)
#
#     @classmethod
#     def _calculate_sharpe_ratio(cls, market_values):
#         returns = market_values.pct_change().dropna()
#         mean = np.mean(returns)
#         std = np.std(returns)
#         if np.isclose(std, 0.0):
#             return np.nan
#         else:
#             return np.sqrt(365) * (mean / std)
#
#     @classmethod
#     def _calculate_sortino_ratio(cls, market_values):
#         returns = market_values.pct_change().dropna()
#         mean = np.mean(returns)
#         downside_deviation = np.sqrt(
#             sum(np.where(returns > 0, 0, (returns - mean)**2)) / len(returns))
#         if np.isclose(downside_deviation, 0.0):
#             return np.nan
#         else:
#             return np.sqrt(365) * (mean / downside_deviation)
#
#     @classmethod
#     def _calculate_max_drawdown(cls, market_values):
#         max_drawdown = 0
#         highest = None
#         for value in market_values:
#             if highest is None or highest < value:
#                 highest = value
#             elif value < highest:
#                 ratio = (highest - value) / highest
#                 if max_drawdown < ratio:
#                     max_drawdown = ratio
#         return max_drawdown
#
#     @classmethod
#     def _calculate_cagr(cls, market_values):
#         timestamps = market_values.index
#         growth = market_values.iloc[-1] / market_values.iloc[0]
#         num_years = (timestamps[-1] - timestamps[0]) / 365.0 / 86400.0 / 1000.0
#         return pow(growth, 1.0 / num_years) - 1.0
#
#     @classmethod
#     def _calculate_turnover(cls, source, portfolio_series):
#         def diff_two_balances(balance1, balance2):
#             assets = set().union(balance1, balance2)
#             return {
#                 asset: abs(balance1[asset] - balance2[asset])
#                 for asset in assets
#             }
#
#         total_turnover = 0
#         last_portfolio = None
#         for timestamp, portfolio in portfolio_series.iteritems():
#             if last_portfolio:
#                 diffs = diff_two_balances(last_portfolio.balance,
#                                           portfolio.balance)
#                 diffs_portfolio = Portfolio(
#                     source, portfolio.base_currency, diffs)
#
#                 diffs_value = cls._context.portfolio_manager \
#                     .get_market_value(source, diffs_portfolio, timestamp)
#                 portfolio_value = cls._context.portfolio_manager \
#                     .get_market_value(source, portfolio, timestamp)
#
#                 total_turnover += (diffs_value / portfolio_value / 2.0)
#
#             last_portfolio = portfolio
#
#         start = datetime_to_micros(cls._context.config.start_datetime)
#         end = datetime_to_micros(cls._context.config.end_datetime)
#         num_days = (end - start) / 86400.0 / 1000.0
#
#         if np.isclose(num_days, 0):
#             return 0
#         else:
#             return total_turnover / num_days
