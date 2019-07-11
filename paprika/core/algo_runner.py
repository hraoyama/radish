from abc import ABC, abstractmethod
from enum import Enum
from typing import NamedTuple, Union

import pandas as pd

from paprika.core.config import BacktestConfig
from paprika.core.context import (BacktestContext, get_context,
                                  set_context)


class RunMode(Enum):
    LIVE = 0,
    BACKTEST = 1,
    PAPER = 2


class RunResult(NamedTuple):
    analysis: 'AnalysisResult'
    portfolios: pd.Series
    market_values: pd.Series


class AlgoRunner(ABC):
    def __init__(self, context):
        self.context = context

    @abstractmethod
    def run(self):
        pass


class AlgoRunnerBacktest(AlgoRunner):
    def __init__(self, context):
        super().__init__(context)

    def run(self):
        self.context.algo.initialise()

        get_context().clock.set_current_frame_time(
            self.context.event_engine.start_datetime)

        self.record_portfolio_for_analysis()
        while self.context.event_engine.run_one_event():
            self.record_portfolio_for_analysis()

        from paprika.portfolio.analysis import PortfolioAnalyser
        analysis, portfolios, market_values = PortfolioAnalyser.analyse(
            self.context.portfolio_recorder)

        return RunResult(
            analysis=analysis,
            portfolios=portfolios,
            market_values=market_values)

    def record_portfolio_for_analysis(self):
        self.context.portfolio_recorder.record_for_analysis(
            self.context.portfolio_manager.portfolio_by_source)


def setup_runner(algo, mode, config: Union[BacktestConfig]=None):
    set_context(BacktestContext(algo, config))

    return get_context()
