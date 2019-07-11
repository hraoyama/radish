"""

This is a global static context valid for all back-testing and execution


"""

import traceback
from typing import Union

from absl import logging

from paprika.core.config import BacktestConfig


class Context:
    def __init__(self, algo, config: Union[BacktestConfig]):
        self.algo = algo
        self.config = config
        self.event_engine = None
        self.event_register = None
        self.marketdata = None
        self.portfolio_manager = None
        self.execution_service = None

        from paprika.utils.time import FrameClock
        self.clock = FrameClock()


class BacktestContext(Context):
    def __init__(self, algo, config):
        super().__init__(algo, config)

    def __enter__(self):
        from paprika.core.event_engine import EventEngineBacktest
        self.event_engine = EventEngineBacktest(
            self.config.start_datetime, self.config.end_datetime)

        from paprika.data.marketdata import MarketDataBacktest
        self.marketdata = MarketDataBacktest()

        from paprika.portfolio.portfolio_manager import PortfolioManagerBacktest
        self.portfolio_manager = \
            PortfolioManagerBacktest(self.config.initial_portfolios)

        from paprika.execution.execution_service_backtest import ExecutionServiceBacktest
        self.execution_service = ExecutionServiceBacktest()

        from paprika.portfolio.analysis import PortfolioRecorder
        self.portfolio_recorder = PortfolioRecorder()

        from paprika.core.algo_runner import AlgoRunnerBacktest
        return AlgoRunnerBacktest(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if exc_type is KeyboardInterrupt:
                return False
            traceback.print_exception(exc_type, exc_val, exc_tb)
            return True


_context = None


def set_context(context: Context):
    global _context
    _context = context


def get_context() -> Union[BacktestContext]:
    return _context
