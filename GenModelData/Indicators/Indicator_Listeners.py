from abc import ABCMeta,abstractmethod

class DailyLogReturnsListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_daily_log_returns_update(self,product,daily_log_returns): pass
