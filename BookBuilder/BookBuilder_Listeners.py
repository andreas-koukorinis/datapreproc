from abc import ABCMeta,abstractmethod

class DailyBookListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnDailyBookUpdate(self,product,dailybook,is_last_trading_day): pass

class IntradayBookListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnIntradayBookUpdate(self,product,intradaybook): pass

