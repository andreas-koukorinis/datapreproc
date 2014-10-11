from abc import ABCMeta,abstractmethod

class DailyBookListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnDailyBookUpdate(self,product,dailybook,is_settlement_day): pass

class IntradayBookListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def OnIntradayBookUpdate(self,product,intradaybook): pass

