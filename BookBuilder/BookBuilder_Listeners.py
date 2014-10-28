from abc import ABCMeta,abstractmethod

class DailyBookListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_dailybook_update( self, product, dailybook ): pass

class IntradayBookListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_intraday_book_update( self, product, intradaybook ): pass

class SettlementListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def after_settlement_day( self ): pass
