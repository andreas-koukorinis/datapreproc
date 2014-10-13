from abc import ABCMeta,abstractmethod

class BackTesterListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_order_update(self,filled_orders,date): pass
   
    @abstractmethod
    def after_settlement_day(self,product): pass
