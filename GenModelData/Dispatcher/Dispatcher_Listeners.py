from abc import ABCMeta,abstractmethod

class DailyEventListener:
    __metaclass__=ABCMeta

    @abstractmethod
    def on_daily_event_update(self,event): pass
