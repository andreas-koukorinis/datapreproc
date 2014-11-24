from numpy import *
import datetime
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder
from Utils.Regular import get_first_futures_contract,is_future_entity,is_future

# Track the daily log returns for the product
class DailyPrice(DailyBookListener):

    instances = {}

    def __init__(self, _identifier, _startdate, _enddate, _config):
        self.listeners = []
        self.values = []
        self.identifier = _identifier
        params = self.identifier.strip().split('.')
        self.product = params[1]
        if is_future(self.product):
            if is_future_entity(self.product):
                self.product = get_first_futures_contract(self.product)
        BookBuilder.get_unique_instance(self.product, _startdate, _enddate, _config).add_dailybook_listener(self)

    def add_listener(self, listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        if identifier not in DailyPrice.instances.keys() :
            new_instance = DailyPrice(identifier, _startdate, _enddate, _config)
            DailyPrice.instances[identifier] = new_instance
        return DailyPrice.instances[identifier]

    # Update the daily price on each ENDOFDAY event
    def on_dailybook_update(self, product, dailybook):
        self.values.append((dailybook[-1][0].date(), dailybook[-1][1]))
        for listener in self.listeners: listener.on_indicator_update(self.identifier, self.values)
