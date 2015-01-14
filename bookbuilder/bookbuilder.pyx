# cython: profile=True
from dispatcher.dispatcher import Dispatcher
from dispatcher.dispatcher_listeners import DailyEventListener
from utils.regular import get_all_products
from utils.global_variables import Globals

'''BookBuilder listens to the dispatcher for daily update of its product
 Each product has a different bookbuilder
 The job of Book Builder is:
 Update the daily book[based on tuples(timestamp,closingprices)] on the 'ENDOFDAY' event corresponding to its product
 Call its Daily book listeners : Backtester,DailyLogReturn Indicator'''
class BookBuilder(DailyEventListener):

    instances = {}

    def __init__(self, product, _startdate, _enddate, _config):
        self.product = product
        self.dailybook = []  # List of tuples (date, price_for_calculations, is_last_trading_day, price_for_filling_order)
        self.intradaybook = []  # List of tuples (type,size,price) # Type = 0 -> bid, type = 1 -> ask
        self.dailybook_listeners = []
        self.intradaybook_listeners = []
        products = get_all_products(_config)
        dispatcher = Dispatcher.get_unique_instance(products, _startdate, _enddate, _config)
        dispatcher.add_event_listener(self, self.product)

    @staticmethod
    def get_unique_instance(product, _startdate, _enddate, _config):
        if(product not in BookBuilder.instances.keys()):
            new_instance = BookBuilder(product, _startdate, _enddate, _config)
            BookBuilder.instances[product]=new_instance
        return BookBuilder.instances[product]

    def add_dailybook_listener(self,listener):
        self.dailybook_listeners.append(listener)

    def add_intradaybook_listener(self, listener):
        self.intradaybook_listeners.append(listener)

    # Update the daily book with closing price and timestamp
    def on_daily_event_update(self, event):
        if Globals.product_type[self.product] == 'etf':
            self.dailybook.append((event['dt'], event['close'], event['is_last_trading_day'], event['open']))
        elif Globals.product_type[self.product] == 'fund':
            self.dailybook.append((event['dt'], event['close'], event['is_last_trading_day'], event['close']))
        elif Globals.product_type[self.product] == 'future':
            self.dailybook.append((event['dt'], event['close'], event['is_last_trading_day'], event['open']))
        for listener in self.dailybook_listeners:
            listener.on_dailybook_update(self.product, self.dailybook)

    # TODO {sanchit}
    def on_intraday_event_update(self, event):
        pass
