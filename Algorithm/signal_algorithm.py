import sys
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from Utils import defaults
from Utils.Regular import get_all_products
from Performance.simple_performance_tracker import SimplePerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator

class SignalAlgorithm(EventsListener): # TODO Should listen to events corresponding to own products
    '''
    Base class for signal development
    User should inherit this class and override init and OnEventListener functions
    '''
    def __init__(self, _all_products, _startdate, _enddate, _config, _agg_config):
        if not _config.has_option('Products', 'trade_products'):
            sys.exit('Cannot proceed without trade_products in signal config')
        self.products = sorted(_config.get('Products', 'trade_products').split(',')) # we are doing this here so that multiple instances of indicators all point to same value.
        self.all_products = get_all_products(_config)
        self.daily_indicators = {}
        self.start_date = _startdate
        self.end_date = _enddate
        self.weights = dict([(_product, 0.0) for _product in self.products])
        self.map_product_to_index = {} # this might be needed, dunno for sure
        _product_index = 0
        for _product in self.products:
            self.map_product_to_index[_product] = _product_index
            _product_index = _product_index + 1

        # Read indicator list from config file
        if _config.has_option('DailyIndicators','names'):
            indicators = _config.get( 'DailyIndicators', 'names' ).strip().split(" ")
            if indicators: # To handle names= case
                for indicator in indicators:
                    indicator_name = indicator.strip().split('.')[0]
                    if is_valid_daily_indicator(indicator_name):
                        _indicator_module = import_module( 'DailyIndicators.' + indicator_name )
                        Indicatorclass = getattr(_indicator_module, indicator_name)
                        _instance = Indicatorclass.get_unique_instance(indicator, _startdate, _enddate, _config)
                        self.daily_indicators[_instance.identifier] = _instance

        # TradeAlgorithm might need to access BookBuilders to access market data.
        self.bb_objects = {}
        for product in self.all_products:
            self.bb_objects[product] = BookBuilder.get_unique_instance (product, _startdate, _enddate, _agg_config)

        # TradeAlgorithm will be notified once all indicators have been updated.
        # Currently it is implemented as an EventsListener
        dispatcher = Dispatcher.get_unique_instance (_all_products, _startdate, _enddate, _agg_config)
        dispatcher.add_events_listener(self)

        self.simple_performance_tracker = SimplePerformanceTracker(self.products, self.all_products, _startdate, _enddate, _config)
        self.init(_config)

    # User is expected to write the function
    def on_events_update(self, concurrent_events):
        pass

    def update_positions(self, dt, weights):
        self.weights = weights # To be used by the aggregator
        self.simple_performance_tracker.update_performance(dt.date())
        self.simple_performance_tracker.update_weights(dt.date(), weights)

    def rollover(self, dt):
        self.simple_performance_tracker.update_performance(dt.date())
