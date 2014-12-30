import sys
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from Utils import defaults
from Utils.Regular import get_all_products
from performance.simple_performance_tracker import SimplePerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator

class SignalAlgorithm(EventsListener): # TODO May be Should listen to events corresponding to own products
    """Base class for signal development
    User should inherit this class and implement init and on_events_update functions
    
    Description: SignalAlgorithm implements the functions which are common to all the signals
                 SignalAlgorithm and hence every signal has access to the simple_performance_tracker,
                 dailybooks for all the products

    Listeners: None
    
    Listening to: Dispathcer for events updates(Currently events are end of day)
    
    Inherited by: Every signal
    """
    
    def __init__(self, _all_products, _startdate, _enddate, _config, _main_strategy_config):
        """Initializes the required variables, daily_indicators mentioned in the signal's config file.
        Instantiates the simple_performance_tracker
        Stores the reference to the required instances like simple_performance_tracker, dailybooks
        Starts listening to dispatcher for events update
        Calls the signal's init function to allow it to perform initialization tasks
        
        Args:
           _all_products(list): The exhaustive list of products aggregator ends up trading.Eg: ['fES_1','fES_2','AQRIX']
           _startdate(date object): The start date of the simulation
           _enddate(date object): The end date of the simulation
           _config(ConfigParser handle): The handle to the config file of the signal
           _main_strategy_config(ConfigParser handle): The handle to the config file of the aggregator

        Returns: Nothing 
        """

        self.day = -1 # TODO move this to "watch" or a global time manager
        self.last_rebalanced_day = -1

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
            self.bb_objects[product] = BookBuilder.get_unique_instance (product, _startdate, _enddate, _main_strategy_config)

        # TradeAlgorithm will be notified once all indicators have been updated.
        # Currently it is implemented as an EventsListener
        dispatcher = Dispatcher.get_unique_instance (_all_products, _startdate, _enddate, _main_strategy_config)
        dispatcher.add_events_listener(self)

        self.simple_performance_tracker = SimplePerformanceTracker(self.products, self.all_products, _startdate, _enddate, _config)

        self.rebalance_frequency = 5
        self.minimum_leverage = 1.0
        self.maximum_leverage = 1.0
        self.init(_config)

    def process_param_file(self, _paramfilepath, _config):
        """This function reads the parameter file and loads up the main parameters.
        In the super class SignalAlgorithm we are reading
        rebalance_frequency(int)
        minimum_leverage(float)
        maximum_leverage(float)
        """
        _param_file_handle = open( _paramfilepath, "r" )
        for _param_line in _param_file_handle:
            # We expect lines like:
            # rebalance_frequency 5
            # minimum_leverage 0.1
            # maximum_leverage 20
            _param_line_words = _param_line.strip().split(' ')
            if (len(_param_line_words) >= 2):
                if (_param_line_words[0] == 'rebalance_frequency'):
                    self.rebalance_frequency = int(_param_line_words[1])
                elif (_param_line_words[0] == 'minimum_leverage'):
                    self.minimum_leverage = float(_param_line_words[1])
                elif (_param_line_words[0] == 'maximum_leverage'):
                    self.maximum_leverage = float(_param_line_words[1])
        
        # legacy param processing code
        if _config.has_option('Parameters', 'rebalance_frequency'):
            self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')

    # User is expected to write the function
    def on_events_update(self, concurrent_events):
        """This function is to be implemented by the signal

        Args:
            concurrent_events(list): list of concurrent events(each event is a dictionary)
        
        Note: 1) The signal is expected to call update_positions function at the end of this function with the new weights
              if it decides to rebalance/recompute weights
              2) Otherwise the signal is expected to call the rollover function

        Returns: Nothing
        """
        pass

    def update_positions(self, dt, weights):
        """Updates the performance and then weights of simple_performance_tracker
           Updates its weights so that the aggregator can use these weights while coming up with aggregate weights

        Args:
            dt(datetime object): The datetime of concurrent events to which the signal responded with new weights
            weights(dict): The new weights the signal wishes to have(dict from trade_products to weight values)

        Returns: Nothing
        """
        self.weights = weights # To be used by the aggregator
        self.simple_performance_tracker.update_performance(dt.date())
        self.simple_performance_tracker.update_weights(dt.date(), weights)

    def rollover(self, dt):
        """Updates the performance of the simple_performance_tracker

        Args:
            dt(datetime object): The datetime of concurrent events to which the strategy responded with a call to this function

        Returns: Nothing
        """
        self.simple_performance_tracker.update_performance(dt.date())
