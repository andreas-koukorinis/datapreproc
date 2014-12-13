import sys
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from OrderManager.OrderManager import OrderManager
from Portfolio import Portfolio
from Utils import defaults
from Performance.PerformanceTracker import PerformanceTracker
from Performance.simple_performance_tracker import SimplePerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from execlogics.execlogic_list import is_valid_execlogic_name, get_module_name_from_execlogic_name

class TradeAlgorithm( EventsListener ):

    '''
    Base class for strategy development
    User should inherit this class and override init and OnEventListener functions
    '''
    def __init__( self, _trade_products, _all_products, _startdate, _enddate, _config, _log_filename):
        self.products = sorted(_trade_products) # we are doing this here so that multiple instances of indicators all point to same value.
        self.all_products = _all_products
        self.daily_indicators = {}
        self.start_date = _startdate
        self.end_date = _enddate

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
                        Indicatorclass = getattr( _indicator_module, indicator_name )
                        _instance = Indicatorclass.get_unique_instance( indicator, _startdate, _enddate, _config )
                        self.daily_indicators[_instance.identifier] = _instance

        # TradeAlgorithm might need to access BookBuilders to access market data.
        self.bb_objects = {}
        for product in self.all_products:
            self.bb_objects[product] = BookBuilder.get_unique_instance ( product, _startdate, _enddate, _config )

        # TradeAlgorithm will be notified once all indicators have been updated.
        # Currently it is implemented as an EventsListener
        dispatcher = Dispatcher.get_unique_instance ( self.all_products, _startdate, _enddate, _config )
        dispatcher.add_events_listener( self )

        self.order_manager = OrderManager.get_unique_instance ( self.all_products, _startdate, _enddate, _config, _log_filename )

        # Give strategy the access to the portfolio instance
        self.portfolio = Portfolio ( self.all_products, _startdate, _enddate, _config )

        # Initialize performance tracker with list of products
        self.performance_tracker = PerformanceTracker( self.all_products, _startdate, _enddate, _config, _log_filename )
        self.performance_tracker.portfolio = self.portfolio # Give Performance Tracker access to the portfolio     
        self.simple_performance_tracker = SimplePerformanceTracker(self.products, self.all_products, _startdate, _enddate, _config)

        #Instantiate ExecLogic
        _exec_logic_name = defaults.EXECLOGIC
        if _config.has_option('Parameters', 'execlogic'):
            _exec_logic_name = _config.get('Parameters', 'execlogic')
        if not(is_valid_execlogic_name(_exec_logic_name)):
            sys.exit("Cannot proceed with invalid Execlogic name")
        _exec_logic_module_name = get_module_name_from_execlogic_name(_exec_logic_name)
        ExecLogicClass = getattr(import_module('execlogics.' + _exec_logic_module_name), _exec_logic_name)
        self.exec_logic = ExecLogicClass(self.products, self.all_products, self.order_manager, self.portfolio, self.bb_objects, self.performance_tracker, self.simple_performance_tracker, _startdate, _enddate, _config)
        # By this time we have initialized all common elements, and hence initialize subclass
        self.init(_config)

    # User is expected to write the function
    def on_events_update(self, concurrent_events):
        pass

    # Return the portfolio variables as a dictionary
    def get_portfolio(self):
        return { 'cash' : self.portfolio.cash, 'num_shares' : self.portfolio.num_shares, 'products' : self.portfolio.products }

    def update_positions(self, dt, weights):
        self.simple_performance_tracker.update_weights(dt.date(), weights)
        self.exec_logic.update_positions(dt, weights)

    def rollover(self, dt):
        self.exec_logic.rollover(dt)
