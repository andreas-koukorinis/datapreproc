import sys
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from Utils.DbQueries import conv_factor
from OrderManager.OrderManager import OrderManager
from Portfolio import Portfolio
from Performance.PerformanceTracker import PerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator

class TradeAlgorithm( EventsListener ):

    '''
    Base class for strategy development
    User should inherit this class and override init and OnEventListener functions
    '''
    def __init__( self, _trade_products, _all_products, _startdate, _enddate, _config, _log_filename):
        self.products = sorted(_trade_products) # we are doing this here so that 
        self.all_products = _all_products
        self.daily_indicators = {}
        self.start_date = _startdate
        self.end_date = _enddate
        self.init( _config )

        # Read indicator list from config file
        if _config.has_option('DailyIndicators','names'):
            indicators = _config.get( 'DailyIndicators', 'names' ).strip().split(" ")
            if indicators: # To handle names= case
                for indicator in indicators:
                    indicator_name = indicator.strip().split('.')[0]
                    if is_valid_daily_indicator(indicator_name):
                        module = import_module( 'DailyIndicators.' + indicator_name )
                        Indicatorclass = getattr( module, indicator_name )
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

        #Instantiate ExecLogic
        exec_logic_name = _config.get( 'Parameters', 'exec_logic' )
        exec_logic_module = import_module( 'ExecLogics.' + exec_logic_name )
        ExecLogicClass = getattr( exec_logic_module, exec_logic_name )
        self.exec_logic = ExecLogicClass( self.products, self.all_products, self.order_manager, self.portfolio, self.bb_objects, self.performance_tracker, _startdate, _enddate, _config )

    # User is expected to write the function
    def on_events_update( self, concurrent_events ):
        pass

    # Return the portfolio variables as a dictionary
    def get_portfolio( self ):
        return { 'cash' : self.portfolio.cash, 'num_shares' : self.portfolio.num_shares, 'products' : self.portfolio.products }

    def update_positions( self, dt, weights ):
        self.exec_logic.update_positions( dt, weights )

    def rollover( self, dt ):
        self.exec_logic.rollover( dt )
