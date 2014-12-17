import sys
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from OrderManager.OrderManager import OrderManager
from Portfolio import Portfolio
from Utils import defaults
from Utils.global_variables import Globals
from Utils.Calculate import get_current_prices, get_mark_to_market, find_most_recent_price, find_most_recent_price_future
from Utils.Regular import is_future, get_next_futures_contract, get_weights_for_trade_products
from Performance.PerformanceTracker import PerformanceTracker
from Performance.simple_performance_tracker import SimplePerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from execlogics.execlogic_list import is_valid_execlogic_name, get_module_name_from_execlogic_name

class TradeAlgorithm(EventsListener):

    '''
    Base class for strategy development
    User should inherit this class and override init and on_events_update functions
    '''
    def __init__( self, _trade_products, _all_products, _startdate, _enddate, _config, _log_filename):
        """
        Parameters ( TODO: what are they ? )
        _trade_products
        _all_products
        _startdate
        _enddate
        """
        self.products = sorted(_trade_products) # we are doing this here so that multiple instances of indicators all point to same value.
        self.all_products = _all_products
        self.daily_indicators = {}
        self.start_date = _startdate
        self.end_date = _enddate

        # this should be a global product to index map, that allows us to work with indices rather than product strings
        self.map_product_to_index = {} 
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

        # Call the strategy subclass
        self.init(_config)

        # TradeAlgorithm might need to access BookBuilders to access market data.
        self.bb_objects = {}
        for product in self.all_products:
            self.bb_objects[product] = BookBuilder.get_unique_instance ( product, _startdate, _enddate, _config )

        self.order_manager = OrderManager.get_unique_instance ( self.all_products, _startdate, _enddate, _config, _log_filename )

        # Give strategy the access to the portfolio instance
        self.portfolio = Portfolio ( self.all_products, _startdate, _enddate, _config )

        # Initialize performance tracker with list of products
        self.performance_tracker = PerformanceTracker( self.all_products, _startdate, _enddate, _config, _log_filename )
        self.performance_tracker.portfolio = self.portfolio # Give Performance Tracker access to the portfolio     
        self.simple_performance_tracker = SimplePerformanceTracker(self.products, self.all_products, _startdate, _enddate, _config)

        # Instantiate ExecLogic:
        # We read the parameter "execlogic" from the config, and based on name we import the relevant module
        # and instantiate the self.exec_logic object
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

        # TradeAlgorithm is an EventsListener, and will be notified after all the EventListeners have been updated
        # and all SignalAlgorithm instances have been updated.
        dispatcher = Dispatcher.get_unique_instance ( self.all_products, _startdate, _enddate, _config )
        dispatcher.add_events_listener( self )

    def on_events_update(self, concurrent_events):
        """Main function that gets control in the TradeAlgorithm. Needs to be implemented in child class"""
        pass

    def get_current_portfolio_weights(self, date):
        #_net_portfolio_value = self.performance_tracker.value[-1]
        # TODO should not recompute
        _net_portfolio_value = get_mark_to_market(date, get_current_prices(self.bb_objects), Globals.conversion_factor, Globals.currency_factor, Globals.product_to_currency, self.performance_tracker, self.portfolio.get_portfolio())
        weights = {}
        for _product in self.portfolio.num_shares.keys():
            _desired_num_shares = self.portfolio.num_shares[_product] + self.order_manager.to_be_filled[_product]
            if _desired_num_shares != 0:
                if is_future(_product):
                    _price = find_most_recent_price_future(self.bb_objects[_product].dailybook, self.bb_objects[get_next_futures_contract(_product)].dailybook, date)
                else:
                    _price = find_most_recent_price(self.bb_objects[_product].dailybook, date)
                _notional_value_product = _price * _desired_num_shares * Globals.conversion_factor[_product] * Globals.currency_factor[Globals.product_to_currency[_product]][date]
            else:
                _notional_value_product = 0.0
            weights[_product] = _notional_value_product/_net_portfolio_value
        return get_weights_for_trade_products(self.products, weights)

    def update_positions(self, dt, weights):
        """This function is going to be called by the child classes.
        It should probably have been called update_allocations,
        since it updates the portfolio allocation weights that the Strategy wants to have.
        It updates the simple_performance_tracker.
        Then it tells the simple_performance_tracker that these are the weights we want to have after this iteration.
        Then it asks self.exec_logic to execute orders to reach the desired portfolio allocations ( weights )"""
        self.simple_performance_tracker.update_performance(dt.date())
        self.simple_performance_tracker.update_weights(dt.date(), weights)
        self.exec_logic.update_positions(dt, weights)

    def rollover(self, dt):
        """
        The reason to make a separate function than update_positions is that in this case we are not rebalncing to the desired weights.
        We are just checking for risk and rollover, things that cannot wait till next rebalncing day.
        Again we need to update simple_performance_tracker since it will be used by the risk manager.
        The simple_performance_tracker should probably be listening to end of day events.
        """
        self.simple_performance_tracker.update_performance(dt.date())
        self.exec_logic.rollover(dt)
