import sys
from importlib import import_module

from dispatcher.dispatcher import Dispatcher
from dispatcher.dispatcher_listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from order_manager.order_manager import OrderManager
from portfolio import Portfolio
from Utils import defaults
from Utils.global_variables import Globals
from Utils.Calculate import get_current_prices, get_mark_to_market, find_most_recent_price, find_most_recent_price_future
from Utils.Regular import is_future, get_next_futures_contract, get_weights_for_trade_products
from performance.performance_tracker import PerformanceTracker
from performance.simple_performance_tracker import SimplePerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from execlogics.execlogic_list import is_valid_execlogic_name, get_module_name_from_execlogic_name

class TradeAlgorithm(EventsListener):
    """Base class for strategy development
    User should inherit this class and implement init and on_events_update functions
    
    Description: TradeAlgorithm implements the functions which are common to all the strategies
    TradeAlgorithm and hence every strategy has access to the order_manager,execlogic,
    portfolio,performance_tracker, simple_performance_tracker, dailybooks for all the products
    
    Listeners: None
    
    Listening to: Dispathcer for events updates(Currently events are end of day)
    
    Inherited by: Every strategy
    """
    
    def __init__( self, _trade_products, _all_products, _startdate, _enddate, _config, _log_filename):
        """Initializes the required variables, daily_indicators mentioned in the config file.
        Instantiates the performance_tracker,portfolio,simple_performance_tracker and execlogic
        Stores the reference to the required instances like order_manager,execlogic, portfolio,
        performance_tracker, simple_performance_tracker, dailybooks
        Starts listening to dispatcher for events update
        Calls the strategy's init function to allow it to perform initialization tasks # TODO call in the end
        
        Args:
            _trade_products(list): The products a strategy is interested in trading.Eg: ['fES','AQRIX']
            _all_products(list): The exhaustive list of products we end up trading.Eg: ['fES_1','fES_2','AQRIX']
            _startdate(date object): The start date of the simulation
            _enddate(date object): The end date of the simulation
            _config(ConfigParser handle): The handle to the config file of the strategy
            _log_filename(string): The file for logging.To pass to order_manager # TODO move to Globals
        
        Returns: Nothing 
        """   
        
        self.products = sorted(_trade_products) # we are doing this here so that multiple instances of indicators all point to same value.
        self.all_products = _all_products
        self.daily_indicators = {}
        self.start_date = _startdate
        self.end_date = _enddate
        
        # TODO this should be a global product to index map, that allows us to work with indices rather than product strings
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
        """This function is to be implemented by the strategy

        Args:
            concurrent_events(list): list of concurrent events(each event is a dictionary)
        
        Note: 1) The strategy is expected to call update_positions function at the end of this function with the new weights
              if it decides to rebalance
              2) Otherwise the strategy is expected to call the rollover function

        Returns: Nothing
        """
        pass

    def get_current_portfolio_weights(self, date):
        """Returns the current portfolio weights based on the notional amounts in each product.
           This function is currently used on by aggregator strategies
        Args: 
            date(date object): The current date on which the weights are to be computed #TODO move to watch

        Note: 1) Even though the portfolio is on the all_products, returns weights are on trade_products(hence the call get_weights_for_trade_products)

        Returns: Nothing
        """
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
        """Updates the performance and then weights of simple_performance_tracker
           Calls the exelogic to place orders such that the new weights are observed

        Args:
            dt(datetime object): The datetime of concurrent events to which the strategy responded with new weights
            weights(dict): The new weights the strategy wishes to have(dict from trade_products to weight values)

        Note:
            1) Updating the performance of simple performance tracker before calling execlogic is necessary,
           because the risk manager may use today's performance of the simple performance tracker to update
           risk level.

        It updates the simple_performance_tracker.
        Then it tells the simple_performance_tracker that these are the weights we want to have after this iteration.
        Then it asks self.exec_logic to execute orders to reach the desired portfolio allocations ( weights )
        
        Returns: Nothing
        """
        self.simple_performance_tracker.update_performance(dt.date())
        self.simple_performance_tracker.update_weights(dt.date(), weights)
        self.exec_logic.update_positions(dt, weights)

    def rollover(self, dt):
        """Updates the performance of the simple_performance_tracker
           Calls the exelogic to place any pending orders(due to product not trading) and to place orders for rollover
           of future contracts 

        Args:
            dt(datetime object): The datetime of concurrent events to which the strategy responded with a call to this function

        Note:
            1) Updating the performance of simple performance tracker before calling execlogic is necessary,
           because the risk manager may use today's performance of the simple performance tracker to update
           risk level.

        The reason to make a separate function than update_positions is that in this case we are not rebalncing to the desired weights.
        We are just checking for risk and rollover, things that cannot wait till next rebalncing day.
        Again we need to update simple_performance_tracker since it will be used by the risk manager.
        The simple_performance_tracker should probably be listening to end of day events.

        Returns: Nothing
        """
        self.simple_performance_tracker.update_performance(dt.date())
        self.exec_logic.rollover(dt)
