# cython: profile=True
import sys
from datetime import datetime
from importlib import import_module
from utils.calculate import get_current_prices, get_current_notional_amounts
from utils.global_variables import Globals
from utils.regular import is_future, get_base_symbol, get_future_mappings
from utils import defaults
from risk_management.risk_manager_list import is_valid_risk_manager_name, get_module_name_from_risk_manager_name
from dispatcher.dispatcher import Dispatcher
from dispatcher.dispatcher_listeners import DistributionDayListener

class ExecLogicAlgo(DistributionDayListener):
    def __init__(self, trade_products, all_products, order_manager, portfolio, bb_objects, performance_tracker, simple_performance_tracker, _startdate, _enddate, _config):
        self.trade_products = trade_products
        self.all_products = all_products
        self.future_mappings = get_future_mappings(all_products)
        self.order_manager = order_manager
        self.portfolio = portfolio
        self.bb_objects = bb_objects
        self.performance_tracker = performance_tracker
        self.conversion_factor = Globals.conversion_factor
        self.currency_factor = Globals.currency_factor
        self.product_to_currency = Globals.product_to_currency
        self.risk_level = 1.0
        #Instantiate RiskManager
        _risk_manager_name = defaults.RISK_MANAGER
        if _config.has_option('RiskManagement', 'risk_manager'):
            _risk_manager_name = _config.get('RiskManagement', 'risk_manager')
        if not is_valid_risk_manager_name(_risk_manager_name):
            sys.exit("Cannot proceed with invalid RiskManager name")
        _risk_manager_module_name = get_module_name_from_risk_manager_name(_risk_manager_name)
        _RiskManagerClass = getattr(import_module('risk_management.' + _risk_manager_module_name), _risk_manager_name)
        self.risk_manager = _RiskManagerClass(performance_tracker, simple_performance_tracker, _config)
        self.leverage = []
        self.last_trading_day_listeners = []
        self.current_date = datetime.strptime(_startdate, "%Y-%m-%d").date()
        if _config.has_option('Parameters', 'debug_level'):
            self.debug_level = _config.getint('Parameters','debug_level')
        else:
            self.debug_level = defaults.DEBUG_LEVEL  # Default value of debug level,in case not specified in config file
        self.orders_to_place = {} # The net order amount(in number of shares) which are to be placed on the next trading day
        self.distributions_to_reinvest = dict([(product, 0.0) for product in self.all_products if Globals.product_type[product] == 'etf' or Globals.product_type[product] == 'fund' or Globals.product_type[product] == 'stock'])
        for product in all_products:
            self.orders_to_place[product] = 0 # Initially there is no pending order for any product
        Dispatcher.get_unique_instance(all_products, _startdate, _enddate, _config).add_distribution_day_listener(self)
        self.init(_config)

    def on_distribution_day(self, event):
        pass

    # Place pending (self.orders_to_place) and rollover orders
    # This function should be implemented by the execlogic class
    def rollover(self, dt):
        pass

    # This function should be implemented by the execlogic class
    def update_positions(self, dt, weights):
        pass

    def notify_last_trading_day(self):
        _last_trading_day_base_symbols = []
        for product in self.all_products:
            _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and (self.current_date == self.bb_objects[product].dailybook[-1][0].date())
            if is_future(product) and _is_last_trading_day:
                _last_trading_day_base_symbols.append(get_base_symbol(product))
        _last_trading_day_base_symbols = list(set(_last_trading_day_base_symbols)) # Give unique base symbols
        for _base_symbol in _last_trading_day_base_symbols:
            self.on_last_trading_day(_base_symbol, self.future_mappings[_base_symbol])

    # Call performance tracker and portfolio on last trading day for a future contract
    # TODO{sanchit} change to listeners
    def on_last_trading_day(self,_base_symbol, future_mappings):
        self.performance_tracker.on_last_trading_day(_base_symbol, future_mappings)
        self.portfolio.on_last_trading_day(_base_symbol, future_mappings)

    def is_trading_day(self, dt, product):
        return self.bb_objects[product].dailybook[-1][0].date() == dt.date() # If the closing price for a product is available for a date,then the product is tradable on that date

    # Place an order to buy/sell 'num_shares' shares of 'product'
    # If num_shares is +ve -> it is a buy trade
    # If num_shares is -ve -> it is a sell trade
    def place_order(self, dt, product, num_shares):
        #if abs(num_shares) > 0.0000000001:
        if num_shares != 0:
            self.order_manager.send_order(dt, product, num_shares)
   
    # These order are sent directly yo the backtester by the order manager
    def place_order_agg(self, dt, product, num_shares):
        #if abs(num_shares) > 0.0000000001:
        if num_shares != 0:
            self.order_manager.send_order_agg(dt, product, num_shares)

    # Place an order to make the total number of shares of 'product' = 'target'
    # It can be a buy or a sell order depending on the current number of shares in the portfolio and the value of the target
    def place_order_target(self, dt, product, target):
        current_num = self.portfolio.get_portfolio()['num_shares'][product] + self.order_manager.to_be_filled[product]
        to_place = target - current_num
        self.place_order(dt, product, to_place)
