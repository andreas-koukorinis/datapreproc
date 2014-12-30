import sys
import os
import datetime
import numpy
import scipy.stats as ss

from utils.regular import get_first_futures_contract, is_future
from utils import defaults
from bookbuilder.bookbuilder import BookBuilder
from daily_indicators.indicator_listeners import IndicatorListener
from daily_indicators.daily_log_returns import DailyLogReturns

class SimplePerformanceTracker(IndicatorListener):
    '''SimplePerformanceTracker listens to the Dispatcher for concurrent events and keeps track of daily_log_returns irrespective of whether the strategy is running or not'''

    def __init__(self, products, all_products, _startdate, _enddate, _config):
        self.products = products
        self.all_products = all_products
        self.map_product_to_index = {} 
        _product_index = 0
        for _product in self.products:
            self.map_product_to_index[_product] = _product_index
            _product_index = _product_index + 1 
        self.daily_log_returns = numpy.empty(shape=(0))
        self.latest_log_returns = numpy.zeros(len(self.products))     
        self.net_log_return = 0.0
        self.cash = 1.0
        self.money_allocation = numpy.zeros(len(self.products)) # track the current weight of each product
        self.rebalance_weights = numpy.zeros(len(self.products)) # on every endofday event, if the product is trading, we will update 
        self.rebalance_date = datetime.datetime.fromtimestamp(0).date()
        self.to_update_rebalance_weight = [False]*len(self.products)
        self.current_loss = 0
        self.current_drawdown = 0
        self.bb_objects = {}
        self.log_return_history = numpy.empty(shape=(0,len(self.products)))

        for _product in self.all_products:
            self.bb_objects[_product] = BookBuilder.get_unique_instance(_product, _startdate, _enddate, _config)
        for _product in self.products:
            _log_return_identifier = 'DailyLogReturns.' + _product 
            DailyLogReturns.get_unique_instance(_log_return_identifier, _startdate, _enddate, _config).add_listener(self)

    def get_rebalance_weights(self):
        """read only access to rebalance_weights. Needed by risk management to assess the leverage sought by strategy"""
        return (self.rebalance_weights)

    def get_desired_leverage(self):
        """returns the currently desired leverage of the strategy"""
        return numpy.sum(numpy.abs(self.rebalance_weights))
    
    def on_indicator_update(self, identifier, daily_log_returns_dt):
        _product = identifier.split('.')[1]
        _date = daily_log_returns_dt[-1][0]
        _log_return = daily_log_returns_dt[-1][1]
        self.latest_log_returns[self.map_product_to_index[_product]] = _log_return

    def compute_todays_log_return(self, date):
        # This is being called from compute_daily_stats
        _nominal_returns = numpy.exp(self.latest_log_returns)
        _new_money_allocation = self.money_allocation*_nominal_returns
        _new_portfolio_value = sum(_new_money_allocation) + self.cash
        _old_portfolio_value = sum(self.money_allocation) + self.cash
        self.money_allocation = _new_money_allocation
        if (_old_portfolio_value <= 0.01) or numpy.isnan(_old_portfolio_value):
            sys.exit("Lost all the money!")
        _logret = numpy.log(_new_portfolio_value/_old_portfolio_value)
        self.daily_log_returns = numpy.append(self.daily_log_returns, _logret)
        self.net_log_return += self.daily_log_returns[-1]
        self.log_return_history = numpy.vstack((self.log_return_history, self.latest_log_returns))
        self.latest_log_returns *= 0.0

    def update_weights(self, date, weights):
        # We don't want to update weights just yet since these are desired weights in future.
        self.to_update_rebalance_weight = [True]*len(self.products)
        for _product in weights.keys():
            _portfolio_value = self.cash + sum(self.money_allocation)
            self.rebalance_weights[self.map_product_to_index[_product]] = _portfolio_value * weights[_product]
        self.rebalance_date = date

    def update_rebalanced_weights_for_trading_products(self, date):
        _portfolio_value = self.cash + sum(self.money_allocation)
        for _product in self.products:
            if self.is_trading_day(date, _product) and date > self.rebalance_date and self.to_update_rebalance_weight[self.map_product_to_index[_product]]:
                self.to_update_rebalance_weight[self.map_product_to_index[_product]] = False
                _new_money_allocated_to_product = self.rebalance_weights[self.map_product_to_index[_product]]
                _old_money_allocated_to_product = self.money_allocation[self.map_product_to_index[_product]]
                self.cash = self.cash - (_new_money_allocated_to_product - _old_money_allocated_to_product) - abs(_new_money_allocated_to_product - _old_money_allocated_to_product)*0.0001 # To account for trading cost
                self.money_allocation[self.map_product_to_index[_product]] = _new_money_allocated_to_product

    def is_trading_day(self, date, product):
        if is_future(product):
            product = get_first_futures_contract(product)
        return len(self.bb_objects[product].dailybook) > 0 and self.bb_objects[product].dailybook[-1][0].date() == date # If the closing price for a product is available for a date

    def update_performance(self, date):
        """All the computation is done by this function. It is called by TradeAlgorithm and SignalAlgorithm"""
        self.date = date
        self.compute_todays_log_return(date)
        self.update_rebalanced_weights_for_trading_products(date) # Read as order executed
        _current_dd_log = self.current_dd(self.daily_log_returns)
        self.current_drawdown = abs((numpy.exp(_current_dd_log) - 1)* 100.0)
        self.current_loss = abs(min(0.0, (numpy.exp(self.net_log_return) - 1)*100.0))

    def get_current_drawdown(self):
        """returns current drawdown"""
        return (self.current_drawdown)

    def get_current_loss(self):
        """returns current loss"""
        return self.current_loss
    
    def current_dd(self, returns):
        """Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value"""
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1])

    def compute_paper_returns(self, return_history): # TODO change to online computation
        if self.daily_log_returns.shape[0] < return_history: # for insufficient history return 0.0
            return 0.0
        else:
            return (numpy.exp(numpy.mean(self.daily_log_returns[-return_history:]) * 252) - 1) * 100

    def compute_historical_volatility(self, _volatility_history): # TODO change to online computation
        _recent_log_ret_anlualized_stdev = 100.0 # need this to be a default
        if self.daily_log_returns.shape[0] < 2: # for insufficient history return 100.0 (same for each strategy)
            _recent_log_ret_anlualized_stdev = 100.0 # need this to be a default
        else:
            _start_idx = max(0, self.daily_log_returns.shape[0] - _volatility_history)
            _recent_log_ret_anlualized_stdev = (numpy.exp(numpy.std(self.daily_log_returns[_start_idx : _start_idx + _volatility_history]) * numpy.sqrt(252.0)) - 1) * 100
            _recent_log_ret_anlualized_stdev = min ( 100.0, _recent_log_ret_anlualized_stdev )
            _recent_log_ret_anlualized_stdev = max ( 1.0, _recent_log_ret_anlualized_stdev )
        return (_recent_log_ret_anlualized_stdev)

    def compute_current_var_estimate(self, return_history):
        """Computes an estimate of daily VAR10 of the strategy based on daily rebalanced CWAS
        
        Args:
            weights: The current weights of the strategy
            return_history: The number of days of return history to be used

        Returns: The estimate of VAR10
        """
        if self.log_return_history.shape[0] < return_history:
            return 0.001 # Low value for complete allocation
        _log_returns = self.log_return_history[-return_history:,:]
        _cwas_log_return_series = numpy.log(1 + numpy.sum((numpy.exp(_log_returns) -1)*self.rebalance_weights, axis=1)) #TODO consider changing to actual weights
        _sorted_cwas_log_return_series = numpy.sort(_cwas_log_return_series)
        n = _sorted_cwas_log_return_series.shape[0]
        _end_index = min(n-1, int(0.1*n)) # Considering the worst 10% days
        _Var10_log = numpy.mean(_sorted_cwas_log_return_series[0:_end_index])
        _Var10 = abs((numpy.exp(_Var10_log) - 1)*100.0) # +ve value for VAR
        return max(0.001, _Var10) # To ensure that we dont return 0
