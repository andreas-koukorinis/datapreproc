import sys
import os
import datetime
import numpy
import scipy.stats as ss
from Utils.Regular import get_first_futures_contract, is_future
from Utils import defaults
from BookBuilder.BookBuilder import BookBuilder
from DailyIndicators.Indicator_Listeners import IndicatorListener
from DailyIndicators.DailyLogReturns import DailyLogReturns

'''SimplePerformanceTracker listens to the Dispatcher for concurrent events and keeps track of daily_log_returns irrespective of whether the strategy is running or not'''
class SimplePerformanceTracker(IndicatorListener):

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

    # Called by Trade Algorithm
    def update_performance(self, date):
        self.date = date
        self.compute_todays_log_return(date)
        self.update_rebalanced_weights_for_trading_products(date) # Read as order executed
        _current_dd_log = self.current_dd(self.daily_log_returns)
        self.current_drawdown = abs((numpy.exp(_current_dd_log) - 1)* 100.0)
        self.current_loss = abs(min(0.0, (numpy.exp(self.net_log_return) - 1)*100.0))

    # Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value 
    def current_dd(self, returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1])

    def compute_paper_returns(self, return_history): # TODO change to online computation
        if self.daily_log_returns.shape[0] < return_history: # for insufficient history return 0.0
            return 0.0
        else:
            return (numpy.exp(numpy.mean(self.daily_log_returns[-return_history:]) * 252) - 1) * 100

    def compute_strategy_volatility(self, volatility_history): # TODO change to online computation
        if self.daily_log_returns.shape[0] < volatility_history: # for insufficient history return 100.0 (same for each strategy)
            return 100.0
        else:
            return (numpy.exp(numpy.std(self.daily_log_returns[-volatility_history:]) * numpy.sqrt(252.0)) - 1) * 100
