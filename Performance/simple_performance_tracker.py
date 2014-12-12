import sys
import os
import datetime
import numpy as np
import scipy.stats as ss

from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EndOfDayListener
from Utils.Regular import get_first_futures_contract, is_future
from Utils import defaults
from BookBuilder.BookBuilder import BookBuilder
from Utils.global_variables import Globals
from DailyIndicators.Indicator_Listeners import IndicatorListener
from DailyIndicators.DailyLogReturns import DailyLogReturns

'''SimplePerformanceTracker listens to the Dispatcher for concurrent events and keeps track of daily_log_returns irrespective of whether the strategy is running or not'''
class SimplePerformanceTracker(EndOfDayListener, IndicatorListener):

    def __init__(self, products, all_products, _startdate, _enddate, _config):
        self.products = products
        self.all_products = all_products
        self.map_product_to_index = {} 
        _product_index = 0
        for _product in self.products:
            self.map_product_to_index[_product] = _product_index
            _product_index = _product_index + 1 
        self.currency_factor = Globals.currency_factor # TODO add in logreturns??
        self.daily_log_returns = np.empty(shape=(0))
        self.latest_log_returns = np.zeros(len(self.products))     
        self.net_log_return = 0.0
        self.cash = 1.0
        self.money_allocation = np.zeros(len(self.products)) # track the current weight of each product
        self.rebalance_weights = np.zeros(len(self.products)) # on every endofday event, if the product is trading, we will update 
        self.rebalance_date = datetime.datetime.fromtimestamp(0).date()
        self.to_update_rebalance_weight = [False]*len(self.products)
        self.current_loss = 0
        self.current_drawdown = 0
        Dispatcher.get_unique_instance(products, _startdate, _enddate, _config).add_end_of_day_listener(self) #TODO check that this should be updated prior to TradingAlgorithm
        self.bb_objects = {}
        self.log_return_history = {}

        for _product in self.all_products:
            self.bb_objects[_product] = BookBuilder.get_unique_instance(_product, _startdate, _enddate, _config)
        for _product in self.products:
            self.log_return_history[_product] = {}
            _log_return_identifier = 'DailyLogReturns.' + _product 
            DailyLogReturns.get_unique_instance(_log_return_identifier, _startdate, _enddate, _config).add_listener(self)

    def on_indicator_update(self, identifier, daily_log_returns_dt):
        _product = identifier.split('.')[1]
        _date = daily_log_returns_dt[-1][0]
        _log_return = daily_log_returns_dt[-1][1]
        self.log_return_history[_product][_date] = _log_return 
        self.latest_log_returns[self.map_product_to_index[_product]] = _log_return

    def compute_todays_log_return(self, date):
        # This is being called from compute_daily_stats
        _nominal_returns = np.exp(self.latest_log_returns)
        _new_money_allocation = self.money_allocation*_nominal_returns
        _new_portfolio_value = sum(_new_money_allocation) + self.cash
        _old_portfolio_value = sum(self.money_allocation) + self.cash
        self.money_allocation = _new_money_allocation
        _logret = np.log(_new_portfolio_value/_old_portfolio_value)
        self.daily_log_returns = np.append(self.daily_log_returns, _logret)
        self.net_log_return += self.daily_log_returns[-1]
        self.latest_log_returns *= 0.0

    def update_weights(self, date, weights):
        # We don't want to update weights just yet since these are desired weights in future.
        self.to_update_rebalance_weight = [True]*len(self.products)
        for _product in weights.keys():
            self.rebalance_weights[self.map_product_to_index[_product]] = weights[_product]
        self.rebalance_date = date

    def update_rebalanced_weights_for_trading_products(self, date):
        _portfolio_value = self.cash + sum(self.money_allocation)
        for _product in self.products:
            if self.is_trading_day(date, _product) and date > self.rebalance_date and self.to_update_rebalance_weight[_product]:
                self.to_update_rebalance_weight[_product] = False
                _new_money_allocated_to_product = _portfolio_value * self.rebalance_weights[self.map_product_to_index[_product]]
                _old_money_allocated_to_product = self.money_allocation[self.map_product_to_index[_product]]
                self.cash -= (_new_money_allocated_to_product - _old_money_allocated_to_product)
                self.money_allocation[self.map_product_to_index[_product]] = _new_money_allocated_to_product

    def is_trading_day(self, date, product):
        if is_future(product):
            product = get_first_futures_contract(product) #TODO check
        return len(self.bb_objects[product].dailybook) > 0 and self.bb_objects[product].dailybook[-1][0].date() == date # If the closing price for a product is available for a date

    # Called by Dispatcher
    def on_end_of_day(self, date):
        self.date = date     
        self.compute_todays_log_return(date)
        _current_dd_log = self.current_dd(self.daily_log_returns)
        self.current_drawdown = abs((np.exp(_current_dd_log) - 1)* 100.0)
        self.current_loss = (np.exp(self.net_log_return) - 1)*100.0
        self.update_rebalanced_weights_for_trading_products(date)

    # Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value 
    def current_dd(self, returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1])
