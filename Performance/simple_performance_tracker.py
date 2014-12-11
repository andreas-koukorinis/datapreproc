import sys
import os
import datetime
import numpy as np
import scipy.stats as ss

from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EndOfDayListener
from Utils.Regular import check_eod, get_dt_from_date, get_next_futures_contract, is_future
from Utils import defaults
from BookBuilder.BookBuilder import BookBuilder
from Utils.global_variables import Globals
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
        self.conversion_factor = Globals.conversion_factor
        self.currency_factor = Globals.currency_factor
        self.daily_log_returns = np.empty(shape=(0))
        self.latest_log_returns = zeros(len(self.products))     
        self.net_log_returns = 0.0
        self.current_weights = np.zeros(len(self.products)) # track the current weight of each product
        self.rebalance_weights = np.zeros(len(self.products))
        self.rebalance_date = datetime.datetime.fromtimestamp(0).date()
        self.to_update_rebalance_weight = [False]*len(self.products)
        self.current_loss = 0
        self.current_drawdown = 0
        Dispatcher.get_unique_instance(products, _startdate, _enddate, _config).add_end_of_day_listener(self)
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
        _nominal_return = 0.0
        for _product in self.products:
            _log_return_product = self.daily_log_returns[_product].get(date, 0.0) 
            _nominal_return += self.current_weights[self.map_product_to_index[_product]]*(np.exp(_log_return_product) - 1) 
        _log_ret = np.log(1 + _nominal_return)
        self.daily_log_returns = np.append(self.daily_log_returns, _log_ret)
        self.net_log_return += self.daily_log_returns[-1]
        self.latest_log_returns *= 0.0

    def update_weights(self, date, weights):
        self.to_update_rebalance_weight = [True]*len(self.products)
        for _product in weights.keys():
            self.rebalance_weights[self.map_product_to_index[_product]] = weights[product]
        self.rebalance_date = date

    def adjust_products_for_log_returns(self):
        self.current_weights = self.current_weights * np.exp(self.latest_log_returns)/sum(self.current_weights)

    def update_rebalanced_weights_for_trading_products(self, date):
        for _product in self.products:
            if is_trading_day(date, _product) and date > self.rebalance_date and self.to_update_rebalance_weight[_product]:
                self.to_update_rebalance_weight[_product] = False
                self.current_weights[self.map_product_to_index[_product]] = self.rebalance_weights[self.map_product_to_index[_product]]    

    def is_trading_day(self, date, product):
        return len(self.bb_objects[product]) > 0 and self.bb_objects[product].dailybook[-1][0].date() == date # If the closing price for a product is available for a date

    # Called by Dispatcher
    def on_end_of_day(self, date):
        self.date = date     
        self.compute_todays_log_return(date)
        self.compute_daily_stats(date)
        _current_dd_log = self.current_dd(self.daily_log_returns)
        self.current_drawdown = abs((np.exp(_current_dd_log) - 1)* 100.0)
        self.current_loss = (np.exp(self.net_log_returns) - 1)*100.0
        self.adjust_products_for_log_returns()
        self.update_rebalanced_weights_for_trading_products(date)

    # Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value 
    def current_dd(self, returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1])
