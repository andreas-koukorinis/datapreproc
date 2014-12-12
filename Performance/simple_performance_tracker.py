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
        self.current_weights = np.zeros(len(self.products)) # track the current weight of each product
        self.rebalance_weights = np.zeros(len(self.products)) # on every endofday event, if the product is trading, we will update 
        self.rebalance_date = datetime.datetime.fromtimestamp(0).date()
        self.current_loss = 0
        self.current_drawdown = 0
        self.lastest_weights_update_date = datetime.datetime.fromtimestamp(0).date()
        Dispatcher.get_unique_instance(products, _startdate, _enddate, _config).add_end_of_day_listener(self)
        self.bb_objects = {}
        self.daily_log_returns = {}
        for _product in self.all_products:
            self.bb_objects[_product] = BookBuilder.get_unique_instance(_product, _startdate, _enddate, _config)
        for _product in self.products:
            self.daily_log_returns[_product] = {}
            _log_return_identifier = 'DailyLogReturns.' + _product 
            DailyLogReturns.get_unique_instance(_log_return_identifier, _startdate, _enddate, _config).add_listener(self)

    def on_indicator_update(self, identifier, daily_log_returns_dt):
        _product = identifier.split('.')[1]
        _date = daily_log_returns_dt[-1][0]
        _log_return = daily_log_returns_dt[-1][1]
        self.daily_log_returns[_product][_date] = _log_return
        
    def compute_todays_log_return(self, date):
        # This is being called from compute_daily_stats
        

    def update_weights(self, date, weights):
        # We don't want to update weights just yet since these are desired weights in future.
        for _product in weights.keys():
            self.rebalance_weights[self.map_product_to_index[_product]] = weights[product]
        self.rebalance_date = date

    # Called by Dispatcher
    def on_end_of_day(self, date):
        self.compute_daily_stats(date)
        _current_dd_log = self.current_dd(self.daily_log_returns)
        self.current_drawdown = abs((exp(_current_dd_log) - 1)* 100)
        #self.current_loss = self.initial_capital - self.value[-1] #TODO
    
    def compute_daily_stats(self, date):
        """Computes the daily stats for the most recent trading day prior to 'date'
        """
        self.date = date
        _logret_today = compute_todays_log_return(date)
        self.daily_log_returns = np.append(self.daily_log_returns, _logret_today)

    # Calculates the current drawdown i.e. the maximum drawdown with end point as the latest return value 
    def current_dd(self, returns):
        if returns.shape[0] < 2:
            return 0.0
        cum_returns = returns.cumsum()
        return -1.0*(max(cum_returns) - cum_returns[-1])
