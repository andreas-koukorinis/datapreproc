from numpy import *
from importlib import import_module
from Algorithm.trade_algorithm import TradeAlgorithm
from Utils.Regular import check_eod, parse_weights
from DailyIndicators.Indicator_List import is_valid_daily_indicator


class MVO(TradeAlgorithm):
    """Perform mean variance optimization"""

    def init(self, _config):
        """Initialize variables with configuration inputs or defaults"""
        self.day = -1
        #Set risk tolerance
        self.risk_tolerance = 0.15
        if _config.has_option('Strategy', 'risk_tolerance'):
            self.risk_tolerance = _config.getfloat('Strategy', 'risk_tolerance')
        #Set rebalance frequency
        self.rebalance_frequency = 1
        if _config.has_option('Parameters', 'rebalance_frequency'):
            self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')
        #Set period
        self.periods = ['21']
        if _config.has_option('Strategy', 'periods'):
            self.periods = _config.get('Strategy', 'periods').split(',')
        #Set signs
        self.signs = parse_weights(_config.get('Strategy', 'signs'))
        #Set indicator
        for product in self.products:
            for period in self.periods:
                indicator_name = 'DailyLogReturns'
                indicator = indicator_name + '.' + product + '.' + period
                if is_valid_daily_indicator(indicator_name):
                    module = import_module('DailyIndicators.' + indicator_name)
                    Indicatorclass = getattr(module, indicator_name)
                    self.daily_indicators[indicator] = Indicatorclass.get_unique_instance(indicator, self.start_date, self.end_date, _config)
