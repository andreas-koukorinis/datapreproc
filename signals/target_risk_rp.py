import sys
import numpy
from importlib import import_module
from Algorithm.signal_algorithm import SignalAlgorithm
from Utils.Regular import check_eod, parse_weights
from DailyIndicators.Indicator_List import is_valid_daily_indicator

class TargetRiskRP(SignalAlgorithm):

    def init(self, _config):
        self.day=-1
        self.target_risk = 10.0
        if _config.has_option('Strategy', 'target_risk'):
            self.target_risk = _config.getfloat('Strategy', 'target_risk')
        self.rebalance_frequency = 1
        if _config.has_option('Parameters', 'rebalance_frequency'):
            self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')
        self.periods = ['21']
        if _config.has_option('Strategy','periods'):
            self.periods = _config.get('Strategy','periods').split(',') 
        self.signs = parse_weights(_config.get('Strategy', 'signs'))
        for product in self.products:
            for period in self.periods:
                indicator_name = 'StdDev'
                indicator = indicator_name + '.' + product + '.' + period
                if is_valid_daily_indicator(indicator_name):
                    module = import_module('DailyIndicators.' + indicator_name)
                    Indicatorclass = getattr(module, indicator_name)
                    self.daily_indicators[indicator] = Indicatorclass.get_unique_instance(indicator, self.start_date, self.end_date, _config)

    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number

        # If today is the rebalancing day,then use indicators to calculate new positions to take
        if all_eod and self.day % self.rebalance_frequency == 0 :
            # Calculate weights to assign to each product using indicators
            for product in self.products:
                target_risk_per_product = self.target_risk/len(self.products)
                _stddev = 0.0
                for period in self.periods:
                    val = self.daily_indicators[ 'StdDev.' + product + '.' + period ].values[1]         
                    _stddev += (numpy.exp(numpy.sqrt(252.0)*val) - 1)*100.0         
                vol_product = _stddev/float(len(self.periods))
                self.weights[product] = self.signs[product]*target_risk_per_product/vol_product
