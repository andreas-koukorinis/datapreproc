import sys
import ConfigParser
import numpy as numpy
from importlib import import_module
from Utils.Regular import check_eod,adjust_file_path_for_home_directory
from DailyIndicators.Indicator_List import is_valid_daily_indicator,get_module_name_from_indicator_name
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from signals.signal_list import is_valid_signal_name, get_module_name_from_signal_name
from Algorithm.TradeAlgorithm import TradeAlgorithm

class AggregatorCWAS(TradeAlgorithm):
    """Implement an aggregator strategy which combines multiple signals using CWAS
       For example: If the config of the aggregator contains:    signals=test/IVWAS_rb1.cfg,test/IVWAS_rb21.cfg 
                                                                 signal_allocations=0.6,0.4
       Then the aggregator places 60% weight on daily rebalanced IVWAS and 40% weight on monthly rebalanced IVWAS
       Since the signals can be leveraged, we may end up having a net leverage > 1 even in the 0.6, 0.4 case 
    """
    
    def init(self, _config):
        self.day = -1
        if _config.has_option('Strategy', 'signal_configs'):
            _signal_configs =  _config.get('Strategy', 'signal_configs').split(',')
            self.signals = []
            for _config_name in _signal_configs:
                _signal_config = ConfigParser.ConfigParser()
                _signal_config.readfp(open(_config_name, 'r'))
                _signalfile = _signal_config.get ('Strategy', 'name')
                if not(is_valid_signal_name(_signalfile)):
                    sys.exit("Cannot proceed with invalid Signal name")
                _signal_module_name = get_module_name_from_signal_name(_signalfile)
                SignalLogic = getattr(import_module('signals.' + _signal_module_name), _signalfile)
                _signal_instance = SignalLogic(self.all_products, self.start_date, self.end_date, _signal_config, _config)
                self.signals.append(_signal_instance)
            self.past_relative_contribution = []
            for i in range(len(self.signals)):
                self.past_relative_contribution.append(dict([(_product, 0.0) for _product in self.products]))
        else:
            sys.exit('Atleast one signal config expected')
        self.signal_allocations = numpy.array([1.0/float(len(_signal_configs))] * len(_signal_configs)) # Equally weighted by default
        if _config.has_option('Strategy', 'signal_allocations'):
            _signal_allocations = _config.get('Strategy', 'signal_allocations').split(',')
            self.signal_allocations = numpy.array([float(x) for x in _signal_allocations]) 

    def update_past_relative_contribution(self, _new_signal_contributions, _new_portfolio_weights):
        for i in range(len(_new_signal_contributions)):
            for _product in self.products:
                if _new_portfolio_weights[_product] == 0:
                    self.past_relative_contribution[i][_product] = 0
                else:
                    self.past_relative_contribution[i][_product] = _new_signal_contributions[i][_product]/_new_portfolio_weights[_product]

    def get_new_portfolio_weights(self, _signal_rebalancing_day, _current_portfolio_weights, _signals, _signal_allocations):
        _new_signal_contributions = []
        _new_portfolio_weights = dict([(_product, 0.0) for _product in self.products])
        for i in range(len(_signals)):
            _new_signal_contributions.append(dict([(_product, 0.0) for _product in self.products]))
            if _signal_rebalancing_day[i]:
                for _product in self.products:
                    _new_signal_contributions[i][_product] = _signal_allocations[i] * _signals[i].weights.get(_product, 0.0)
                    _new_portfolio_weights[_product] += _new_signal_contributions[i][_product]
            else:
                for _product in self.products:
                    _new_signal_contributions[i], _current_portfolio_weights, self.past_relative_contribution[i]
                    _new_signal_contributions[i][_product] = _current_portfolio_weights[_product] * self.past_relative_contribution[i][_product]
                    _new_portfolio_weights[_product] += _new_signal_contributions[i][_product]
        #print self.signals[0].weights, self.signals[1].weights, _current_portfolio_weights, _new_portfolio_weights, self.past_relative_contribution, _new_signal_contributions
        self.update_past_relative_contribution(_new_signal_contributions, _new_portfolio_weights) # TODO should not call here
        return _new_portfolio_weights
         
    def on_events_update(self, events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
        _signal_rebalancing_day = [False] * len(self.signals)
        _is_rebalancing_day = False
        #print events[0]['dt'].date()
        for i in range(len(self.signals)):
            if self.day % self.signals[i].rebalance_frequency == 0:
                _signal_rebalancing_day[i] = True
                _is_rebalancing_day = True
        if _is_rebalancing_day:
            _current_portfolio_weights = self.get_current_portfolio_weights(events[0]['dt'].date())
            _new_weights = self.get_new_portfolio_weights(_signal_rebalancing_day, _current_portfolio_weights, self.signals, self.signal_allocations)
            #print events[0]['dt'].date(), self.signals[0].weights, self.signals[1].weights, _current_portfolio_weights, _new_weights, self.past_relative_contribution
            self.update_positions(events[0]['dt'], _new_weights)
        else:
            self.rollover(events[0]['dt'])
