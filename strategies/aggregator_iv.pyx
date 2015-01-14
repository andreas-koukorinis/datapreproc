import sys
import ConfigParser
import numpy
from importlib import import_module
from utils.regular import check_eod, adjust_file_path_for_home_directory, is_float_zero
from daily_indicators.indicator_list import is_valid_daily_indicator, get_module_name_from_indicator_name
from daily_indicators.portfolio_utils import make_portfolio_string_from_products
from signals.signal_list import is_valid_signal_name, get_module_name_from_signal_name
from strategies.trade_algorithm import TradeAlgorithm

class AggregatorIV(TradeAlgorithm):
    """Implement an aggregator strategy which combines multiple signals using IVAS
       For example: If the config of the aggregator contains:    signal_configs=test/IVWAS_rb1.cfg,test/IVWAS_rb21.cfg 
                                                                 volatility_history=63
                                                                 volatility_computation_interval=126
       This aggregator will recompute the relative allocations to the strategies every 6 months based on past 3 month volatility
       and assign weights based on the latest rebalanced weights and new relative allocations
       Note that the relative allocations will be positive numbers summing to 1 
    """
    
    def init(self, _config):
        self.day = -1
        self.signals = [] # This is the set of SignalAlgorithm instances
        if _config.has_option('Strategy', 'signal_configs'):
            _signal_configs = [adjust_file_path_for_home_directory(x) for x in _config.get('Strategy', 'signal_configs').split(',')]
            self.volatility_history = 63
            if _config.has_option('Strategy', 'volatility_history'):
                self.volatility_history = _config.getint('Strategy', 'volatility_history')
            self.volatility_computation_interval = 63
            if _config.has_option('Strategy', 'volatility_computation_interval'):
                self.volatility_computation_interval = _config.getint('Strategy', 'volatility_computation_interval')
            self.last_day_volatility_computed = 0
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
        else:
            sys.exit('Strategy::signal_configs is needed in the config file')

        if len(self.signals) < 1:
            sys.exit('No SignalAlgorithm instances were created. Hence exiting')

        # past_relative_contribution is needed in updating weights, since the rebalancing frequency of different SignalAlgorithm instances could be different.
        self.past_relative_contribution = []
        for i in range(len(self.signals)):
            self.past_relative_contribution.append(dict([(_product, 0.0) for _product in self.products]))
        self.signal_allocations = numpy.array([1.0/float(len(_signal_configs))] * len(_signal_configs)) # Equally weighted by default

    def update_signal_allocations_using_volatility(self, _signals):
        for i in range(len(_signals)):
            _this_signal_log_ret_stdev = _signals[i].simple_performance_tracker.compute_historical_volatility(self.volatility_history)
            self.signal_allocations[i] = 1.0/_this_signal_log_ret_stdev
        self.signal_allocations = self.signal_allocations/sum(self.signal_allocations) # Normalize, dont need abs since all values are positive

    def update_past_relative_contribution(self, _new_signal_contributions, _new_portfolio_weights, _new_portfolio_abs_weights):
        for i in range(len(_new_signal_contributions)):
            for _product in self.products:
                if is_float_zero(_new_portfolio_abs_weights[_product]):
                    self.past_relative_contribution[i][_product] = 0
                elif (not is_float_zero(_new_portfolio_abs_weights[_product])) and is_float_zero(_new_portfolio_weights[_product]):
                    self.past_relative_contribution[i][_product] = _new_signal_contributions[i][_product]
                else:
                    self.past_relative_contribution[i][_product] = _new_signal_contributions[i][_product]/_new_portfolio_weights[_product]

    def get_new_portfolio_weights(self, _signal_rebalancing_day, _current_portfolio_weights, _signals):
        _new_signal_contributions = []
        _new_portfolio_weights = dict([(_product, 0.0) for _product in self.products])
        _new_portfolio_abs_weights = dict([(_product, 0.0) for _product in self.products])
        if self.day >= self.last_day_volatility_computed + self.volatility_computation_interval:
            self.update_signal_allocations_using_volatility(self.signals)
            self.last_day_volatility_computed = self.day
            _signal_rebalancing_day = [True] * len(_signals) # Treat the day when allocations are changed as the rebalacing day for each signal
            #print ( self.day, self.signal_allocations )

        for i in range(len(_signals)):
            _new_signal_contributions.append(dict([(_product, 0.0) for _product in self.products]))
            if _signal_rebalancing_day[i]:
                for _product in self.products:
                    _new_signal_contributions[i][_product] = self.signal_allocations[i] * _signals[i].weights.get(_product, 0.0)
                    _new_portfolio_weights[_product] += _new_signal_contributions[i][_product]
                    _new_portfolio_abs_weights[_product] += abs(_new_signal_contributions[i][_product])
            else:
                for _product in self.products:
                    if is_float_zero(_current_portfolio_weights[_product]) and (not is_float_zero(self.past_relative_contribution[i][_product])):
                        _new_signal_contributions[i][_product] = self.past_relative_contribution[i][_product]
                    else:
                        _new_signal_contributions[i][_product] = _current_portfolio_weights[_product] * self.past_relative_contribution[i][_product]
                    _new_portfolio_weights[_product] += _new_signal_contributions[i][_product]
                    _new_portfolio_abs_weights[_product] += abs(_new_signal_contributions[i][_product])
        self.update_past_relative_contribution(_new_signal_contributions, _new_portfolio_weights, _new_portfolio_abs_weights)
        return _new_portfolio_weights
         
    def on_events_update(self, events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
        _signal_rebalancing_day = [False] * len(self.signals)
        _is_rebalancing_day = False
        for i in range(len(self.signals)):
            if self.day % self.signals[i].rebalance_frequency == 0:
                _signal_rebalancing_day[i] = True
                _is_rebalancing_day = True
        if _is_rebalancing_day:
            _current_portfolio_weights = self.get_current_portfolio_weights(events[0]['dt'].date())
            _new_weights = self.get_new_portfolio_weights(_signal_rebalancing_day, _current_portfolio_weights, self.signals)
            self.update_positions(events[0]['dt'], _new_weights)
        else:
            self.rollover(events[0]['dt'])
