# cython: profile=True
import sys
import ConfigParser
import numpy
from importlib import import_module
from utils.regular import check_eod, adjust_file_path_for_home_directory
from daily_indicators.indicator_list import is_valid_daily_indicator, get_module_name_from_indicator_name
from daily_indicators.portfolio_utils import make_portfolio_string_from_products
from signals.signal_list import is_valid_signal_name, get_module_name_from_signal_name
from strategies.trade_algorithm import TradeAlgorithm

class AggregatorBELReturn(TradeAlgorithm):
    """Implement an aggregator strategy which combines multiple signals using Best expert learning approach based on past returns(as estimate of future returns) as the utility function
       For example: If the config of the aggregator contains:    signal_configs=test/IVWAS_rb1.cfg,test/IVWAS_rb21.cfg 
                                                                 return_history=126
                                                                 return_computation_interval=21

       **Weight of a singal = exp(past return of signal)/sum_signal_weights

       This aggregator will recompute the relative allocations to the strategies every 1 month based on past 3 month return of the signal
       and assign weights based on the latest rebalanced weights and new relative allocations
       Note that the relative allocations will be positive numbers summing to 1 
    """
    
    def init(self, _config):
        self.day = -1
        self.signals = [] # This is the set of SignalAlgorithm instances
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        if _config.has_option('Strategy', 'signal_configs'):
            _signal_configs = [adjust_file_path_for_home_directory(x) for x in _config.get('Strategy', 'signal_configs').split(',')]
            self.return_history = 126
            if _config.has_option('Strategy', 'return_history'):
                self.return_history = _config.getint('Strategy', 'return_history')
            self.return_computation_interval = 21
            if _config.has_option('Strategy', 'return_computation_interval'):
                self.return_computation_interval = _config.getint('Strategy', 'return_computation_interval')
            self.last_day_return_computed = 0
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

        self.rebalance_frequency = 100000000 # Initialize to some high value
        for _signal in self.signals:
            self.rebalance_frequency = min(self.rebalance_frequency, _signal.rebalance_frequency) # strategy rebalances at the frequency of minimum rebalance frequency signal
        self.signal_allocations = numpy.array([1.0/float(len(_signal_configs))] * len(_signal_configs)) # Equally weighted by default

    def update_signal_allocations_using_bel_return(self, _signals):
        for i in range(len(_signals)):
            _this_signal_return = _signals[i].simple_performance_tracker.compute_historical_return(self.return_history)
            self.signal_allocations[i] = _this_signal_return
        self.signal_allocations = numpy.exp(self.signal_allocations) # Best expert learning assigns weights as exp of utility function (here expected returns)
        self.signal_allocations = self.signal_allocations/numpy.sum(self.signal_allocations) # Normalize, dont need abs since all values are positive
 
    def on_events_update(self, events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number

        # Update signal allocations based on return
        if self.day >= self.last_day_return_computed + self.return_computation_interval:
            self.update_signal_allocations_using_bel_return(self.signals)
            self.last_day_return_computed = self.day

        if self.day % self.rebalance_frequency == 0:
            # today is a rebalancing day for the minimum rebalance frequency signal
            for _product in self.products: # Since each signal can trade on different products, we are not using numpy array here
                self.map_product_to_weight[_product] = 0.0
                for i in range(len(self.signals)):
                    self.map_product_to_weight[_product] += self.signals[i].weights.get(_product, 0.0) * self.signal_allocations[i]
            self.update_positions(events[0]['dt'], self.map_product_to_weight)
        else:
            self.rollover(events[0]['dt'])
