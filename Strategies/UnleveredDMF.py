import sys
import numpy as numpy
from importlib import import_module
from Utils.Regular import check_eod,adjust_file_path_for_home_directory
from DailyIndicators.Indicator_List import is_valid_daily_indicator,get_module_name_from_indicator_name
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from Algorithm.TradeAlgorithm import TradeAlgorithm

class UnleveredDMF( TradeAlgorithm ):
    """Implement a momentum strategy on multiple products.
    The estimated return on each product is a sum of the discretized returns in the past durations.
    For instance if we are provided the TrendIndicator as Trend
    and TrendComputationParameters for fES: 5 63 252
    We will interpret this as we need to create two instances of Trend indicator with arguments 63 and 252
    and recompute them every 5 days.
    On every recomputation day, we will take the sign of the values of the indicators.
    We will sum them up, and divide by the maximum positive score.
    Hence we have a normalized expected return for each product.
    We have an estimate of risk, StdDev for each product.
    Based on Kelly Breiman bet sizing, we aim to take a position propotional to the ( expected return / expected risk )
    in each prodct. These weights are normalized to have a full capital usage at all times in this version,
    hence the name Unlevered.
    """
    
    def init( self, _config ):
        self.day = -1
        self.rebalance_frequency = 1
        if _config.has_option('Parameters', 'rebalance_frequency'):
            self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')
        self.stdev_computation_indicator_name="AverageStdDev"
        self.stdev_computation_history ="63 252"
        self.stdev_computation_interval=5
        self.trend_computation_indicator_name="AverageDiscretizedTrend"
        self.trend_computation_history ="21 63 252"
        self.trend_computation_interval=5

        self.trend_indicator_vec=[]
        self.stdev_indicator_vec=[]
        _modelfilepath="/dev/null"
        if _config.has_option('Strategy','modelfilepath'):
            _modelfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','modelfilepath'))
        self.process_model_file(_modelfilepath, _config)

        self.expected_return_vec=numpy.zeros(len(self.products))
        self.expected_risk_vec=numpy.ones(len(self.products))
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.dmf_weights = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products

    def process_model_file(self, _modelfilepath, _config):
        _model_file_handle = open( _modelfilepath, "r" )
        _map_product_to_stdev_computation_history ={}
        _map_product_to_trend_computation_history ={}
        for _model_line in _model_file_handle:
            # We expect lines like:
            # Default StdDevIndicator AverageStdDev
            # Default StdDevComputationParameters 5 63 
            # Default TrendIndicator AverageDiscretizedTrend
            # Default TrendComputationParameters 5 21 63 252
            # fES TrendComputationParameters 5 63 252
            _model_line_words = _model_line.strip().split(' ')
            if (len(_model_line_words) >= 3):
                if (_model_line_words[0] == 'Default'):
                    if _model_line_words[1] == 'StdDevIndicator':
                        self.stdev_computation_indicator_name=_model_line_words[2]
                    elif _model_line_words[1] == 'StdDevComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) >= 2:
                            self.stdev_computation_interval=int(_computation_words[0])
                            self.stdev_computation_history = ' '.join ( [ str(y) for y in _computation_words[1:] ] )
                    elif _model_line_words[1] == 'TrendIndicator':
                        self.trend_computation_indicator_name=_model_line_words[2]
                    elif _model_line_words[1] == 'TrendComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) >= 2:
                            self.trend_computation_interval=int(_computation_words[0])
                            self.trend_computation_history = ' '.join ( [ str(y) for y in _computation_words[1:] ] )
                else:
                    _product=_model_line_words[0]
                    if _product in self.products:
                        if _model_line_words[1] == 'StdDevComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) >= 2:
                                #set the refreshing interval to the minimum of current and previous values
                                self.stdev_computation_interval=numpy.min(self.stdev_computation_interval,int(_computation_words[0])) 
                                _map_product_to_stdev_computation_history = ' '.join ( [ str(y) for y in _computation_words[1:] ] )
                        elif _model_line_words[1] == 'TrendComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) >= 2:
                                #set the refreshing interval to the minimum of current and previous values
                                self.trend_computation_interval=numpy.min(self.trend_computation_interval,int(_computation_words[0]))
                                _map_product_to_trend_computation_history = ' '.join ( [ str(y) for y in _computation_words[1:] ] )

        if is_valid_daily_indicator(self.stdev_computation_indicator_name):
            _stdev_indicator_module = import_module('DailyIndicators.' + get_module_name_from_indicator_name(self.stdev_computation_indicator_name))
            StdDevIndicatorClass = getattr(_stdev_indicator_module, self.stdev_computation_indicator_name)
        else:
            print ( "stdev_computation_indicator string %s is invalid" %(self.stdev_computation_indicator_name) )
            sys.exit(0)

        if is_valid_daily_indicator(self.trend_computation_indicator_name):
            _trend_indicator_module = import_module('DailyIndicators.' + get_module_name_from_indicator_name(self.trend_computation_indicator_name))
            TrendIndicatorClass = getattr(_trend_indicator_module, self.trend_computation_indicator_name)
        else:
            print ( "stdev_computation_indicator string %s is invalid" %(self.trend_computation_indicator_name) )
            sys.exit(0)

        _stdev_computation_history_vec = self.stdev_computation_history.split(' ')
        _trend_computation_history_vec = self.trend_computation_history.split(' ')
        # We have read the model. Now we need to create the indicators
        for _product in self.products:
            _identifier=self.stdev_computation_indicator_name+'.'+_product+'.'+('.'.join(_stdev_computation_history_vec))
            if _product in _map_product_to_stdev_computation_history:
                _identifier=self.stdev_computation_indicator_name+'.'+_product+'.'+('.'.join(_stdev_computation_history_vec))
            self.stdev_indicator_vec.append(StdDevIndicatorClass.get_unique_instance(_identifier,self.start_date, self.end_date, _config))

            _identifier=self.trend_computation_indicator_name+'.'+_product+'.'+('.'.join(_trend_computation_history_vec))
            if _product in _map_product_to_trend_computation_history:
                _identifier=self.trend_computation_indicator_name+'.'+_product+'.'+('.'.join(_trend_computation_history_vec))
            self.trend_indicator_vec.append(TrendIndicatorClass.get_unique_instance(_identifier,self.start_date, self.end_date, _config))

    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
        
        # If today is the rebalancing day, then use indicators to calculate new positions to take
        if all_eod and(self.day % self.rebalance_frequency == 0):
            _need_to_recompute_dmf_weights = False # By default we don't need to change weights unless some input has changed
            if (self.day % self.stdev_computation_interval) == 0:
                # we need to recompute risk estimate
                for i in xrange(len(self.expected_risk_vec)):
                    if len(self.stdev_indicator_vec[i].indicator_values) >= 1:
                        self.expected_risk_vec[i] = self.stdev_indicator_vec[i].indicator_values[-1]
                _need_to_recompute_dmf_weights = True
            if (self.day % self.trend_computation_interval) == 0:
                # we need to recompute risk estimate
                for i in xrange(len(self.expected_return_vec)):
                    if len(self.trend_indicator_vec[i].indicator_values) >= 1:
                        self.expected_return_vec[i] = self.trend_indicator_vec[i].indicator_values[-1]
                _need_to_recompute_dmf_weights = True

            if _need_to_recompute_dmf_weights:
                self.dmf_weights = self.expected_return_vec/self.expected_risk_vec
                self.dmf_weights = self.dmf_weights/numpy.sum(numpy.abs(self.dmf_weights))
                for _product in self.products:
                    self.map_product_to_weight[_product] = self.dmf_weights[self.map_product_to_index[_product]] # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it
            self.update_positions( events[0]['dt'], self.map_product_to_weight )
        else:
            self.rollover( events[0]['dt'] )
