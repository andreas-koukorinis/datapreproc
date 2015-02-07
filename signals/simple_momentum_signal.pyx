# cython: profile=True
import sys
import numpy
from importlib import import_module
import math
from utils.regular import check_eod, adjust_file_path_for_home_directory, is_float_zero, parse_weights, adjust_to_desired_l1norm_range
from daily_indicators.indicator_list import is_valid_daily_indicator,get_module_name_from_indicator_name
from daily_indicators.portfolio_utils import make_portfolio_string_from_products
from signals.signal_algorithm import SignalAlgorithm
from daily_indicators.correlation_log_returns import CorrelationLogReturns

class SimpleMomentumSignal( SignalAlgorithm ):
    """Implement a simple momentum strategy on multiple products.
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
        # Set target risk
        self.target_risk = 10.0
        self.stdev_computation_indicator_name = "AverageStdDev"
        self.stdev_computation_history = ['63', '252']
        self.stdev_computation_interval = 5
        self.trend_computation_indicator_name = "AverageDiscretizedTrend"
        self.trend_computation_history = ['21', '63', '252']
        self.trend_computation_interval = 5
         # Set Correlation computation indicator
        self.correlation_computation_indicator = 'CorrelationLogReturns'
        # Set correlation computation hisory
        self.correlation_computation_history = 252
        # Set correlation computation interval
        self.correlation_computation_interval = 21
        # create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = numpy.eye(len(self.products))


        self.trend_indicator_vec = []
        self.stdev_indicator_vec = []
        self.expected_return_vec = numpy.zeros(len(self.products))
        self.expected_risk_vec = numpy.ones(len(self.products))
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.dmf_weights = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products

        _paramfilepath="/dev/null"
        if _config.has_option('Parameters', 'paramfilepath'):
            _paramfilepath=adjust_file_path_for_home_directory(_config.get('Parameters', 'paramfilepath'))
        self.process_param_file(_paramfilepath, _config)

        _modelfilepath="/dev/null"
        if _config.has_option('Strategy','modelfilepath'):
            _modelfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','modelfilepath'))
        self.process_model_file(_modelfilepath, _config)
        self.target_risk = math.sqrt((math.log((self.target_risk/100.0) + 1)**2)/252.0)

    def process_param_file(self, _paramfilepath, _config):
        super(SimpleMomentumSignal, self).process_param_file(_paramfilepath, _config)

    
    def process_model_file(self, _modelfilepath, _config):
        _model_file_handle = open( _modelfilepath, "r" )
        _map_product_to_stdev_computation_history = {}
        _map_product_to_trend_computation_history = {}
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
                            self.stdev_computation_interval = int(_computation_words[0])
                            self.stdev_computation_history = _computation_words[1:]
                    elif _model_line_words[1] == 'TrendIndicator':
                        self.trend_computation_indicator_name=_model_line_words[2]
                    elif _model_line_words[1] == 'TrendComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) >= 2:
                            self.trend_computation_interval = int(_computation_words[0])
                            self.trend_computation_history = _computation_words[1:]
                    elif _model_line_words[1] == 'CorrelationLogReturnsIndicator':
                        self.correlation_computation_indicator = _model_line_words[2]
                    elif _model_line_words[1] == 'CorrelationLogReturnsComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) == 2:
                            self.correlation_computation_history = int(_computation_words[0])
                            self.correlation_computation_interval = int(_computation_words[1])
                else:
                    _product=_model_line_words[0]
                    if _product in self.products:
                        if _model_line_words[1] == 'StdDevComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) >= 2:
                                #set the refreshing interval to the minimum of current and previous values
                                self.stdev_computation_interval=numpy.min(self.stdev_computation_interval,int(_computation_words[0])) 
                                _map_product_to_stdev_computation_history[_product] = _computation_words[1:]
                        elif _model_line_words[1] == 'TrendComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) >= 2:
                                #set the refreshing interval to the minimum of current and previous values
                                self.trend_computation_interval=numpy.min(self.trend_computation_interval,int(_computation_words[0]))
                                _map_product_to_trend_computation_history = _computation_words[1:]
            elif len(_model_line_words) == 2:
                if _model_line_words[0] == 'TargetRisk':
                    self.target_risk = float(_model_line_words[1])

        if is_valid_daily_indicator(self.stdev_computation_indicator_name):
            _stdev_indicator_module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.stdev_computation_indicator_name))
            StdDevIndicatorClass = getattr(_stdev_indicator_module, self.stdev_computation_indicator_name)
        else:
            print ( "stdev_computation_indicator string %s is invalid" %(self.stdev_computation_indicator_name) )
            sys.exit(0)

        if is_valid_daily_indicator(self.trend_computation_indicator_name):
            _trend_indicator_module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.trend_computation_indicator_name))
            TrendIndicatorClass = getattr(_trend_indicator_module, self.trend_computation_indicator_name)
        else:
            print ( "stdev_computation_indicator string %s is invalid" %(self.trend_computation_indicator_name) )
            sys.exit(0)

        # We have read the model. Now we need to create the indicators
        for _product in self.products:
            _identifier = self.stdev_computation_indicator_name + '.' + _product + '.' + '.'.join(_map_product_to_stdev_computation_history.get(_product, self.stdev_computation_history))
            self.stdev_indicator_vec.append(StdDevIndicatorClass.get_unique_instance(_identifier,self.start_date, self.end_date, _config))

            _identifier = self.trend_computation_indicator_name + '.' + _product + '.' + '.'.join(_map_product_to_stdev_computation_history.get(_product, self.stdev_computation_history))
            self.trend_indicator_vec.append(TrendIndicatorClass.get_unique_instance(_identifier,self.start_date, self.end_date, _config))

        # Set correlation computation indicator
        if is_valid_daily_indicator(self.correlation_computation_indicator):
            _portfolio_string = make_portfolio_string_from_products(self.products)  # this allows us to pass a portfolio to the CorrelationLogReturns indicator.
            # TODO Should we change the design of passing arguments to the indicators from a '.' concatenated list to a variable argument set?
            self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance(self.correlation_computation_indicator + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config)
        else:
            print "Correlation computation indicator %s invalid!" % self.correlation_computation_indicator
            sys.exit(0)


    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
        
        if all_eod:
            _need_to_recompute_dmf_weights = False # By default we don't need to change weights unless some input has changed
            if self.day % self.correlation_computation_interval == 0:
                # we need to recompute the correlation matrix
                self.logret_correlation_matrix = self.correlation_computation_indicator.get_correlation_matrix() # this command will not do anything if the values have been already computed. else it will
                # TODO Add tests here for the correlation matrix to make sense.
                # If it fails, do not overwrite previous values, or throw an error
                _need_to_recompute_erc_weights = True
                self.last_date_correlation_matrix_computed = self.day
            if (self.day % self.stdev_computation_interval) == 0:
                # we need to recompute risk estimate
                for i in xrange(len(self.expected_risk_vec)):
                    self.expected_risk_vec[i] = max(0.000001, self.stdev_indicator_vec[i].get_stdev()) # a max with 1% is just to not have divide by 0 problems.
                _need_to_recompute_dmf_weights = True
            if (self.day % self.trend_computation_interval) == 0:
                # we need to recompute expected return estimate from trend 
                for i in xrange(len(self.expected_return_vec)):
                    self.expected_return_vec[i] = 0.002 * self.trend_indicator_vec[i].get_trend()
                _need_to_recompute_dmf_weights = True

            if _need_to_recompute_dmf_weights:
                # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * numpy.outer(self.expected_risk_vec, self.expected_risk_vec) # we should probably do it when either self.stdev_logret or _correlation_matrix has been updated

                self.dmf_weights = self.expected_return_vec/self.expected_risk_vec
                _annualized_stdev_of_portfolio = math.sqrt((numpy.asmatrix(self.dmf_weights) * numpy.asmatrix(_cov_mat) * numpy.asmatrix(self.dmf_weights).T)[0, 0])
                self.dmf_weights= self.dmf_weights*(self.target_risk/_annualized_stdev_of_portfolio)
                self.dmf_weights = adjust_to_desired_l1norm_range (self.dmf_weights, self.minimum_leverage, self.maximum_leverage)

                for _product in self.products:
                    self.map_product_to_weight[_product] = self.dmf_weights[self.map_product_to_index[_product]] # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            if (self.day - self.last_rebalanced_day >= self.rebalance_frequency) or _need_to_recompute_dmf_weights:
                # if either weights have changed or it is a rebalancing day, then ask execlogic to update weights
                # TODO{gchak} change rebalancing from days to magnitude of divergence from weights
                # so change the above to
                # if sum ( abs ( desired weights - current weights ) ) > threshold, then
                # update_positions
                self.update_positions( events[0]['dt'], self.map_product_to_weight )
                self.last_rebalanced_day = self.day
            else:
                self.rollover( events[0]['dt'] )
