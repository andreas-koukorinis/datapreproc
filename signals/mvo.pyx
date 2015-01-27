# cython: profile=True
""" Implementation of Rolling Mean Variance Optimization in the lines of Modern Portfolio Theory."""

import sys
from importlib import import_module
import numpy
from signals.signal_algorithm import SignalAlgorithm
from utils.regular import check_eod, efficient_frontier, adjust_file_path_for_home_directory, adjust_to_desired_l1norm_range
from daily_indicators.indicator_list import is_valid_daily_indicator, get_module_name_from_indicator_name
from daily_indicators.portfolio_utils import make_portfolio_string_from_products
from daily_indicators.correlation_log_returns import CorrelationLogReturns

class MeanVarianceOptimization(SignalAlgorithm):
    """Implementation of the Rolling Mean Variance Optimization.
    Weights are assigned to minimize the following quantity:
    (expected risk) - (risk tolerance) * (expected returns)

    Items read from config:
        risk_tolerance: constant in the Optimization function
        max_allocation: maximum allocation to one asset
        exp_return_computation_inteval: days after which expected returns is recalculated
        exp_return_computation_history: days over which expected returns is calculated
        stddev_computation_interval: days after which std dev is recalculated
        stddev_computation_history: days over which std dev is calculated
        correlation_computation_interval: days after which correlaion matrix is recalculated
        correlation_computation_history: days over which correlaion matrix is calculated
        stdev_computation_indicator : The indicator to use to compute the estimate of ex-ante standard deviation
    """
    def init(self, _config):
        """Initialize variables with configuration inputs or defaults"""
        self.day = -1
        # Set computational variables
        self.last_date_correlation_matrix_computed = -100000
        self.last_date_stdev_computed = -100000
        self.last_date_exp_return_computed = -100000
        self.exp_log_returns = numpy.array([0.0]*len(self.products))
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products])  # map from product to weight, which will be passed downstream
        self.weights = numpy.array([0.0]*len(self.products))  # these are the weights, with products occuring in the same order as the order in self.products
        self.stdev_logret = numpy.array([1.0]*len(self.products))  # these are the stddev values, with products occuring in the same order as the order in self.products
        self.stdev_indicator_vec = []
        self.exp_return_indicator_vec = {}
        # Create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = numpy.eye(len(self.products))
        
        _paramfilepath="/dev/null"
        if _config.has_option('Parameters', 'paramfilepath'):
            _paramfilepath=adjust_file_path_for_home_directory(_config.get('Parameters', 'paramfilepath'))
        self.process_param_file(_paramfilepath, _config)

        _modelfilepath="/dev/null"
        if _config.has_option('Strategy','modelfilepath'):
            _modelfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','modelfilepath'))
        self.process_model_file(_modelfilepath, _config)

    def process_param_file(self, _paramfilepath, _config):
        super(MeanVarianceOptimization, self).process_param_file(_paramfilepath, _config)
    
    def process_model_file(self, _modelfilepath, _config):
        _model_file_handle = open( _modelfilepath, "r" )
        _map_product_to_stdev_computation_history = {}
        _map_product_to_exp_return_computation_history = {}
        
        # Set target risk
        self.target_risk = 5.0
        # Set risk tolerance
        self.risk_tolerance = 0.015
        # Set maximum allocation
        self.max_allocation = 0.5
        # Set expected returns indicator
        self.exp_return_indicator = 'ExpectedReturns'
        # Set expected return computation history
        self.exp_return_computation_history = ['252']
        # Set expected return computation inteval
        self.exp_return_computation_interval = 21
        # Set StdDev indicator name
        self.stddev_computation_indicator = 'StdDev'
        # Set StdDev computation history
        self.stddev_computation_history = ['252']
        # Set StdDev computation interval
        self.stddev_computation_interval = 21
        # Set Correlation computation indicator
        self.correlation_computation_indicator = 'CorrelationLogReturns'
        # Set correlation computation hisory
        self.correlation_computation_history = 252
        # Set correlation computation interval
        self.correlation_computation_interval = 21

        for _model_line in _model_file_handle:
            # We expect lines like:
            # Default StdDevIndicator StdDev
            # Default StdDevComputationParameters 252 21
            # Default ExpectedReturnsIndicator ExpectedReturns
            # Default ExpectedReturnsComputationParameters 252 21
            _model_line_words = _model_line.strip().split(' ')
            if (len(_model_line_words) >= 3):
                if (_model_line_words[0] == 'Default'):
                    if _model_line_words[1] == 'StdDevIndicator':
                        self.stddev_computation_indicator = _model_line_words[2]
                    elif _model_line_words[1] == 'StdDevComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) == 2:
                            self.stddev_computation_history = [_computation_words[0]]
                            self.stddev_computation_interval = int(_computation_words[1])
                    elif _model_line_words[1] == 'ExpectedReturnsIndicator':
                        self.exp_return_indicator = _model_line_words[2]
                    elif _model_line_words[1] == 'ExpectedReturnsComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) == 2:
                            self.exp_return_computation_history = [_computation_words[0]]
                            self.exp_return_computation_interval = int(_computation_words[1])
                    elif _model_line_words[1] == 'CorrelationLogReturnsIndicator':
                        self.correlation_computation_indicator = _model_line_words[2]
                    elif _model_line_words[1] == 'CorrelationLogReturnsComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) == 2:
                            self.correlation_computation_history = int(_computation_words[0])
                            self.correlation_computation_interval = int(_computation_words[1])
                    elif _model_line_words[1] == 'RiskTolerance':
                        self.risk_tolerance = float(_model_line_words[2])
                    elif _model_line_words[1] == 'MaxAllocation':
                        self.max_allocation = float(_model_line_words[2])
                else:
                    _product=_model_line_words[0]
                    if _product in self.products:
                        if _model_line_words[1] == 'StdDevComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) == 2:
                                # set the refreshing interval to the minimum of current and previous values
                                _map_product_to_stdev_computation_history[_product] = _computation_words[0]
                                self.stdev_computation_interval = min(self.stdev_computation_interval, int(_computation_words[1])) 
                        elif _model_line_words[1] == 'ExpectedReturnsComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) == 2:
                                #set the refreshing interval to the minimum of current and previous values
                                _map_product_to_exp_return_computation_history[_product] = _computation_words[0]
                                self.exp_return_computation_interval = min(self.exp_return_computation_interval, int(_computation_words[1]))
            elif len(_model_line_words) == 2:
                if _model_line_words[0] == 'TargetRisk':
                    self.target_risk = float(_model_line_words[1])

        # Set indicator for ExpectedReturns
        if is_valid_daily_indicator(self.exp_return_indicator):
            module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.exp_return_indicator))
            Indicatorclass = getattr(module, self.exp_return_indicator)
            for product in self.products:
                indicator = self.exp_return_indicator + '.' + product + '.' + '.'.join(_map_product_to_exp_return_computation_history.get(product, self.exp_return_computation_history))
                self.exp_return_indicator_vec[product] = Indicatorclass.get_unique_instance(indicator, self.start_date, self.end_date, _config)
        else:
            print "Expected returns computation indicator %s invalid!" % self.exp_return_indicator
            sys.exit(0)
        
        # Set indicator for stddev_computation_indicator
        if is_valid_daily_indicator(self.stddev_computation_indicator):
            module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.stddev_computation_indicator))
            StdDevIndicatorClass = getattr(module, self.stddev_computation_indicator)
            for _product in self.products:
               _identifier = self.stddev_computation_indicator + '.' + _product + '.' + '.'.join(_map_product_to_stdev_computation_history.get(product, self.stddev_computation_history))
               self.stdev_indicator_vec.append(StdDevIndicatorClass.get_unique_instance(_identifier, self.start_date, self.end_date, _config))
        else:
            print "Stdev computation indicator %s invalid!" % self.stddev_computation_indicator
            sys.exit(0)
        
        # Set correlation computation indicator
        if is_valid_daily_indicator(self.correlation_computation_indicator):
            _portfolio_string = make_portfolio_string_from_products(self.products)  # this allows us to pass a portfolio to the CorrelationLogReturns indicator.
            # TODO Should we change the design of passing arguments to the indicators from a '.' concatenated list to a variable argument set?
            self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance(self.correlation_computation_indicator + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config)
        else:
            print "Correlation computation indicator %s invalid!" % self.correlation_computation_indicator
            sys.exit(0)    

       
    def on_events_update(self, events):
        """Implementation of Mean Variance Optimization in the lines of Modern Portfolio Theory.
        It is different from MPT in the sense that the weights can be positive and negative.
        The sum of absolute value of weights is limited by the maximum leverage which is given as input.
        It minimizes the utility function: (risk of portfolio) -  (risk tolerance) * (expected returns).
        Maximum allocaion limit enforces diversification in portfolio.
        More details of implementation in efficient_frontier() function in Utils.Regular
        """
        all_eod = check_eod(events)  # Check if all events are ENDOFDAY
        if all_eod:
            self.day += 1  # Track the current day number
        
        if all_eod:
            _need_to_recompute_weights = False  # By default we don't need to change weights unless some input has changed
            if self.day >= (self.last_date_correlation_matrix_computed + self.correlation_computation_interval):
                # we need to recompute the correlation matrix
                # this command will not do anything if the values have been already computed. else it will
                self.logret_correlation_matrix = self.correlation_computation_indicator.get_correlation_matrix()
                # TODO Add tests here for the correlation matrix to make sense.
                # If it fails, do not overwrite previous values, or throw an error
                _need_to_recompute_weights = True
                self.last_date_correlation_matrix_computed = self.day

            if self.day >= (self.last_date_stdev_computed + self.stddev_computation_interval):
                # Get the stdev values from the stdev indicators
                for i in xrange(len(self.stdev_logret)):
                    self.stdev_logret[i] = max(0.000001, self.stdev_indicator_vec[i].get_stdev()) # a max with 1% is just to not have divide by 0 problems.
                    # TODO should not accessing an array without checking the length!
                    # TODO should add some sanity checks before overwriting previous value.
                    # TODO we can make tests here that the module needs to pass.
                _need_to_recompute_weights = True
                self.last_date_stdev_computed = self.day

            if self.day >= (self.last_date_exp_return_computed + self.exp_return_computation_interval):
                # Get the expected log return values values from the expected returns indicators
                for _product in self.products:
                    self.exp_log_returns[self.map_product_to_index[_product]] =  self.exp_return_indicator_vec[_product].values[1]
                _need_to_recompute_weights = True
                self.last_date_exp_return_computed = self.day

            if _need_to_recompute_weights:
                # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * numpy.outer(self.stdev_logret, self.stdev_logret)  # we should probably do it when either self.stdev_logret or _correlation_matrix has been updated

                # Recompute weights
                self.weights = efficient_frontier(self.exp_log_returns, _cov_mat, self.maximum_leverage, self.risk_tolerance, self.max_allocation)

                # In the following steps we resize the portfolio to the target risk level.
                # We have just used stdev as the measure of risk here since it is simple.
                # TODO improve risk calculation
                _annualized_stdev_of_portfolio = 100.0*(numpy.exp(numpy.sqrt(252.0 * (numpy.asmatrix(self.weights) * numpy.asmatrix(_cov_mat) * numpy.asmatrix(self.weights).T))[0, 0]) - 1)
                self.weights = self.weights*(self.target_risk/_annualized_stdev_of_portfolio)
                self.weights = adjust_to_desired_l1norm_range (self.weights, self.minimum_leverage, self.maximum_leverage)   

                for _product in self.products:
                    self.map_product_to_weight[_product] = self.weights[self.map_product_to_index[_product]]  # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            if (self.day - self.last_rebalanced_day >= self.rebalance_frequency) or _need_to_recompute_weights:
                # if either weights have changed or it is a rebalancing day, then ask execlogic to update weights
                # TODO{gchak} change rebalancing from days to magnitude of divergence from weights
                # so change the above to
                # if sum ( abs ( desired weights - current weights ) ) > threshold, then
                # update_positions
                self.update_positions(events[0]['dt'], self.map_product_to_weight)
                self.last_rebalanced_day = self.day
            else:
                self.rollover(events[0]['dt'])
