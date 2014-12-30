""" Implementation of Rolling Mean Variance Optimization in the lines of Modern Portfolio Theory."""

import sys
from importlib import import_module
import numpy
from signals.signal_algorithm import SignalAlgorithm
from Utils.Regular import check_eod, efficient_frontier, adjust_file_path_for_home_directory
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from DailyIndicators.CorrelationLogReturns import CorrelationLogReturns


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
        _paramfilepath = "/dev/null"
        if _config.has_option('Parameters', 'paramfilepath'):
            _paramfilepath = adjust_file_path_for_home_directory(_config.get('Parameters', 'paramfilepath'))
        self.process_param_file(_paramfilepath, _config)

        self.day = -1
        # Set risk tolerance
        self.risk_tolerance = 0.015
        if _config.has_option('Strategy', 'risk_tolerance'):
            self.risk_tolerance = _config.getfloat('Strategy', 'risk_tolerance')
        # Set maximum allocation
        self.max_allocation = 0.5
        if _config.has_option('Strategy', 'max_allocation'):
            self.max_allocation = _config.getfloat('Strategy', 'max_allocation')
        # Set expected return computation history
        self.exp_return_computation_history = 252
        if _config.has_option('Strategy', 'exp_return_computation_history'):
            self.exp_return_computation_history = _config.getint('Strategy', 'exp_return_computation_history')
        # Set expected return computation inteval
        self.exp_return_computation_interval = self.exp_return_computation_history / 5
        if _config.has_option('Strategy', 'exp_return_computation_interval'):
            self.exp_return_computation_history = _config.getint('Strategy', 'exp_return_computation_interval')
        # Set StdDev indicator name
        self.stddev_computation_indicator = 'StdDev'
        if _config.has_option('Strategy', 'stddev_computation_indicator'):
            self.stddev_computation_indicator = _config.get('Strategy', 'stddev_computation_indicator')
        # Set StdDev computation history
        self.stddev_computation_history = 252
        if _config.has_option('Strategy', 'stddev_computation_history'):
            self.stddev_computation_history = max(2, _config.getint('Strategy', 'stddev_computation_history'))
        # Set StdDev computation interval
        if _config.has_option('Strategy', 'stddev_computation_interval'):
            self.stddev_computation_interval = max(1, _config.getint('Strategy', 'stddev_computation_interval'))
        else:
            self.stddev_computation_interval = max(1, self.stddev_computation_history/5)
        # Set correlation computation hisory
        self.correlation_computation_history = 252
        if _config.has_option('Strategy', 'correlation_computation_history'):
            self.correlation_computation_history = max(2, _config.getint('Strategy', 'correlation_computation_history'))
        # Set correlation computation interval
        if _config.has_option('Strategy', 'correlation_computation_interval'):
            self.correlation_computation_interval = max(1, _config.getint('Strategy', 'correlation_computation_interval'))
        else:
            self.correlation_computation_interval = max(2, self.correlation_computation_history/5)
        # Set computational variables
        self.last_date_correlation_matrix_computed = 0
        self.last_date_stdev_computed = 0
        self.last_date_exp_return_computed = 0
        self.last_rebalanced_day = -1
        self.stdev_computation_indicator_mapping = {}  # map from product to the indicator to get the stddev value
        self.exp_log_returns = numpy.array([0.0]*len(self.products))
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products])  # map from product to weight, which will be passed downstream
        self.weights = numpy.array([0.0]*len(self.products))  # these are the weights, with products occuring in the same order as the order in self.products
        self.stddev_logret = numpy.array([1.0]*len(self.products))  # these are the stddev values, with products occuring in the same order as the order in self.products
        # Create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = numpy.eye(len(self.products))
        # Set indicator for ExpectedReturns
        for product in self.products:
            self.expreturns_indicator = 'ExpectedReturns'
            indicator = self.expreturns_indicator + '.' + product + '.' + str(self.exp_return_computation_history)
            if is_valid_daily_indicator(self.expreturns_indicator):
                module = import_module('DailyIndicators.' + self.expreturns_indicator)
                Indicatorclass = getattr(module, self.expreturns_indicator)
                self.daily_indicators[indicator] = Indicatorclass.get_unique_instance(indicator, self.start_date, self.end_date, _config)
        # Set indicator for stddev_computation_indicator
        if is_valid_daily_indicator(self.stddev_computation_indicator):
            for product in self.products:
                _orig_indicator_name = self.stddev_computation_indicator + '.' + product + '.' + str(self.stddev_computation_history)  # this would be something like StdDev.fTY.252
                module = import_module('DailyIndicators.' + self.stddev_computation_indicator)
                Indicatorclass = getattr(module, self.stddev_computation_indicator)
                self.daily_indicators[_orig_indicator_name] = Indicatorclass.get_unique_instance(_orig_indicator_name, self.start_date, self.end_date, _config)
        else:
            print "Stdev computation indicator %s invalid!" % self.stddev_computation_indicator
            sys.exit(0)
        # Set correlation computation indicator
        _portfolio_string = make_portfolio_string_from_products(self.products)  # this allows us to pass a portfolio to the CorrelationLogReturns indicator.
        # TODO Should we change the design of passing arguments to the indicators from a '.' concatenated list to a variable argument set?
        self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance("CorrelationLogReturns" + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config)

    def process_param_file(self, _paramfilepath, _config):
        super(MeanVarianceOptimization, self).process_param_file(_paramfilepath, _config)

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
                # Get the stdev values from the stddev indicators
                for _product in self.products:
                     # earlier this was self.stddev_computation_indicator[_product] but due to error, switched to this
                    self.stddev_logret[self.map_product_to_index[_product]] = self.daily_indicators[self.stddev_computation_indicator + '.' + _product + '.' + str(self.stddev_computation_history)].values[1]
                    # TODO should not accessing an array without checking the length!
                    # TODO should add some sanity checks before overwriting previous value.
                    # TODO we can make tests here that the module needs to pass.
                _need_to_recompute_weights = True
                self.last_date_stdev_computed = self.day

            if self.day >= (self.last_date_exp_return_computed + self.exp_return_computation_interval):
                # Get the expected log return values values from the expected returns indicators
                for _product in self.products:
                    self.exp_log_returns[self.map_product_to_index[_product]] = self.daily_indicators[self.expreturns_indicator + '.' + _product + '.' + str(self.exp_return_computation_history)].values[1]
                _need_to_recompute_weights = True
                self.last_date_exp_return_computed = self.day

            if _need_to_recompute_weights:
                # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * numpy.outer(self.stddev_logret, self.stddev_logret)  # we should probably do it when either self.stddev_logret or _correlation_matrix has been updated

                # Recompute weights
                self.weights = efficient_frontier(self.exp_log_returns, _cov_mat, self.maximum_leverage, self.risk_tolerance, self.max_allocation)

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
