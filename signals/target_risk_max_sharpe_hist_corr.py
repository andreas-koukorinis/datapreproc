import sys
import numpy
from numpy.linalg import inv
from importlib import import_module
from scipy.optimize import minimize

from Utils.Regular import check_eod, parse_weights, adjust_to_desired_l1norm_range
from Utils.correct_signs_weights import correct_signs_weights
from DailyIndicators.Indicator_List import is_valid_daily_indicator,get_module_name_from_indicator_name
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from DailyIndicators.CorrelationLogReturns import CorrelationLogReturns
from signals.signal_algorithm import SignalAlgorithm

class TargetRiskMaxSharpeHistCorr(SignalAlgorithm):
    """Implementation of the max sharpe strategy under historical correlations without regularization

    Items read from config :
    target_risk : this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
    rebalance_frequency : This is the number of days after which we rebalance to the assigned weights
    stdev_computation_history :
    stdev_computation_interval :
    stdev_computation_indicator : The indicator to use to compute the estimate of ex-ante standard deviation.
    correlation_computation_history :
    correlation_computation_interval :

    """
    def init(self, _config):
        self.target_risk = 10 # this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
        if _config.has_option('Strategy', 'target_risk'):
            self.target_risk = _config.getfloat('Strategy', 'target_risk') 

        _paramfilepath="/dev/null"
        if _config.has_option('Strategy','paramfilepath'):
            _paramfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','paramfilepath'))
        #self.process_param_file(_paramfilepath, _config)

        # by default we are long in all products
        self.allocation_signs = numpy.ones(len(self.products))
        if _config.has_option('Strategy', 'allocation_signs'):
            _given_allocation_signs = parse_weights(_config.get('Strategy', 'allocation_signs'))
            for _product in _given_allocation_signs:
                self.allocation_signs[self.map_product_to_index[_product]] = _given_allocation_signs[_product]

        self.stdev_computation_indicator = 'AverageStdDev'
        if _config.has_option('Strategy', 'stdev_computation_indicator'):
            self.stdev_computation_indicator = _config.get('Strategy', 'stdev_computation_indicator')

        self.stdev_computation_history_vec = ['63'] # changed from int to a string to support AverageStdDev
        if _config.has_option('Strategy', 'stdev_computation_history'):
            _stdev_computation_history_string = _config.get('Strategy', 'stdev_computation_history')
            self.stdev_computation_history_vec = [max(2,int(x)) for x in _stdev_computation_history_string.split(',')]
        if (self.stdev_computation_indicator == 'StdDev') and (len(self.stdev_computation_history_vec) > 1):
            sys.exit('For stdev_computation_indicator StdDev only one history value is allowed')
        
        if _config.has_option('Strategy', 'stdev_computation_interval'):
            self.stdev_computation_interval = max(1, _config.getint('Strategy', 'stdev_computation_interval'))
        else:
            self.stdev_computation_interval = max(1, min(self.stdev_computation_history_vec)/5)

        self.correlation_computation_history = 1000
        if _config.has_option('Strategy', 'correlation_computation_history'):
            self.correlation_computation_history = max(2, _config.getint('Strategy', 'correlation_computation_history'))

        if _config.has_option('Strategy', 'correlation_computation_interval'):
            self.correlation_computation_interval = max(1, _config.getint('Strategy', 'correlation_computation_interval'))
        else:
            self.correlation_computation_interval = max(2, self.correlation_computation_history/5)

        # Some computational variables
        self.last_date_correlation_matrix_computed = 0
        self.last_date_stdev_computed = 0
        self.stdev_computation_indicator_mapping = {} # map from product to the indicator to get the stdev value
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.erc_weights = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products
        self.erc_weights_optim = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products
        self.stdev_logret = numpy.array([1.0]*len(self.products)) # these are the stdev values, with products occuring in the same order as the order in self.products
        # create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = numpy.eye(len(self.products))
        
        if is_valid_daily_indicator(self.stdev_computation_indicator):
            _stdev_indicator_module = import_module('DailyIndicators.' + get_module_name_from_indicator_name(self.stdev_computation_indicator))
            StdevIndicatorClass = getattr(_stdev_indicator_module, self.stdev_computation_indicator)
        else:
            print ( "stdev_computation_indicator string %s is invalid" %(self.stdev_computation_indicator) )
            sys.exit(0)

        for product in self.products:
            _orig_indicator_name = self.stdev_computation_indicator + '.' + product + '.' + '.'.join([str(x) for x in self.stdev_computation_history_vec]) # this would be something like StdDev.fZN.252
            self.daily_indicators[_orig_indicator_name] = StdevIndicatorClass.get_unique_instance(_orig_indicator_name, self.start_date, self.end_date, _config)
            # self.stdev_computation_indicator[product] = self.daily_indicators[_orig_indicator_name]
            # No need to attach ourselves as a listener to the indicator for now. We are going to access the value directly.

        _portfolio_string = make_portfolio_string_from_products(self.products) # this allows us to pass a portfolio to the CorrelationLogReturns indicator.
        # TODO Should we change the design of passing arguments to the indicators from a '.' concatenated list to a variable argument set?
        self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance("CorrelationLogReturns" + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config)


    def process_param_file(self, _paramfilepath, _config):
        super(TargetRiskEqualRiskContribution, self).process_param_file(_paramfilepath, _config)

    def on_events_update(self, events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number

        if all_eod:
            _need_to_recompute_erc_weights = False # By default we don't need to change weights unless some input has changed
            if self.day >= (self.last_date_correlation_matrix_computed + self.correlation_computation_interval):
                # we need to recompute the correlation matrix
                self.logret_correlation_matrix = self.correlation_computation_indicator.get_correlation_matrix() # this command will not do anything if the values have been already computed. else it will
                # TODO Add tests here for the correlation matrix to make sense.
                # If it fails, do not overwrite previous values, or throw an error
                _need_to_recompute_erc_weights = True
                self.last_date_correlation_matrix_computed = self.day

            if self.day >= (self.last_date_stdev_computed + self.stdev_computation_interval):
                # Get the stdev values from the stdev indicators
                for _product in self.products:
                    _orig_indicator_name = self.stdev_computation_indicator + '.' + _product + '.' + '.'.join([str(x) for x in self.stdev_computation_history_vec])
                    self.stdev_logret[self.map_product_to_index[_product]] = self.daily_indicators[_orig_indicator_name].get_stdev() 
                    # TODO should not accessing an array without checking the length!
                    # TODO should add some sanity checks before overwriting previous value.
                    # TODO we can make tests here that the module needs to pass.
                _need_to_recompute_erc_weights = True
                self.last_date_stdev_computed = self.day

            if _need_to_recompute_erc_weights:
                # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * numpy.outer(self.stdev_logret, self.stdev_logret) # we should probably do it when either self.stdev_logret or _correlation_matrix has been updated

                _annualized_risk = 100.0*(numpy.exp(numpy.sqrt(252.0)*self.stdev_logret)-1) # we should do this only when self.stdev_logret has been updated
                _expected_sharpe_ratios = self.allocation_signs # switched to self.allocation_signs from not multiplying anything 
                zero_corr_no_sign_risk_parity_weights = (1.0/_annualized_risk) # IVWAS without support for signs
                zero_corr_risk_parity_weights = (1.0/_annualized_risk) * _expected_sharpe_ratios # what IVWAS would have done 
                if numpy.sum(numpy.abs(self.erc_weights)) < 0.001:
                    # Initialize weights
                    self.erc_weights_optim = zero_corr_risk_parity_weights/numpy.sum(numpy.abs(zero_corr_risk_parity_weights))
                    self.erc_weights = self.erc_weights_optim

                _t_expected_sharpe_ratios = numpy.asmatrix(self.allocation_signs).T # switched to self.allocation_signs from numpy.ones(len(self.products))
                # Set erc_weights_optim to inv ( correlation martix ) * zero_corr_no_sign_risk_parity_weights
                self.erc_weights_optim = numpy.ravel(numpy.diagflat(zero_corr_no_sign_risk_parity_weights) * inv(self.logret_correlation_matrix) * _t_expected_sharpe_ratios)
                self.erc_weights = self.erc_weights_optim

                # We check whether weights produced here have the same signs as self.allocation_signs.
                # Otherwise we try to correct them
                if sum(numpy.abs(numpy.sign(self.erc_weights)-numpy.sign(self.allocation_signs))) > 0:
                    # some sign isn't what it should be
                    _check_sign_of_weights = False # this is sort of a debugging exercise
                    if _check_sign_of_weights:
                        print ( "Sign-check-fail: On date %s weights %s" %(events[0]['dt'], [ str(x) for x in self.erc_weights ]) )
                    self.erc_weights = correct_signs_weights(self.erc_weights, zero_corr_risk_parity_weights)

                # In the following steps we resize the portfolio to the target risk level.
                # We have just used stdev as the measure of risk here since it is simple.
                # TODO improve risk calculation
                _annualized_stdev_of_portfolio = 100.0*(numpy.exp(numpy.sqrt(252.0 * (numpy.asmatrix(self.erc_weights) * numpy.asmatrix(_cov_mat) * numpy.asmatrix(self.erc_weights).T))[0, 0]) - 1)
                self.erc_weights = self.erc_weights*(self.target_risk/_annualized_stdev_of_portfolio)

                self.erc_weights = adjust_to_desired_l1norm_range (self.erc_weights, self.minimum_leverage, self.maximum_leverage)
                
                for _product in self.products:
                    self.map_product_to_weight[_product] = self.erc_weights[self.map_product_to_index[_product]] # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            if (self.day - self.last_rebalanced_day >= self.rebalance_frequency) or _need_to_recompute_erc_weights:
                # if either weights have changed or it is a rebalancing day, then ask execlogic to update weights
                # TODO{gchak} change rebalancing from days to magnitude of divergence from weights
                # so change the above to
                # if sum ( abs ( desired weights - current weights ) ) > threshold, then
                # update_positions
                self.update_positions( events[0]['dt'], self.map_product_to_weight )
                self.last_rebalanced_day = self.day
            else:
                self.rollover( events[0]['dt'] )
