import sys
import numpy
from numpy.linalg import inv
from importlib import import_module
from scipy.optimize import minimize

from Utils.Regular import check_eod, adjust_file_path_for_home_directory, is_float_zero, parse_weights, adjust_to_desired_l1norm_range
from Utils.correct_signs_weights import correct_signs_weights
from DailyIndicators.Indicator_List import is_valid_daily_indicator,get_module_name_from_indicator_name
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from DailyIndicators.CorrelationLogReturns import CorrelationLogReturns
from signals.signal_algorithm import SignalAlgorithm

class TargetRiskVolatilityParity(SignalAlgorithm):
    """Implementation of the volatility partity algorithm, that assigns weights to each product inversely proportional to its risk.
    We use stdev computed over a recent period for risk computation.
    If we assume products are not correlated(or equally correlated), this is an optimal soltuion in terms of risk adjusted returns.

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
        #Defaults
        self.target_risk = 10.0 # this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
        self.allocation_signs = numpy.ones(len(self.products)) # by default we are long in all products
        self.stdev_computation_indicator = 'AverageStdDev'
        self.stdev_computation_history = ['63']
        self.stdev_computation_interval = max(1, min([int(_hist) for _hist in self.stdev_computation_history])/5)
        self.correlation_computation_history = 1000
        self.correlation_computation_interval = max(2, self.correlation_computation_history/5)
        self.stdev_indicator_vec = []
        self.correlation_computation_indicator = None

        _paramfilepath="/dev/null"
        if _config.has_option('Parameters', 'paramfilepath'):
            _paramfilepath=adjust_file_path_for_home_directory(_config.get('Parameters', 'paramfilepath'))
        self.process_param_file(_paramfilepath, _config)

        _modelfilepath="/dev/null"
        if _config.has_option('Strategy','modelfilepath'):
            _modelfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','modelfilepath'))
        self.process_model_file(_modelfilepath, _config)

        # Some computational variables
        self.last_date_correlation_matrix_computed = 0
        self.last_date_stdev_computed = 0
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.erc_weights = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products
        self.erc_weights_optim = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products
        self.stdev_logret = numpy.array([1.0]*len(self.products)) # these are the stdev values, with products occuring in the same order as the order in self.products
        # create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = numpy.eye(len(self.products))
        
    def process_param_file(self, _paramfilepath, _config):
        super(TargetRiskVolatilityParity, self).process_param_file(_paramfilepath, _config)

    def process_model_file(self, _modelfilepath, _config):
        _model_file_handle = open( _modelfilepath, "r" )
        _map_product_to_stdev_computation_history = {}
        for _model_line in _model_file_handle:
            # We expect lines like:
            # target_risk 10
            # Default StdDevIndicator AverageStdDev
            # Default StdDevComputationParameters 5 63
            # CorrelationComputationParameters 30 252
            # allocation_signs f6J -1
            # fES StdDevComputationParameters 5 252
            _model_line_words = _model_line.strip().split(' ')
            if len(_model_line_words) >= 3:
                if _model_line_words[0] == 'Default':
                    if _model_line_words[1] == 'StdDevIndicator':
                        self.stdev_computation_indicator_name = _model_line_words[2]
                    elif _model_line_words[1] == 'StdDevComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) >= 2:
                            self.stdev_computation_interval = max(1, int(_computation_words[0]))
                            self.stdev_computation_history = _computation_words[1:]
                elif _model_line_words[0] == 'CorrelationComputationParameters':
                    self.correlation_computation_interval = max(1, int(_model_line_words[1]))
                    self.correlation_computation_history = max(2, int(_model_line_words[2]))
                elif _model_line_words[0] == 'allocation_signs':
                    _sign_words = _model_line_words[1:]
                    if len(_sign_words) % 2 != 0:
                        sys.exit('Something wrong in model file.allocation signs not in pairs')
                    idx = 0
                    while idx < len(_sign_words):
                        _product, _sign = _sign_words[idx], float(_sign_words[idx+1])
                        self.allocation_signs[self.map_product_to_index[_product]] = _sign
                        idx += 2
                else:
                    _product = _model_line_words[0]
                    if _product in self.products:
                        if _model_line_words[1] == 'StdDevComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) >= 2:
                                #set the refreshing interval to the minimum of current and previous values
                                self.stdev_computation_interval = min(self.stdev_computation_interval, int(_computation_words[0])) 
                                _map_product_to_stdev_computation_history[_product] = _computation_words[1:]
            elif len(_model_line_words) == 2:
                if _model_line_words[0] == 'target_risk':
                    self.target_risk = float(_model_line_words[1])
                elif _model_line_words[0] == 'allocation_signs':
                    _sign_words = _model_line_words[1:]
                    if len(_sign_words) % 2 != 0:
                        sys.exit('Something wrong in model file.allocation signs not in pairs')
                    idx = 0
                    while idx < len(_sign_words):
                        _product, _sign = _sign_words[idx], float(_sign_words[idx+1])
                        self.allocation_signs[self.map_product_to_index[_product]] = _sign
                        idx += 2

        if is_valid_daily_indicator(self.stdev_computation_indicator_name):
            _stdev_indicator_module = import_module('DailyIndicators.' + get_module_name_from_indicator_name(self.stdev_computation_indicator_name))
            StdDevIndicatorClass = getattr(_stdev_indicator_module, self.stdev_computation_indicator_name)
        else:
            sys.exit( "stdev_computation_indicator string %s is invalid" %(self.stdev_computation_indicator_name) )

        
        # We have read the model. Now we need to create the indicators
        for _product in self.products:
            _identifier = self.stdev_computation_indicator_name + '.' + _product + '.' + '.'.join(_map_product_to_stdev_computation_history.get(_product, self.stdev_computation_history))
            self.stdev_indicator_vec.append(StdDevIndicatorClass.get_unique_instance(_identifier, self.start_date, self.end_date, _config))

        _portfolio_string = make_portfolio_string_from_products(self.products) # this allows us to pass a portfolio to the CorrelationLogReturns indicator.
        # TODO Should we change the design of passing arguments to the indicators from a '.' concatenated list to a variable argument set?
        self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance("CorrelationLogReturns" + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config)

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
                for i in xrange(len(self.stdev_logret)):
                    self.stdev_logret[i] = max(0.000001, self.stdev_indicator_vec[i].get_stdev()) # a max with 1% is just to not have divide by 0 problems.
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
                zero_corr_risk_parity_weights = (1.0/_annualized_risk) * _expected_sharpe_ratios # what IVWAS would have done 
                self.erc_weights = zero_corr_risk_parity_weights

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
