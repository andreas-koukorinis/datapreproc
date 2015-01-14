# cython: profile=True
import sys
import numpy
from importlib import import_module

from utils.regular import check_eod, adjust_file_path_for_home_directory, is_float_zero, parse_weights, adjust_to_desired_l1norm_range
from daily_indicators.indicator_list import is_valid_daily_indicator,get_module_name_from_indicator_name
from daily_indicators.portfolio_utils import make_portfolio_string_from_products
from signals.signal_algorithm import SignalAlgorithm
from daily_indicators.correlation_log_returns import CorrelationLogReturns

class CTAM( SignalAlgorithm ):
    """Implement a crossover momentum strategy on multiple products.
    The estimated return on each product is the sign of the crossover signal(mvavg(short) - mvavg(long))
    For instance if we are provided the CrossoverIndicator as Crossover
    and CrossoverComputationParameters for fES: 5 50 200
    We will interpret this as we need to create two instances of Moving average indicator with arguments 50 and 200
    and recompute them every 5 days.
    On every recomputation day, we will take the sign of MA(short)- MA(long)
    We have an estimate of risk of the crossover signal associated with each product, StdDevCrossover for each product.
    Based on Kelly Breiman bet sizing, we aim to take a position propotional to the ( expected return / expected risk )
    in each product. These weights are normalized to have a leverage of 1 in this version
    """
    
    def init( self, _config ):
        self.target_risk = 10.0
        # Set StdDev indicator name
        self.stddev_computation_indicator = 'StdDev'
        # Set StdDev computation history
        self.stddev_computation_history = 252
        # Set StdDev computation interval
        self.stddev_computation_interval = 21
        # Set Correlation computation indicator
        self.correlation_computation_indicator = 'CorrelationLogReturns'
        # Set correlation computation hisory
        self.correlation_computation_history = 252
        # Set correlation computation interval
        self.correlation_computation_interval = 21
        # create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = numpy.eye(len(self.products))

        self.crossover_computation_indicator_name = "Crossover"
        self.crossover_computation_history = ['50', '200'] # Shorter period should be 1st
        self.crossover_computation_interval = max(1, min([int(_hist) for _hist in self.crossover_computation_history])/5)
        self.crossover_volatility_computation_indicator_name = "StdDevCrossover"
        self.crossover_volatility_computation_history = "21"
        self.crossover_volatility_computation_interval = int(self.crossover_volatility_computation_history)/5
        self.crossover_indicator_vec = []
        self.crossover_volatility_indicator_vec = []
        self.crossover_vec = numpy.zeros(len(self.products))
        self.crossover_volatility_vec = numpy.ones(len(self.products))
        self.stddev_logret = numpy.array([1.0]*len(self.products))  # these are the stddev values, with products occuring in the same order as the order in self.products
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.ctam_weights = numpy.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products

        _paramfilepath="/dev/null"
        if _config.has_option('Parameters', 'paramfilepath'):
            _paramfilepath=adjust_file_path_for_home_directory(_config.get('Parameters', 'paramfilepath'))
        self.process_param_file(_paramfilepath, _config)

        _modelfilepath="/dev/null"
        if _config.has_option('Strategy','modelfilepath'):
            _modelfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','modelfilepath'))
        self.process_model_file(_modelfilepath, _config)

    def process_param_file(self, _paramfilepath, _config):
        super(CTAM, self).process_param_file(_paramfilepath, _config)
    
    def process_model_file(self, _modelfilepath, _config):
        _model_file_handle = open( _modelfilepath, "r" )
        _map_product_to_crossover_computation_history = {}
        _map_product_to_crossover_volatility_computation_history = {}
        for _model_line in _model_file_handle:
            # We expect lines like:
            # Default CrossoverIndicator Crossover
            # Default CrossoverComputationParameters 5 50 200 
            # Default CrossoverVolatilityIndicator StdDevCrossover
            # Default CrossoverVolatilityComputationParameters 5 21
            # fES CrossoverComputationParameters 5 50 100
            # fZN CrossoverVolatilityComputationParameters 5 63
            _model_line_words = _model_line.strip().split(' ')
            if (len(_model_line_words) >= 3):
                if (_model_line_words[0] == 'Default'):
                    if _model_line_words[1] == 'CrossoverIndicator':
                        self.crossover_computation_indicator_name = _model_line_words[2]
                    elif _model_line_words[1] == 'CrossoverComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) == 3:
                            self.crossover_computation_interval = int(_computation_words[0])
                            self.crossover_computation_history = _computation_words[1:]
                    elif _model_line_words[1] == 'CrossoverVolatilityIndicator':
                        self.crossover_volatility_computation_indicator_name = _model_line_words[2]
                    elif _model_line_words[1] == 'CrossoverVolatilityComputationParameters':
                        _computation_words = _model_line_words[2:]
                        if len(_computation_words) == 2:
                            self.crossover_volatility_computation_interval = int(_computation_words[0])
                            self.crossover_volatility_computation_history = _computation_words[1]
                    elif _model_line_words[1] == 'StdDevIndicator':
                        self.stdev_computation_indicator_name=_model_line_words[2]
                    elif _model_line_words[1] == 'StdDevComputationParameters':
                        _computation_words = _model_line_words[2:]
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
                        if _model_line_words[1] == 'CrossoverComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) == 3:
                                #set the refreshing interval to the minimum of current and previous values
                                self.crossover_computation_interval = min(self.crossover_computation_interval, int(_computation_words[0])) 
                                _map_product_to_crossover_computation_history[_product] = _computation_words[1:]
                        elif _model_line_words[1] == 'CrossoverVolatilityComputationParameters':
                            _computation_words = _model_line_words[2:]
                            if len(_computation_words) == 2:
                                #set the refreshing interval to the minimum of current and previous values
                                self.crossover_volatility_computation_interval = min(self.crossover_volatility_computation_interval, int(_computation_words[0]))   
                                _map_product_to_crossover_volatility_computation_history[_product] = _computation_words[1]
            elif len(_model_line_words) == 2:
                if _model_line_words[0] == 'TargetRisk':
                    self.target_risk = float(_model_line_words[1])

        if is_valid_daily_indicator(self.crossover_computation_indicator_name):
            _crossover_indicator_module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.crossover_computation_indicator_name))
            CrossoverIndicatorClass = getattr(_crossover_indicator_module, self.crossover_computation_indicator_name)
        else:
            sys.exit("crossover_computation_indicator string %s is invalid" %(self.crossover_computation_indicator_name))

        if is_valid_daily_indicator(self.crossover_volatility_computation_indicator_name):
            _crossover_volatility_indicator_module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.crossover_volatility_computation_indicator_name))
            CrossoverVolatilityIndicatorClass = getattr(_crossover_volatility_indicator_module, self.crossover_volatility_computation_indicator_name)
        else:
            sys.exit("crossover_computation_indicator string %s is invalid" %(self.crossover_volatility_computation_indicator_name))

        # We have read the model. Now we need to create the indicators
        for _product in self.products:
            _identifier = self.crossover_computation_indicator_name + '.' + _product + '.' + '.'.join(_map_product_to_crossover_computation_history.get(_product, self.crossover_computation_history))
            self.crossover_indicator_vec.append(CrossoverIndicatorClass.get_unique_instance(_identifier, self.start_date, self.end_date, _config))

        for _product in self.products:
            _identifier = self.crossover_volatility_computation_indicator_name + '.' + _product + '.' + _map_product_to_crossover_volatility_computation_history.get(_product, self.crossover_volatility_computation_history) + '.' + '.'.join(_map_product_to_crossover_computation_history.get(_product, self.crossover_computation_history))
            self.crossover_volatility_indicator_vec.append(CrossoverVolatilityIndicatorClass.get_unique_instance(_identifier, self.start_date, self.end_date, _config))

        # Set indicator for stddev_computation_indicator
        if is_valid_daily_indicator(self.stddev_computation_indicator):
            module = import_module('daily_indicators.' + get_module_name_from_indicator_name(self.stddev_computation_indicator))
            Indicatorclass = getattr(module, self.stddev_computation_indicator)
            for product in self.products:
                _orig_indicator_name = self.stddev_computation_indicator + '.' + product + '.' + str(self.stddev_computation_history)  # this would be something like StdDev.fTY.252
                self.daily_indicators[_orig_indicator_name] = Indicatorclass.get_unique_instance(_orig_indicator_name, self.start_date, self.end_date, _config)
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

    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number
        
        if all_eod:
            _need_to_recompute_ctam_weights = False # By default we don't need to change weights unless some input has changed
            if (self.day % self.crossover_computation_interval) == 0:
                # we need to recompute crossover signal
                for i in xrange(len(self.crossover_vec)):
                    self.crossover_vec[i] = max(0.000001, self.crossover_indicator_vec[i].get_crossover()) # a max with 1% is just to not have divide by 0 problems.
                _need_to_recompute_ctam_weights = True
            if (self.day % self.crossover_volatility_computation_interval) == 0:
                # we need to recompute estimate of crossover volatility
                for i in xrange(len(self.crossover_volatility_vec)):
                    self.crossover_volatility_vec[i] = self.crossover_volatility_indicator_vec[i].get_crossover_volatility()
                _need_to_recompute_ctam_weights = True
            if self.day % self.correlation_computation_interval == 0:
                # we need to recompute the correlation matrix
                # this command will not do anything if the values have been already computed. else it will
                self.logret_correlation_matrix = self.correlation_computation_indicator.get_correlation_matrix()
                # TODO Add tests here for the correlation matrix to make sense.
                # If it fails, do not overwrite previous values, or throw an error
                _need_to_recompute_ctam_weights = True
                

            if self.day % self.stddev_computation_interval == 0:
                # Get the stdev values from the stddev indicators
                for _product in self.products:
                     # earlier this was self.stddev_computation_indicator[_product] but due to error, switched to this
                    self.stddev_logret[self.map_product_to_index[_product]] = self.daily_indicators[self.stddev_computation_indicator + '.' + _product + '.' + str(self.stddev_computation_history)].values[1]
                    # TODO should not accessing an array without checking the length!
                    # TODO should add some sanity checks before overwriting previous value.
                    # TODO we can make tests here that the module needs to pass.
                _need_to_recompute_ctam_weights = True
                

            if _need_to_recompute_ctam_weights:
                  # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * numpy.outer(self.stddev_logret, self.stddev_logret)  # we should probably do it when either self.stddev_logret or _correlation_matrix has been updated

                self.ctam_weights = self.crossover_vec/self.crossover_volatility_vec
                self.ctam_weights = self.ctam_weights/numpy.sum(numpy.abs(self.ctam_weights)) # Currently leverage is taken as 1
                _annualized_stdev_of_portfolio = 100.0*(numpy.exp(numpy.sqrt(252.0 * (numpy.asmatrix(self.ctam_weights) * numpy.asmatrix(_cov_mat) * numpy.asmatrix(self.ctam_weights).T))[0, 0]) - 1)
                # self.ctam_weights = self.ctam_weights*(self.target_risk/_annualized_stdev_of_portfolio)
                self.ctam_weights = adjust_to_desired_l1norm_range(self.ctam_weights, self.minimum_leverage, self.maximum_leverage)

                for _product in self.products:
                    self.map_product_to_weight[_product] = self.ctam_weights[self.map_product_to_index[_product]] # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            if (self.day - self.last_rebalanced_day >= self.rebalance_frequency) or _need_to_recompute_ctam_weights:
                # if either weights have changed or it is a rebalancing day, then ask execlogic to update weights
                # TODO{gchak} change rebalancing from days to magnitude of divergence from weights
                # so change the above to
                # if sum ( abs ( desired weights - current weights ) ) > threshold, then
                # update_positions
                self.update_positions(events[0]['dt'], self.map_product_to_weight)
                self.last_rebalanced_day = self.day
            else:
                self.rollover(events[0]['dt'])
