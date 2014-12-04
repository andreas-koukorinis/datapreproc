import sys
import numpy as np
from importlib import import_module
from scipy.optimize import minimize
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from DailyIndicators.CorrelationLogReturns import CorrelationLogReturns
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products

class TargetRiskMaxSharpeHistCorr(TradeAlgorithm):
    """Implementation of the max sharpe strategy under historical correlations without regularization

    Items read from config :
    target_risk : this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
    rebalance_frequency : This is the number of days after which we rebalance to the assigned weights
    stddev_computation_history :
    stddev_computation_interval :
    stddev_computation_indicator : The indicator to use to compute the estimate of ex-ante standard deviation.
    correlation_computation_history :
    correlation_computation_interval :

    """
    def init(self, _config):
        self.day = -1 # TODO move this to "watch" or a global time manager
        self.target_risk = _config.getfloat('Strategy', 'target_risk') # this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
        self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')
        self.stddev_computation_history = max(2, _config.getint('Strategy', 'stddev_computation_history'))
        self.stddev_computation_interval = max(1, _config.getint('Strategy', 'stddev_computation_interval'))
        self.stddev_computation_indicator = _config.get('Strategy', 'stddev_computation_indicator')
        self.correlation_computation_history = max(2, _config.getint('Strategy', 'correlation_computation_history'))
        self.correlation_computation_interval = max(1, _config.getint('Strategy', 'correlation_computation_interval'))
        # Some computational variables
        self.last_date_correlation_matrix_computed = 0 # TODO change these to actual dates
        self.last_date_stdev_computed = 0 # TODO change thse to actual dates
        self.stdev_computation_indicator_mapping = {} # map from product to the indicator to get the stddev value
        self.map_product_to_weight = dict([(product, 0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.erc_weights = np.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products
        self.erc_weights_optim = np.array([0.0]*len(self.products)) # these are the weights, with products occuring in the same order as the order in self.products
        self.stddev_logret = np.array([1.0]*len(self.products)) # these are the stddev values, with products occuring in the same order as the order in self.products
        # create a diagonal matrix of 1s for correlation matrix
        self.logret_correlation_matrix = np.zeros(shape=(len(self.products), len(self.products)))
        for i in xrange(len(self.products)):
            self.logret_correlation_matrix[i,i] = 1.0

        
        self.map_product_to_index = {} # this might be needed, dunno for sure
        _product_index = 0
        for _product in self.products:
            self.map_product_to_index[_product] = _product_index
            _product_index = _product_index + 1

        if is_valid_daily_indicator(self.stddev_computation_indicator):
            for product in self.products:
                _orig_indicator_name = self.stddev_computation_indicator + '.' + product + '.' + str(self.stddev_computation_history) # this would be something like StdDev.fTY.252
                module = import_module('DailyIndicators.' + self.stddev_computation_indicator)
                Indicatorclass = getattr(module, self.stddev_computation_indicator)
                self.daily_indicators[_orig_indicator_name] = Indicatorclass.get_unique_instance(_orig_indicator_name, self.start_date, self.end_date, _config)
                # self.stddev_computation_indicator[product] = self.daily_indicators[_orig_indicator_name]
                # No need to attach ourselves as a listener to the indicator for now. We are going to access the value directly.
        else:
            print("Stdev computation indicator %s invalid!" %(self.stddev_computation_indicator))
            sys.exit(0)

        _portfolio_string = make_portfolio_string_from_products(self.products) # this allows us to pass a portfolio to the CorrelationLogReturns indicator.
        # TODO Should we change the design of passing arguments to the indicators from a '.' concatenated list to a variable argument set?
        self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance("CorrelationLogReturns" + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config)


    def on_events_update(self, events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number

        # If today is the rebalancing day, then use indicators to calculate new positions to take
        if all_eod and(self.day % self.rebalance_frequency == 0):
            _need_to_recompute_erc_weights = False # By default we don't need to change weights unless some input has changed
            if self.day >= (self.last_date_correlation_matrix_computed + self.correlation_computation_interval):
                # we need to recompute the correlation matrix
                self.logret_correlation_matrix = self.correlation_computation_indicator.get_correlation_matrix() # this command will not do anything if the values have been already computed. else it will
                # TODO Add tests here for the correlation matrix to make sense.
                # If it fails, do not overwrite previous values, or throw an error
                _need_to_recompute_erc_weights = True
                self.last_date_correlation_matrix_computed = self.day

            if self.day >= (self.last_date_stdev_computed + self.stddev_computation_interval):
                # Get the stdev values from the stddev indicators
                for _product in self.products:
                    self.stddev_logret[self.map_product_to_index[_product]] = self.daily_indicators[self.stddev_computation_indicator + '.' + _product + '.' + str(self.stddev_computation_history)].values[1] # earlier this was self.stddev_computation_indicator[_product] but due to error in line 57, switched to this
                    # TODO should not accessing an array without checking the length!
                    # TODO should add some sanity checks before overwriting previous value.
                    # TODO we can make tests here that the module needs to pass.
                _need_to_recompute_erc_weights = True
                self.last_date_stdev_computed = self.day

            if _need_to_recompute_erc_weights:
                # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * np.outer(self.stddev_logret, self.stddev_logret) # we should probably do it when either self.stddev_logret or _correlation_matrix has been updated

                if np.sum(np.abs(self.erc_weights)) < 0.001:
                    # Initialize weights
                    _annualized_risk = 100.0*(np.exp(np.sqrt(252.0)*self.stddev_logret)-1) # we should do this only when self.stddev_logret has been updated
                    zero_corr_risk_parity_weights = 1.0/(_annualized_risk)
                    self.erc_weights_optim = zero_corr_risk_parity_weights/np.sum(np.abs(zero_corr_risk_parity_weights))
                    self.erc_weights = self.erc_weights_optim

                # Set erc_weights_optim to inv ( correlation martix ) * zero_corr_risk_parity_weights
                # self.erc_weights_optim = 
                self.erc_weights = self.erc_weights_optim

                _annualized_stddev_of_portfolio = 100.0*(np.exp(np.sqrt(252.0*(np.asmatrix(self.erc_weights)*np.asmatrix(_cov_mat)*np.asmatrix(self.erc_weights).T))[0, 0])-1)
                self.erc_weights = self.erc_weights*(self.target_risk/_annualized_stddev_of_portfolio)
                #TODO{gchak} have a global debug mode
                print ( "On date %s weights %s" %(events[0]['dt'], [ str(x) for x in self.erc_weights ]) )
                for _product in self.products:
                    self.map_product_to_weight[_product] = self.erc_weights[self.map_product_to_index[_product]] # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            self.update_positions(events[0]['dt'], self.map_product_to_weight)
        else:
            self.rollover(events[0]['dt'])
