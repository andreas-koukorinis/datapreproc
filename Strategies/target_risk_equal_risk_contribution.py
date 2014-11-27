import sys
import numpy as np
from Algorithm.TradeAlgorithm import TradeAlgorithm
from Utils.Regular import check_eod
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from DailyIndicators.CorrelationLogReturns import CorrelationLogReturns
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products

class TargetRiskEqualRiskContribution(TradeAlgorithm):
    """Implementation of the ERC risk balanced strategy

    Items read from config :
    target_risk : this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
    rebalance_frequency : This is the number of days after which we rebalance to the assigned weights
    stddev_computation_history :
    stddev_computation_interval :
    stddev_computation_indicator : The indicator to use to compute the estimate of ex-ante standard deviation.
    correlation_computation_history :
    correlation_computation_interval :

    """
    def init( self, _config ):
        self.day=-1 # TODO move this to "watch" or a global time manager
        self.target_risk = _config.getfloat( 'Strategy', 'target_risk' ) # this is the risk value we want to have. For now we are just interpreting that as the desired ex-ante stdev value. In future we will improve this to a better risk measure
        self.rebalance_frequency = _config.getint( 'Parameters', 'rebalance_frequency' )
        self.stddev_computation_history = _config.getint( 'Strategy', 'stddev_computation_history' )
        self.stddev_computation_interval = _config.getint( 'Strategy', 'stddev_computation_interval' )
        self.stddev_computation_indicator = _config.get( 'Strategy', 'stddev_computation_indicator' )
        self.correlation_computation_history = _config.getint( 'Strategy', 'correlation_computation_history' )
        self.correlation_computation_interval = _config.getint( 'Strategy', 'correlation_computation_interval' )
        # Some computational variables
        self.last_date_correlation_computed = 0 # TODO change these to actual dates
        self.last_date_stdev_computed = 0 # TODO change thse to actual dates
        self.stdev_computation_indicator_mapping = {} # map from product to the indicator to get the stddev value
        self.map_product_to_weight=dict([(product,0.0) for product in self.products]) # map from product to weight, which will be passed downstream
        self.erc_weights = np.array([[0.0]*len(self.products)]) # these are the weights, with products occuring in the same order as the order in self.products
        self.stddev_logret = np.array([[1.0]*len(self.products)]) # these are the stddev values, with products occuring in the same order as the order in self.products

        self.map_product_to_index={} # this might be needed, dunno for sure
        _product_index = 0
        for _product in self.products:
            self.map_product_to_index[_product] = _product_index
            _product_index = _product_index + 1

        if ( is_valid_daily_indicator ( self.stddev_computation_indicator ) ):
            for product in self.products:
                _orig_indicator_name = self.stddev_computation_indicator + product + str(self.stddev_computation_history) #this would be something like StdDev.fTY.21
                module = import_module( 'DailyIndicators.' + stddev_computation_indicator )
                Indicatorclass = getattr( module, stddev_computation_indicator )
                self.daily_indicators[_orig_indicator_name] = Indicatorclass.get_unique_instance( _orig_indicator_name, self.start_date, self.end_date, _config )
                self.stddev_computation_indicator[product] = self.daily_indicators[_orig_indicator_name]
                # No need to attach ourselves as a listenr to the indicator for now. We are going to access the value directly
        else:
            print ("Stdev computation indicator %s invalid!" %(self.stddev_computation_indicator))
            sys.exit(0)

        _portfolio_string = make_portfolio_string_from_products ( self.products )
        self.correlation_computation_indicator = CorrelationLogReturns.get_unique_instance( "CorrelationLogReturns" + '.' + _portfolio_string + '.' + str(self.correlation_computation_history), self.start_date, self.end_date, _config )


    def on_events_update(self,events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number

        # If today is the rebalancing day, then use indicators to calculate new positions to take
        if all_eod and (self.day % self.rebalance_frequency == 0):
            _need_to_recompute_erc_weights = False # By default we don't need to change weights unless some input has changed
            if self.day >= ( self.last_date_correlation_matrix_computed + self.correlation_computation_interval ):
                # we need to recompute the correlation matrix
                self.logret_correlation_matrix = self.correlation_computation_indicator.recompute() # this command will not do anything if the values have been already computed. else it will
                # TODO Add tests here for the correlation matrix to make sense, and otherwise do not overwrite previous value
                # Or throw an erorr
                _need_to_recompute_erc_weights = True
                self.last_date_correlation_matrix_computed = self.day

            if self.day >= ( self.last_date_stdev_computed + self.stddev_computation_interval ):
                # Get the stdev values from the stddev indicators
                for _product in self.products:
                    self.stddev_logret[self.map_product_to_index[_product]] = self.stddev_computation_indicator[_product].values[1] # TODO should not accessing an array without checking the length!
                    # TODO should add some sanity checks before overwriting previous value.
                    # TODO we can make tests here that the module needs to pass.
                _need_to_recompute_erc_weights = True
                self.last_date_stdev_computed = self.day

            if _need_to_recompute_erc_weights:
                # Calculate weights to assign to each product using indicators
                # compute covariance matrix from correlation matrix and
                _cov_mat = self.logret_correlation_matrix * np.outer(self.stddev_logret, self.stddev_logret) # we should probably do it when either self.stddev_logret or _correlation_matrix has been updated

                if np.sum ( self.erc_weights ) == 0:
                    # Initialize weights
                    _annualized_risk = 100.0*(np.exp(np.sqrt(252.0)*self.stddev_logret)-1) # we should do this only when self.stddev_logret has been updated
                    zero_corr_risk_parity_weights = 1.0/(_annualized_risk)
                    zero_corr_risk_parity_weights = zero_corr_risk_parity_weights/np.sum(np.abs(zero_corr_risk_parity_weights))

                # Function to return the L1 norm of the series of { risk_contrib - man ( risk_contrib ) }, or sum of absolute values of the series
                def _get_l1_norm_risk_contributions(_given_weights):
                    _cov_vec = array(asmatrix( _cov_mat )*asmatrix( _given_weights ).T)[:,0]
                    _trc = _given_weights*_cov_vec
                    return sum(abs(_trc - mean(_trc)))

                _constraints =  {'type':'eq', 'fun': lambda x: sum(abs(x)) - 1}
                self.erc_weights = minimize ( _get_l1_norm_risk_contributions, self.erc_weights, method='SLSQP',constraints=_constraints,options={'ftol': 0.0000000000000000000000000001,'disp': True,'maxiter':10000 } ).x

                _annualized_stddev_of_portfolio = 100.0*(np.exp(np.sqrt(252.0*(np.asmatrix(self.erc_weights)*np.asmatrix(_cov_mat)*np.asmatrix(erc_weights).T))[0,0])-1)
                self.erc_weights = self.erc_weights*(self.target_risk/_annualized_stddev_of_portfolio)
                for _product in self.products:
                    self.map_product_to_weight[_product] = self.erc_weights[self.map_product_to_index[_product]] # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            self.update_positions( events[0]['dt'], self.map_product_to_weight )
        else:
            self.rollover( events[0]['dt'] )
