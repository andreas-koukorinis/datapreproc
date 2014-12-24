import sys
from importlib import import_module
import numpy
from numpy import hstack, vstack
from cvxopt import matrix, solvers
from cvxopt.solvers import qp
from Algorithm.signal_algorithm import SignalAlgorithm
from Utils.Regular import check_eod
from DailyIndicators.Indicator_List import is_valid_daily_indicator
from DailyIndicators.portfolio_utils import make_portfolio_string_from_products
from DailyIndicators.CorrelationLogReturns import CorrelationLogReturns


class MVO(SignalAlgorithm):
    """Perform mean variance optimization"""

    # Helper functions to succinctly represent constraints for optimizer
    def lag(self, x, k):
        """ Helper function lag(x,k)
        Produces a lag of k in a series x with leading zeros
        Args:
            x(list): Vector of numbers
            k(int): Discrete steps of lag to be introduced
        Returns:
            List: Vector of numbers shifted by k padded with zeros
        Example:
            >>> lag([1,2,3,4],2)
            [0,0,1,2]
        """
        if k == 0:
            return x
        elif k > len(x):
            return len(x)*[0]
        else:
            return k*[0]+x[0:-k]

    def shift_vec(self, x, m):
        """ Helper function shift_vec(x,m)
        Produces a matrix of vectors each with a lag i where i ranges from 0 to m-1
        Args:
            x(list): Vector of numbers
            m(int): Number of lagged vectors to be produced
        Returns:
            mat(matrix): Array of vectors each with a lag i where i ranges from 0 to m-1
        Example:
            >>> shift_vec([1,2,3],3)
            [[1,2,3],[0,1,2],[0,0,1]]'
        """
        n = len(x)
        mat = [self.lag(x, i) for i in range(0, m)]
        mat = matrix(mat, (n, m))

    def efficient_frontier(self,expected_returns, covariance, leverage, risk_tolerance, max_allocation=0.5):
        """ Function that calculates the efficient frontier
            by minimizing (Variance - risk tolerance * expected returns)
            with a given lerverage and risk tolance
            Args:
                returns(matrix) - matrix containing log daily returns for n securities
                covariance(matrix) - matrix containing covariance of the n securities
                leverage(float) - acceptable value of levarage
                risk_tolerance(float) - parameter of risk tolrance used in MVO
                max_allocation(float) - maximum weight to be allocated to one security
                                        (set low threshold to diversify)
            Returns:
                matrix of optimal weights performance stats exp.returns, std.dev, sharpe ratio
        """
        n = expected_returns.shape[0]  # Number of products
        # Setup inputs for optimizer
        # min(-d^T b + 1/2 b^T D b) with the constraints A^T b <= b_0
        # Constraint: sum(abs(weights)) <= leverage is non-linear
        # To make it linear introduce a dummy weight vector y = [y1..yn]
        # w1<y1,-w1<y1,w2<y2,-w2<y2,...
        # y1,y2,..,yn > 0
        # y1 + y2 + ... + yn <= leverage
        # Optimization will be done to find both w and y i.e n+n weights
        # Dmat entries for y kept low to not affect minimzing function as much as possible
        # Not kept 0 to still keep Dmat as semi-definite
        dummy_var_dmat = 0.000001*numpy.eye(n)
        Dmat = vstack((hstack((covariance, matrix(0., (n, n)))), hstack((matrix(0., (n, n)), dummy_var_dmat))))
        # Constraint:  y1 + y2 + ... + yn <= leverage
        Amat = vstack((matrix(0, (n, 1)), matrix(1, (n, 1))))
        bvec = [leverage]
        # Constraints:   y1, y2 ,..., yn >= 0
        Amat = hstack((Amat, vstack((matrix(0, (n, n)), -1*numpy.eye(n)))))
        bvec = bvec + n*[0]
        # Constraints:  y1, y2 ,..., yn <= max_allocation
        Amat = hstack((Amat, vstack((matrix(0, (n, n)), numpy.eye(n)))))
        bvec = bvec + n*[max_allocation]
        # Constraints:  -w1 <= y1, -w2 <= y2, ..., -wn <= yn
        dummy_wt_constraint1 = [-1] + (n-1)*[0] + [-1] + (n-1)*[0]
        Amat = hstack((Amat, self.shift_vec(dummy_wt_constraint1, n)))
        bvec = bvec + n*[0]
        # Constraints:  w1 <= y1, w2 <= y2, ..., wn <= yn
        dummy_wt_constraint2 = [1] + (n-1)*[0] + [-1] + (n-1)*[0]
        Amat = hstack((Amat, self.shift_vec(dummy_wt_constraint2, n)))
        bvec = bvec + n*[0]
        # Convert all NumPy arrays to CVXOPT matrics
        Dmat = matrix(Dmat)
        bvec = matrix(bvec, (len(bvec), 1))
        Amat = matrix(Amat.T)
        dvec = matrix(hstack((expected_returns, n*[0])).T)
        # Optimize
        portfolios = qp(Dmat, -1*risk_tolerance*dvec, Amat, bvec)['x']
        return portfolios

    def init(self, _config):
        """Initialize variables with configuration inputs or defaults"""
        self.day = -1
        solvers.options['show_progress'] = False
        # Set leverage
        self.leverage = 1
        if _config.has_option('Strategy', 'leverage'):
            self.leverage = _config.getfloat('Strategy', 'leverage')
        # Set risk tolerance
        self.risk_tolerance = 0.015
        if _config.has_option('Strategy', 'risk_tolerance'):
            self.risk_tolerance = _config.getfloat('Strategy', 'risk_tolerance')
        # Set maximum allocation
        self.max_allocation = 0.5
        if _config.has_option('Strategy', 'max_allocation'):
            self.max_allocation = _config.getfloat('Strategy', 'max_allocation')
        # Set rebalance frequency
        self.rebalance_frequency = 1
        if _config.has_option('Parameters', 'rebalance_frequency'):
            self.rebalance_frequency = _config.getint('Parameters', 'rebalance_frequency')
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

    def on_events_update(self, events):
        """Implement strategy and update weights"""
        all_eod = check_eod(events)  # Check if all events are ENDOFDAY
        if all_eod:
            self.day += 1  # Track the current day number

        # If today is the rebalancing day, then use indicators to calculate new positions to take
        if all_eod and(self.day % self.rebalance_frequency == 0):
            _need_to_recompute_weights = False  # By default we don't need to change weights unless some input has changed
            if self.day >= (self.last_date_correlation_matrix_computed + self.correlation_computation_interval):
                # we need to recompute the correlation matrix
                self.logret_correlation_matrix = self.correlation_computation_indicator.get_correlation_matrix()  # this command will not do anything if the values have been already computed. else it will
                # TODO Add tests here for the correlation matrix to make sense.
                # If it fails, do not overwrite previous values, or throw an error
                _need_to_recompute_weights = True
                self.last_date_correlation_matrix_computed = self.day

            if self.day >= (self.last_date_stdev_computed + self.stddev_computation_interval):
                # Get the stdev values from the stddev indicators
                for _product in self.products:
                    self.stddev_logret[self.map_product_to_index[_product]] = self.daily_indicators[self.stddev_computation_indicator + '.' + _product + '.' + str(self.stddev_computation_history)].values[1]  # earlier this was self.stddev_computation_indicator[_product] but due to error in line 57, switched to this
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

                self.weights = self.efficient_frontier(self.exp_log_returns, _cov_mat, self.leverage, self.risk_tolerance, self.max_allocation)

            for _product in self.products:
                    self.map_product_to_weight[_product] = self.weights[self.map_product_to_index[_product]]  # This is completely avoidable use of map_product_to_index. We could just start an index at 0 and keep incrementing it

            self.update_positions(events[0]['dt'], self.map_product_to_weight)
        else:
            self.rollover(events[0]['dt'])
