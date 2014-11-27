import numpy as np
from Utils.Regular import get_dt_from_date
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns
from portfolio_utils import get_products_from_portfolio_string,make_portfolio_string_from_products

class CorrelationLogReturns(IndicatorListener):
    """Compute the correlation of log returns of a number of product for the specified number of days

    Attributes:
        Note: While DailyIndicators usually have the add_listener function, this indicator does not


    """

    instances = {}

    def __init__(self, identifier, _startdate, _enddate, _config):
        params = identifier.strip().split('.') # interpretation of params is PortfolioString and number of days to look back
        _portfolio = params[1]
        self.products = sorted(get_products_from_portfolio_string(_portfolio)) # the only reason we need to sort again here is if the indicator has been initialized from "DailyIndicators" config
        self.logret_history = int(params[2])
        self.identifier = params[0] + '.' + make_portfolio_string_from_products(self.products) + '.' + params[2]
        self.logret_matrix = np.array([[0.0]*len(self.products)]) # 2d array of log returns # TODO change name
        self.map_identifier_to_index = {}
        self.date = get_dt_from_date(_startdate).date() # current date. whenever we get an update for a daet which is later than this date, we add a new row to self.logret_matrix

        # create a diagonal matrix
        self.correlation_matrix = np.zeros(shape=(len(self.products), len(self.products)))
        for i in xrange(len(self.products)):
            self.correlation_matrix[i,i] = 1.0

        self.correlation_matrix_already_computed = False

        _index = 0 #later used for column to store returns in
        for product in self.products:
            _this_identifier='DailyLogReturns.' + product
            DailyLogReturns.get_unique_instance(_this_identifier, _startdate, _enddate, _config).add_listener(self)
            self.map_identifier_to_index[_this_identifier]=_index
            _index = _index + 1

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        """Get the instance of the same indicator if already created before this.

        This should be agnostic of order of the products in the portfolio string
        """
        _split_identifier_words = identifier.split('.')
        _products = sorted(get_products_from_portfolio_string(_split_identifier_words[1]))
        _sorted_identifier = _split_identifier_words[0] + '.' + make_portfolio_string_from_products(_products) + '.' + _split_identifier_words[2]
        if _sorted_identifier not in CorrelationLogReturns.instances.keys() :
            new_instance = CorrelationLogReturns(_sorted_identifier, _startdate, _enddate, _config)
            CorrelationLogReturns.instances[_sorted_identifier] = new_instance
        return CorrelationLogReturns.instances[_sorted_identifier]

    def on_indicator_update(self, identifier, daily_log_returns_dt):
        """Update the standard deviation indicators on each ENDOFDAY event
        """
        current_date = daily_log_returns_dt[-1][0]
        if current_date > self.date:
            # If we are inside this if-statement, then this is the first update of a new day
            self.logret_matrix = np.insert(self.logret_matrix, self.logret_matrix.shape[0], [0.0]*len(self.products), axis=0)
            self.date = current_date
        self.logret_matrix[-1,self.map_identifier_to_index[identifier]] = daily_log_returns_dt[-1][1]
        if self.logret_matrix.shape[0] > self.logret_history:
            self.logret_matrix = np.delete(self.logret_matrix,(0), axis=0)
        self.correlation_matrix_already_computed = False # this just marks that we need to recompute elements


    def get_correlation_matrix(self):
        """Returns the correlation matrix of log returns
        For performance reasons, perhaps we don't want to recompute this in on_indicator_update, but only when requested
        """
        if not self.correlation_matrix_already_computed:
            self.covariance_matrix = np.cov(self.logret_matrix.T)
            # TODO, probably this can be made more efficient, since we already have the covariance matrix
            self.correlation_matrix = np.corrcoef(self.logret_matrix.T) 
            self.stddev_logret = np.std(self.logret_matrix,axis=0,ddof=1)
            self.correlation_matrix_already_computed = True
        return(self.correlation_matrix)
