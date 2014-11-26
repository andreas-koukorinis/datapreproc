import numpy as np
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns
from Utils.Regular import get_dt_from_date

# Compute the correlation of log returns of a number of product for the specified number of days
class CorrelationLogReturns( IndicatorListener ):

    instances = {}

    def __init__(self, identifier, _startdate, _enddate, _config):
        params = identifier.strip().split('.') # interpretation of params is PortfolioString and number of days to look back
        _portfolio = params[1]
        self.products = sorted(_portfolio.split(','))
        self.period = int(params[2])
        print self.period
        self.identifier = params[0] + '.' + ','.join(self.products) + '.' + params[2]
        self.values = np.array([[0.0]*len(self.products)])
        self.listeners = []
        self.map_identifier_to_index = {}
        self.date = get_dt_from_date(_startdate).date()
        _index = 0 #later used for column to store returns in
        for product in self.products:
            _this_identifier='DailyLogReturns.' + product
            DailyLogReturns.get_unique_instance(_this_identifier, _startdate, _enddate, _config).add_listener(self)
            self.map_identifier_to_index[_this_identifier]=_index
            _index = _index + 1

    def add_listener(self, listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        # TODO ... this should be agnostic of order of the products in the portfolio string
        _identifier = identifier.split('.')
        _products = _identifier[1].split(',')
        _sorted_identifier = _identifier[0] + '.' + ','.join(sorted(_products)) + '.' + _identifier[2]
        if _sorted_identifier not in CorrelationLogReturns.instances.keys() :
            new_instance = CorrelationLogReturns (_sorted_identifier, _startdate, _enddate, _config)
            CorrelationLogReturns.instances[_sorted_identifier] = new_instance
        return CorrelationLogReturns.instances[_sorted_identifier]

    # Update the standard deviation indicators on each ENDOFDAY event
    def on_indicator_update(self, identifier, daily_log_returns_dt):
        current_date = daily_log_returns_dt[-1][0]
        if current_date > self.date:
            self.values = np.insert(self.values, self.values.shape[0], [0.0]*len(self.products), axis=0)
            self.date = current_date
        self.values[-1,self.map_identifier_to_index[identifier]] = daily_log_returns_dt[-1][1]
        if self.values.shape[0] > self.period:
            self.values = np.delete(self.values, (0), axis=0)
        # TODO shoudl we check for the identifer or assume it is in the map ?
