import sys
from importlib import import_module
from BookBuilder.BookBuilder import BookBuilder
from Portfolio import Portfolio
from Performance.SignalPerformanceTracker import SignalPerformanceTracker
from DailyIndicators.Indicator_List import is_valid_daily_indicator

class SignalAlgorithm():

    '''
    Base class for signal generator
    '''
    def __init__(self, _products, _aggregator, _startdate, _enddate, _config):
        self.products = _products
        self.aggregator = _aggregator
        self.init(_config)
        indicators = _config.get('DailyIndicators', 'names').strip().split(" ")
        self.daily_indicators = {}
        for indicator in indicators:
            indicator_name = indicator.strip().split('.')[0]
            if is_valid_daily_indicator(indicator_name):
                module = import_module('DailyIndicators.' + indicator_name)
                Indicatorclass = getattr(module, indicator_name)
                self.daily_indicators[indicator] = Indicatorclass.get_unique_instance(indicator, _startdate, _enddate, _config)

        # Give signal generator the access to the portfolio instance
        self.portfolio = Portfolio(self.products, _startdate, _enddate, _config)

        # Initialize performance tracker with list of products
        self.performance_tracker = SignalPerformanceTracker(self.products, _startdate, _enddate, _config)
        self.performance_tracker.portfolio = self.portfolio # Give Performance Tracker access to the portfolio     

    # User is expected to write the function
    def compute_signal(self, concurrent_events):
        pass

    # Return the portfolio variables as a dictionary
    def get_portfolio(self):
        return { 'cash' : self.portfolio.cash, 'num_shares' : self.portfolio.num_shares, 'products' : self.portfolio.products }

    def update_signal(self, dt, weights):
        self.performance_tracker.update_performance(dt, weights) # TODO should call once a day
        self.aggregator.update_signal(dt, weights)
