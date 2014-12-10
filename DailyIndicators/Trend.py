import sys
import numpy as numpy
from DailyIndicators.Indicator_Listeners import IndicatorListener
from DailyIndicators.DailyLogReturns import DailyLogReturns


class Trend(IndicatorListener):
    """Track the direction of trend for the product
    In the config file this indicator will be specfied as : Trend,product,period
    """

    instances = {}

    def __init__(self, identifier, _startdate, _enddate, _config):
        self.values = ()# Tuple of the form(dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        if len(params) <= 2:
            print("Trend requires at least three parameters in the identifier, like Trend.fES.63");
            sys.exit(0)
            #TODO{gchak} do something better than just exit ! Print a better error message.
        self.product = params[1]
        self.period = int(params[2])
        self.current_sum = 0.0
        self.listeners = []
        daily_log_ret = DailyLogReturns.get_unique_instance('DailyLogReturns.' + self.product, _startdate, _enddate, _config)
        daily_log_ret.add_listener(self)

    def add_listener(self, listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        if identifier not in Trend.instances.keys():
            new_instance = Trend(identifier, _startdate, _enddate, _config)
            Trend.instances[identifier] = new_instance
        return Trend.instances[identifier]

    def on_indicator_update(self, identifier, daily_log_returns_dt):
        """Update the past trend observed on each ENDOFDAY event"""
        daily_log_returns_dt_len = len(daily_log_returns_dt)
        if daily_log_returns_dt_len > self.period:
            _new_sum = self.current_sum - daily_log_returns_dt[daily_log_returns_dt_len - self.period - 1][1] + daily_log_returns_dt[daily_log_returns_dt_len - 1][1]
            val = _new_sum
            self.current_sum = _new_sum
        elif daily_log_returns_dt_len < 1:
            val = 0.0 # Dummy value for insufficient lookback period(case where only 1 log return)
        else:
            _new_sum = self.current_sum + daily_log_returns_dt[daily_log_returns_dt_len - 1][1]
            self.current_num += 1
            val = _new_sum
            self.current_sum = _new_sum
        self.values = (daily_log_returns_dt[-1][0], val)
        for listener in self.listeners:
            listener.on_indicator_update(self.identifier, self.values)
