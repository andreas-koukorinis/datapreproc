# cython: profile=True
import sys
import numpy
from datetime import datetime
from indicator_listeners import IndicatorListener
from daily_log_returns import DailyLogReturns
from crossover import Crossover
from utils.regular import is_float_zero

# In the config file this indicator will be specfied as StdDevCrossover.product.stddev_period.crossover_short_period.crossover_long_period
class StdDevCrossover(IndicatorListener):
    """Track the standard deviation of crossover signal based logret for the product
       Identifier: 
           StdDevCrossover.product.stddev_period.crossover_short_period.crossover_long_period
           Eg: StdDevCrossover.fES.63.50.200
       Listeners:
           None
       Listening to:
           DailyLogReturns of the corresponding product
           Crossover indicator of the corresponding product
    """
    instances = {}

    def __init__(self, identifier, _startdate, _enddate, _config):
        """Initializes the required variables like identifier, stddev_period, current_sum, current_num, current_pow_sum etc
           and starts listening to dailylogreturns and crossover signal of the corresponding product
           
           Args:
               _identifier(string): The identifier for this indicator. Eg: StdDevCrossover.fES.63.50.200
               _startdate(date object): The start date of the simulation
               _enddate(date object): The end date of the simulation
               _config(ConfigParser handle): The handle to the config file of the strategy            
        """        
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.stddev_period = int(params[2])
        self.date = datetime.fromtimestamp(0).date()
        self.dailylogreturn_identifier = 'DailyLogReturns.' + self.product
        self.crossover_identifier = 'Crossover.' + self.product + '.' + params[3] + '.' + params[4]
        DailyLogReturns.get_unique_instance(self.dailylogreturn_identifier, _startdate, _enddate, _config).add_listener(self)
        Crossover.get_unique_instance(self.crossover_identifier, _startdate, _enddate, _config).add_listener(self)
        self.num_listening = 2
        self.num_updates = 0
        self.current_crossover = 0.0
        # To maintain running stdDev
        self.current_sum = 0.0 # Sum of log returns in the window
        self.current_num = 0.0 # Number of elements in the window
        self.current_pow_sum = 0.0 # Sum of squares of log returns in the window
        self.signal_values = []
        self.listeners = []

    def get_crossover_volatility(self):
        if len(self.values) >= 2:
            return (self.values[1])
        else:
            return 1.0

    def add_listener(self, listener):
        """Used by other classes to register as on_indicator_update listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen
        """
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        """This static function is used by other classes to add themselves as a listener to the StdDevCrossover"""
        if identifier not in StdDevCrossover.instances.keys() :
            new_instance = StdDevCrossover(identifier, _startdate, _enddate, _config)
            StdDevCrossover.instances[identifier] = new_instance
        return StdDevCrossover.instances[identifier]

    def on_indicator_update(self, identifier, values):
        """On a logreturn/crossover update, this function is called by the DailyLogReturns/Crossover instance and the new values are set here
           If we have recieved updates for both logreturn/crossover, new stddev_crossover value is computed in an online fashion and the tuple
           (date, new_stddevcrossover_value) is passed onto the listeners of this indicator

           Args:
               identifier(string): The identifier for the indicator whose update has come.In this case DailyLogReturns.product or Crossover.product.short_period.long_period
               values (list of tuples (date, logretvalue) or tuple (date, crossover_value): 
        """        
        self.num_updates += 1 # Received another update
        if identifier == self.dailylogreturn_identifier:
            self.current_logret = values[-1][1]
            self.date = values[-1][0]
        elif identifier == self.crossover_identifier:
            self.current_crossover = values[1]
            self.date = values[0]
        if self.num_updates == self.num_listening: # If we have received all updates
            self.num_updates = 0            
            _signal = self.current_crossover * self.current_logret
            self.signal_values.append(_signal)
            n = len(self.signal_values)
            if n > self.stddev_period:
                self.current_sum =  self.current_sum - self.signal_values[-self.stddev_period-1] + self.signal_values[-1]
                self.current_pow_sum =  self.current_pow_sum - pow(self.signal_values[-self.stddev_period-1], 2) + pow(self.signal_values[-1], 2)
                _val = numpy.sqrt(self.current_pow_sum/self.current_num - pow(self.current_sum/self.current_num, 2) )
            elif n < 2:
                _val = 1.0 # Dummy value for insufficient lookback period(case where only 1 log return)
                if n == 1:
                    self.current_sum = self.signal_values[-1]
                    self.current_pow_sum = pow(self.signal_values[-1], 2)
                    self.current_num = 1
            else:
                self.current_sum = self.current_sum + self.signal_values[-1]
                self.current_pow_sum =  self.current_pow_sum + pow(self.signal_values[-1], 2)
                self.current_num += 1
                _val = numpy.sqrt(self.current_pow_sum/self.current_num - pow(self.current_sum/self.current_num, 2))
            if numpy.isnan(_val):
                print ("something wrong")
            if is_float_zero(_val):
                _val = 1.0 #TODO check
            self.values = (self.date, _val)
            for listener in self.listeners: 
                listener.on_indicator_update(self.identifier, self.values)
