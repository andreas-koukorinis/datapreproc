# cython: profile=True
import sys
import numpy
import math
from indicator_listeners import IndicatorListener
from daily_log_returns import DailyLogReturns
from moving_average import MovingAverage
from utils.global_variables import Globals

# In the config file this indicator will be specfied as StdDevCrossover.product.stddev_period.crossover_short_period.crossover_long_period
class Crossover(IndicatorListener):
    """Track the crossover signal value i.e. sign(mvavg(short) - mvavg(long)) for the product
       Identifier: 
           Crossover.product.crossover_short_period.crossover_long_period
           Eg: Crossover.fES.50.200
       Listeners:
           None
       Listening to:
           DailyLogReturns of the corresponding product
           Moving Average indicator (logn and short) of the corresponding product
    """
    def __init__(self, identifier, _startdate, _enddate, _config):
        """Initializes the required variables like identifier, self.current_mv_short, self.current_mv_long etc
           and starts listening to short and long moving averages of the corresponding product
           
           Args:
               _identifier(string): The identifier for this indicator. Eg: Crossover.fES.50.200
               _startdate(date object): The start date of the simulation
               _enddate(date object): The end date of the simulation
               _config(ConfigParser handle): The handle to the config file of the strategy            
        """
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.crossover_short_identifier = 'MovingAverage.' + self.product + '.' + params[2]
        self.crossover_long_identifier = 'MovingAverage.' + self.product + '.' + params[3]
        MovingAverage.get_unique_instance(self.crossover_short_identifier, _startdate, _enddate, _config).add_listener(self)
        MovingAverage.get_unique_instance(self.crossover_long_identifier, _startdate, _enddate, _config).add_listener(self)
        self.num_listening = 2
        self.num_updates = 0
        self.current_mv_short = 0.0
        self.current_mv_long = 0.0
        self.listeners = []

    def get_crossover(self):
        if len(self.values) >= 2:
            return (self.values[1])
        else:
            return 0

    def add_listener(self, listener):
        """Used by other classes to register as on_indicator_update listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen

           Returns: Nothing       
        """
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        """This static function is used by other classes to add themselves as a listener to the StdDev"""
        if identifier not in Globals.crossover_instances.keys() :
            new_instance = Crossover(identifier, _startdate, _enddate, _config)
            Globals.crossover_instances.instances[identifier] = new_instance
        return Globals.crossover_instances[identifier]

    def on_indicator_update(self, identifier, values):
        """On a moving average update, this function is called by the MovingAverage instance and the new moving avg value is set here
           On computation of a new stddev value, the tuple (date, new_crossover_value) is passed onto the listeners of this indicator

           Args:
               identifier(string): The identifier for the indicator whose update has come.In this case MovingAverage.product.period
               values(tuple (date, mvavg)): The moving average of the product
        """ 
        self.num_updates += 1 # Received another update
        if identifier == self.crossover_short_identifier:
            self.current_mv_short = values[1]
        elif identifier == self.crossover_long_identifier:
            self.current_mv_long = values[1]
        if self.num_updates == self.num_listening: # If we have received all updates
            self.num_updates = 0
            _val = numpy.sign(self.current_mv_short - self.current_mv_long)
            if math.isnan(_val):
                print ("something wrong")
            self.values = (values[0], _val)
            for listener in self.listeners: 
                listener.on_indicator_update(self.identifier, self.values)

