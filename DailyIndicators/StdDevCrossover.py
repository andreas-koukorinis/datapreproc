import sys
from numpy import *
from Indicator_Listeners import IndicatorListener
from DailyLogReturns import DailyLogReturns
from MovingAverage import MovingAverage

# In the config file this indicator will be specfied as StdDevCrossover.product.stddev_period.crossover_short_period.crossover_long_period
class StdDevCrossover( IndicatorListener ):

    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = () # Tuple of the form (dt,value)
        self.identifier = identifier
        params = identifier.strip().split('.')
        self.product = params[1]
        self.stddev_period = int(params[2])
        self.crossover_short_period = int(params[3])
        self.crossover_long_period = int(params[4])
        self.date = get_dt_from_date(_startdate).date()
        self.dailylogreturn_identifier = 'DailyLogReturns.' + self.product
        self.crossover_short_identifier = 'MovingAverage.' + self.product + '.' + params[3]
        self.crossover_long_identifier = 'MovingAverage.' + self.product + '.' + params[4]
        DailyLogReturns.get_unique_instance(self.dailylogreturn_identifier, _startdate, _enddate, _config).add_listener(self) 
        MovingAverage.get_unique_instance(self.crossover_short_identifier, _startdate, _enddate, _config).add_listener(self)
        MovingAverage.get_unique_instance(self.crossover_long_identifier, _startdate, _enddate, _config).add_listener(self)
        self.num_listening = 3
        self.num_updates = 0
        self.current_logret = 0.0
        self.logrets = empty(shape=(0))
        self.current_mv_short = 0.0
        self.current_mv_long = 0.0
        self.listeners = []

    def add_listener(self, listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(identifier, _startdate, _enddate, _config):
        if identifier not in StdDevCrossover.instances.keys() :
            new_instance = StdDevCrossover(identifier, _startdate, _enddate, _config)
            StdDevCrossover.instances[identifier] = new_instance
        return StdDevCrossover.instances[identifier]

    # TODO can optimize std
    def on_indicator_update(self, identifier, values):
        self.num_updates += 1 # Received another update
        if identifier == self.dailylogreturn_identifier:
            self.current_logret = values[-1][1]
            self.date = values[-1][0]
        elif identifier == self.crossover_short_identifier:
            self.current_mv_short = values[1]
            self.date = values[0]
        elif identifier == self.crossover_long_identifier:
            self.current_mv_long = values[1]
            self.date = values[0]
        else:
            sys.exit('Unhandled case in StdDevCrossover')
        if self.num_updates == self.num_listening: # If we have received all updates
            self.num_updates = 0
            signal = sign(self.current_mv_short - self.current_mv_long)
            logret = signal*self.current_logret
            self.logrets = append(self.logrets,logret)
            if self.logrets.shape[0] > self.stddev_period:
                self.logrets = delete(self.logrets, 0)
            self.values = (self.date, std(self.logrets))
            for listener in self.listeners: 
                listener.on_indicator_update(self.identifier, self.values)
