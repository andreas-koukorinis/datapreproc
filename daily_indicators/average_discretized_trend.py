import numpy as numpy
from indicator_listeners import IndicatorListener
from trend import Trend

class AverageDiscretizedTrend( IndicatorListener ):
    """Track the average trend of log returns over a list of periods for the product.
    In the config file this indicator will be specfied as : AverageDiscretizedTrend.product.period1.period2...
    We will instantiate StdDev indicators for each duration and then maintain an average of the indicator values of those.

    """
    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = (0.0,1) # Tuple of the form (dt,value) #TODO better init for default dt
        self.identifier = identifier # e.g. AverageDiscretizedTrend.fES.63.252
        params = identifier.strip().split('.')
        if len(params) <= 2:
            print ( "AverageDiscretizedTrend requires at least three parameters in the identifier, like AverageDiscretizedTrend.fES.63" );
            sys.exit(0)
            #TODO{gchak} do something better than just exit ! Print a better error message.
        self.product = params[1] #technically we don't need to store this, adn could be a temporary variable
        self.trend_vec = numpy.zeros(len(params)-2) # sign of the last value received from this indicator, initialized to 0
        self.received_updates = numpy.zeros(len(params)-2) # 0 if we have nto received an update from that indicator
        self.map_identifier_to_index = {}
        self.trend_computation_indicator_vec = []
        for i in range(2,len(params)):
            _identifier = 'Trend.' + self.product + '.' + str(int(params[i]))
            self.map_identifier_to_index[_identifier]=(i-2)
            self.trend_computation_indicator_vec.append(Trend.get_unique_instance( _identifier, _startdate, _enddate, _config ))
            self.trend_computation_indicator_vec[-1].add_listener( self )
        self.trend_vec_len = len(self.trend_vec) # converted to float to compute averages later
        self.listeners = []

    def add_listener( self, listener ):
        self.listeners.append( listener )

    def get_trend(self):
        if len(self.values) >= 2:
            return (self.values[1])
        else:
            return 0

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        if identifier not in AverageDiscretizedTrend.instances.keys() :
            new_instance = AverageDiscretizedTrend( identifier, _startdate, _enddate, _config )
            AverageDiscretizedTrend.instances[identifier] = new_instance
        return AverageDiscretizedTrend.instances[identifier]

    def _have_we_received_all_updates(self):
        if numpy.sum(self.received_updates) == self.trend_vec_len:
            #If we have received all updates then we set it to 0
            self.received_updates = self.received_updates * 0 # is there a faster way to set to 0 ?
            return True
        return False
        
    def on_indicator_update( self, identifier, values ):
        """On receiving updates from the trend indicators 
        compute the average when we have received all updates
        """
        _index = self.map_identifier_to_index[identifier]
        # We are not checking if 'identifier' is a valid key in the map, but this can be a simple test tat the StdDev indicator has been written correctly
        
        self.received_updates[_index] = 1 # mark that we have received upddate for this
        self.trend_vec[_index] = numpy.sign(values[1]) # very rudimentary form of discretization
        if self._have_we_received_all_updates():
            val = numpy.sum(self.trend_vec)/float(self.trend_vec_len) # Compute the average of trends
            self.values = ( values[0], val )
            for listener in self.listeners: 
                listener.on_indicator_update( self.identifier, self.values )
