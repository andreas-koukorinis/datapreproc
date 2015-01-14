import sys
import numpy
from indicator_listeners import IndicatorListener
from stddev import StdDev

class AverageStdDev( IndicatorListener ):
    """Track the average standard deviation of log returns over a list of periods for the product.
    In the config file this indicator will be specfied as : AverageStdDev.product.period1.period2...
    We will instantiate StdDev indicators for each duration and then maintain an average of the indicator values of those.

    """
    instances = {}

    def __init__( self, identifier, _startdate, _enddate, _config ):
        self.values = (0.0,1) # Tuple of the form (dt,value) #TODO better init for default dt
        self.identifier = identifier # e.g. AverageStdDev.fES.63.252
        params = identifier.strip().split('.')
        if len(params) <= 2:
            print ( "AverageStdDev requires at least three parameters in the identifier, like AverageStdDev.fES.63" );
            sys.exit(0)
            #TODO{gchak} do something better than just exit ! Print a better error message.
        self.product = params[1] #technically we don't need to store this, adn could be a temporary variable
        self.stdev_vec = numpy.ones(len(params)-2) # the last value received from this indicator
        self.received_updates = numpy.zeros(len(params)-2) # 0 if we have nto received an update from that indicator
        self.map_identifier_to_index = {}
        self.stdev_computation_indicator_vec = []
        for i in range(2,len(params)):
            _identifier = 'StdDev.' + self.product + '.' + str(int(params[i]))
            self.map_identifier_to_index[_identifier]=(i-2)
            self.stdev_computation_indicator_vec.append(StdDev.get_unique_instance( _identifier, _startdate, _enddate, _config ))
            self.stdev_computation_indicator_vec[-1].add_listener( self )
        self.stdev_vec_len = len(self.stdev_vec) # converted to float to compute averages later #constant
        self.listeners = []

    def get_stdev(self):
        if len(self.values) >= 2:
            return (self.values[1])
        else:
            return 1
    
    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config):
        if identifier not in AverageStdDev.instances.keys() :
            new_instance = AverageStdDev( identifier, _startdate, _enddate, _config )
            AverageStdDev.instances[identifier] = new_instance
        return AverageStdDev.instances[identifier]

    def _have_we_received_all_updates(self):
        if numpy.sum(self.received_updates) == self.stdev_vec_len:
            #If we have received all updates then we set it to 0
            self.received_updates = self.received_updates * 0 # is there a faster way to set to 0 ?
            return True
        return False
    
    
    def on_indicator_update( self, identifier, values ):
        """Update the standard deviation indicators on each ENDOFDAY event
        and compute the average when we have received all updates
        """
        _index = self.map_identifier_to_index[identifier]
        # We are not checking if 'identifier' is a valid key in the map, but this can be a simple test tat the StdDev indicator has been written correctly
        
        self.received_updates[_index] = 1 # mark that we have received upddate for this
        self.stdev_vec[_index] = values[1]
        if self._have_we_received_all_updates():
            val = numpy.sum(self.stdev_vec)/float(self.stdev_vec_len) # Compute the average of stdevs
            self.values = ( values[0], val )
            for listener in self.listeners: 
                listener.on_indicator_update( self.identifier, self.values )
