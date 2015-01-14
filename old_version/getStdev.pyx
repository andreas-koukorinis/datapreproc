from numpy import *

# Returns the simple Standard deviation of 'returns' ndarray
def get_simple_stdev(returns):
    return std(returns,axis=0)

# Returns the simple Standard deviation of 'returns' ndarray
def get_annualized_stdev(returns):
    return get_simple_stdev(returns) * ( sqrt(252) )

#To be completed
def get_exponential_stdev(returns,decay):
    print 'To be completed'
