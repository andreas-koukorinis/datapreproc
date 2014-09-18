#!/usr/bin/python

from numpy import *

# Returns the simple Standard deviation of 'returns' ndarray
def getSimpleStdev(returns):
    return std(returns,axis=0)	

# Returns the simple Standard deviation of 'returns' ndarray
def getAnnualizedStdev(returns):
    return getSimpleStdev(returns) * ( sqrt(252)/returns.shape[0] )

#To be completed
def getExponentialStdev(returns,decay):
    print 'To be completed'

