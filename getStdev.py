#!/usr/bin/python

from numpy import *

# Returns the simple Standard deviation of 'returns' ndarray
def getSimpleStdev(returns):
    return 1/std(returns,axis=0)	

#To be completed
def getExponentialStdev(returns):
    print 'Not completed'
