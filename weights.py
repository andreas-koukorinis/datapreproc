#!/usr/bin/python

from numpy import *
from getStdev import getSimpleStdev,getExponentialStdev


# Returns Weights for unlevered trend following RP portfolio[Demystified]
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# weightfunc_args[1] = lookback_trend : how many days in the past are considered for deciding the trend
# weightfunc_args[0] = lookback risk : number of days to be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk,lookback_trend)
def setWeightsUnleveredDemystified(data,day,weightfunc_args):
    lookback_risk=weightfunc_args[0] 
    lookback_trend=weightfunc_args[1]
    past_ret = array(data[day-lookback_risk:day,:])      
    risk = getSimpleStdev(past_ret)			
    w = sign(sum(data[day-lookback_trend:day,:],axis=0))/risk						# weights = Sign(excess returns)/Risk
    w = w/sum(absolute(w))										# normalize the weights to ensure unlevered portfolio		
    print '\nMoney Allocated:'
    print w*100000
    print 'Risk:'
    print risk*sqrt(250)
    return w


# Returns Weights for unlevered RP portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# weightfunc_args[0] = lookback risk : number of days to be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk)
def setWeightsUnleveredRP(data,day,weightfunc_args):
    lookback_risk=weightfunc_args[0] 
    past_ret = array(data[day-lookback_risk:day,:])      
    risk = getSimpleStdev(past_ret)			
    w = 1/risk												# weights = 1/Risk
    w = w/sum(absolute(w))
    print '\nMoney Allocated:'
    print w*100000
    print 'Risk:'
    print risk*sqrt(250)										# normalize the weights to ensure unlevered portfolio		
    return w



