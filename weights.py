#!/usr/bin/python

from numpy import *
from getStdev import getSimpleStdev


# Returns Weights for unlevered trend following RP portfolio[Demystified]
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# weightfunc_args[1] = lookback_trend : how many days in the past are considered for deciding the trend
# rebalance_freq : after how many days should the portfolio be rebalanced 
# weightfunc_args[0] = lookback risk : what multiple of [rebalance_freq] should be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk,lookback_trend)
def setWeightsUnleveredDemystified(data,day,rebalance_freq,weightfunc_args):
    periodic_ret = []

    lookback_risk=weightfunc_args[0] 
    lookback_trend=weightfunc_args[1]

    for i in range(day-lookback_risk*rebalance_freq,day,rebalance_freq):
        periodic_ret.append(sum(data[i:i+rebalance_freq,:],axis=0))
    periodic_ret = array(periodic_ret)      
    risk = 1/getSimpleStdev(periodic_ret)			
    w = sign(sum(data[day-lookback_trend:day,:],axis=0))/risk						# weights = Sign(excess returns)/Risk
    w = w/sum(absolute(w))										# normalize the weights to ensure unlevered portfolio		
    return w


# Returns Weights for unlevered RP portfolio
# rebalance_freq : after how many days should the portfolio be rebalanced 
# weightfunc_args[0] = lookback risk : what multiple of [rebalance_freq] should be used to calculate the risk associated with the instrument
####weightfunc_args -> list(lookback_risk)
def setWeightsUnleveredRP(data,day,rebalance_freq,weightfunc_args):
    periodic_ret = []

    lookback_risk=weightfunc_args[0] 
    
    for i in range(day-lookback_risk*rebalance_freq,day,rebalance_freq):
        periodic_ret.append(sum(data[i:i+rebalance_freq,:],axis=0))
    periodic_ret = array(periodic_ret)      
    risk = 1/getSimpleStdev(periodic_ret)			
    w = 1/risk												# weights = 1/Risk
    w = w/sum(absolute(w))										# normalize the weights to ensure unlevered portfolio		
    return w



