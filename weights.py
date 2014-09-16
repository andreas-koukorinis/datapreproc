#!/usr/bin/python

from numpy import *

# Returns Weights for unlevered RP portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# lookback_trend : how many days in the past are considered for deciding the trend
# rebalance_freq : after how many days should the portfolio be rebalanced 
# lookback risk : what multiple of [rebalance_freq] should be used to calculate the risk associated with the instrument
def setWeightsUnleveredRP(data,day,lookback_trend,lookback_risk,rebalance_freq):
    periodic_ret = []
    for i in range(day-lookback_risk*rebalance_freq,day,rebalance_freq):
        periodic_ret.append(sum(data[i:i+rebalance_freq,:],axis=0))
    periodic_ret = array(periodic_ret)      
    risk = 1/std(exp(periodic_ret)-1,axis=0)			
    w = sign(sum(data[day-lookback_trend:day,:],axis=0))/risk						# weights = Sign(excess returns)/Risk
    w = w/sum(absolute(w))										# normalize the weights to ensure unlevered portfolio		
    return w


