#!/usr/bin/python

import csv
from numpy import *
import scipy.ndimage

#TO CORRECT : FUTURE CONVERSION COST NOT INCLUDED,FUTURE SPECIFIC SYMBOL NOT CONSIDERED,CONVERSION FACTOR NOT SET,RISK BASED ON SIMPLE STDDEV,NO TRANSACTION COST,DATA TO BE CHANGED

# read log returns data directly from file
def getData(file_):
    data = []
    with open(file_, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    data = array(data)
    data = data.astype(float)
    return data

#[??specific not handled??]
# Compute Returns based on prices and Specific Symbol (data is filtered beforehand based on common dates)
# Prices : should be n*k 2d array where n is the number of trading days and k is the number of instruments
# Specific : ??? 
def getReturns(prices,specific):
    prices=prices.astype(float)
    returns = zeros([prices.shape[0]-1,prices.shape[1]]) 
    # Without Specific symbol
    for i in xrange(1,prices.shape[0]):
        returns[i-1,:] = log(prices[i,:]/prices[i-1,:])
    # With Specific symbol
#   for i in xrange(1,prices.shape[0]-1):
#       returns[i,:] = where(specific[i,:]==specific[i-1,:],prices[i,:]/prices[i-1,:],)
    return returns

    
# Returns Weights for unlevered RP portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# day : the day number on which the weights are to be calculated
# lookback_trend : how many days in the past are considered for deciding the trend
# rebalance_freq : after how many days should the portfolio be rebalanced 
# lookback risk : what multiple of [rebalance_freq] should be used to calculate the risk associated with the instrument
def setWeightsU(data,day,lookback_trend,lookback_risk,rebalance_freq):
    window = ones(data.shape[1])
    origin=-(rebalance_freq/2)							
    periodic_ret = scipy.ndimage.filters.convolve1d(data[day-lookback_risk*rebalance_freq:day,:],window, axis=0,origin=-(rebalance_freq/2))[rebalance_freq-1::rebalance_freq,:] 
											# Convolve for running window sum of returns,then select every (rebalance_freq)^th element 
    risk = 1/std(periodic_ret,axis=0)			
    w = sign(sum(data[day-lookback_trend:day,:],axis=0))/risk				# weights = Sign(excess returns)/Risk
    w = w/sum(absolute(w))								# normalize the weights to ensure unlevered portfolio		
    return w


# Main script
periodic_returns = []
PnL = 0
#data = getData('data.csv')
data = getReturns(load('data_ES_TY.csv.npy'),load('data_SPECS_ES_TY.csv.npy'))
num_instruments = data.shape[1]
num_days = data.shape[0]
conversion_factor = ones(num_instruments)						# ??Should have been directly loaded from file?? 
data = log(1+((exp(data)-1)*conversion_factor))						# To take care of tick value versus absolute value

# Parameters
capital = 100000
rebalance_freq = 5                           						# In number of days
lookback_trend = 60                            					# In number of days
lookback_risk = 20			     						# In multiple of rebalance frequency
day = max(lookback_risk*rebalance_freq,lookback_trend)					# Start from offset so that historical returns can be used
w = setWeightsU(data,day,lookback_trend,lookback_risk,rebalance_freq)			# Set initial weights for unlevered RP portfolio
		
for i in range(day+rebalance_freq,num_days,rebalance_freq):
    w1 = setWeightsU(data,i,lookback_trend,lookback_risk,rebalance_freq)		# Compute new weights on every rebalancing day
    periodic_returns.append(log(1+ sum((w-w1)*(exp(sum(data[i-rebalance_freq:i,:],axis=0))-1))))	# convert log returns to actual returns,take weighted sum and then log
    w=w1

# Print Results
PnL = capital*(exp(sum(periodic_returns))-1)						# PnL = capital*Actual Returns 
print (periodic_returns)
print "PnL: %f"%PnL
