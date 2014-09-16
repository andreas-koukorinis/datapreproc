#!/usr/bin/python

from init import load_data,compute_returns,compute_returns_specs
from weights import setWeightsUnleveredRP
from numpy import *
import ConfigParser


#TO CORRECT : RISK BASED ON SIMPLE STDDEV OF RETURNS????,EXCESS RETURN ZERO,ADD STATISTICS,ADD LEVERED RP PORTFOLIO


# Returns : Periodic returns for the portfolio
# setWeights : The function used to compute weights for the portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# lookback_trend : how many days in the past are considered for deciding the trend
# rebalance_freq : after how many days should the portfolio be rebalanced 
# lookback risk : what multiple of [rebalance_freq] should be used to calculate the risk associated with the instrument
def computePortfolioResults(setWeights,data,lookback_risk,lookback_trend,rebalance_freq):
    num_instruments = data.shape[1]
    num_days = data.shape[0]
    periodic_returns = []

    day = max(lookback_risk*rebalance_freq,lookback_trend)						# Start from offset so that historical returns can be used
    w = setWeights(data,day,lookback_trend,lookback_risk,rebalance_freq)				# Set initial weights for unlevered RP portfolio
		
    for i in range(day+rebalance_freq,num_days,rebalance_freq):
        w1 = setWeights(data,i,lookback_trend,lookback_risk,rebalance_freq)				# Compute new weights on every rebalancing day
        periodic_returns.append(log(1+ sum((w-w1)*(exp(sum(data[i-rebalance_freq:i,:],axis=0))-1))))	# convert log returns to actual returns,take weighted sum and then log
        w=w1
    return periodic_returns


# Main script
# Read config file
config = ConfigParser.ConfigParser()
config.readfp(open(r'config.txt'))
capital = config.getint('Parameters', 'capital')
rebalance_freq = config.getint('Parameters', 'rebalance_freq')
lookback_trend = config.getint('Parameters', 'lookback_trend')
lookback_risk = config.getint('Parameters', 'lookback_risk')
file_returns = config.get('Files', 'returns') if(config.has_option('Files', 'returns')) else ''
file_prices = config.get('Files', 'prices') if(config.has_option('Files', 'prices')) else ''
file_specs = config.get('Files', 'specs') if(config.has_option('Files', 'specs')) else ''


#Compute/Load Log Returns
if(config.has_option('Files', 'returns')):
    data = load_data(file_returns,'float')
elif(config.has_option('Files', 'specs')): 
    prices = load_data(file_prices,'float')
    specs = load_data(file_specs,'string')
    data = compute_returns_specs(prices,specs)
else:
    prices = load_data(file_prices,'float')
    data = compute_returns(prices)


periodic_returns = computePortfolioResults(setWeightsUnleveredRP,data,lookback_risk,lookback_trend,rebalance_freq)


# Print Results
PnL = capital*(exp(sum(periodic_returns))-1)								# PnL = capital*Actual Returns 
#print (periodic_returns)
print "PnL: %f"%PnL
