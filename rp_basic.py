#!/usr/bin/python

from init import getLogReturns
import weights 
from getPerfStats import getPerfStats
from numpy import *
import ConfigParser


#TO CORRECT : RISK BASED ON SIMPLE STDDEV OF RETURNS????,EXCESS RETURN ZERO,ADD STATISTICS,ADD LEVERED RP PORTFOLIO


# Returns : Periodic returns for the portfolio
# setWeights : The function used to compute weights for the portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# rebalance_freq : after how many days should the portfolio be rebalanced 
# weightfunc_args : contains the arguments given to weight function(can be different depending on the weight function passed).Check config.txt
def computePortfolioResults(Weightsfunc,data,rebalance_freq,weightfunc_args):
    num_instruments = data.shape[1]
    num_days = data.shape[0]
    periodic_returns = []

    lookback_risk=weightfunc_args[0]
    lookback_trend=weightfunc_args[1] if(len(weightfunc_args)>1) else 0
     
    day = max(lookback_risk*rebalance_freq,lookback_trend)   								# Start from offset so that historical returns can be used
    
    w = Weightsfunc(data,day,rebalance_freq,weightfunc_args)								# Set initial weights for unlevered RP portfolio

    if(day+rebalance_freq >=num_days):
        print('Data not sufficient for initial lookback')
		
    for i in range(day+rebalance_freq,num_days,rebalance_freq):
        w1 = Weightsfunc(data,i,rebalance_freq,weightfunc_args)								# Compute new weights on every rebalancing day
        periodic_returns.append(log(1+ sum((w-w1)*(exp(sum(data[i-rebalance_freq:i,:],axis=0))-1))))		# convert log returns to actual returns,take weighted sum and then log
        w=w1
    return array(periodic_returns).astype(float)


# Main script
# Read config file
config = ConfigParser.ConfigParser()
config.readfp(open(r'config.txt'))
weightfunc_name = config.get('WeightFunction', 'func')
weightfunc_args = config.get('WeightFunction', 'args').strip().split(",")
weightfunc_args = [int(i) for i in weightfunc_args]
Weightsfunc = getattr(weights, weightfunc_name)
rebalance_freq = config.getint('Parameters', 'rebalance_freq')
products = config.get('Products', 'symbols').strip().split(",")
startdate = config.get('Products', 'startdate')
enddate = config.get('Products', 'enddate')

#Compute/Load Log Returns
data = getLogReturns(products,startdate,enddate)   

#Compute Portfolio periodic returns
periodic_returns = computePortfolioResults(Weightsfunc,data,rebalance_freq,weightfunc_args)

# Print Results
getPerfStats(periodic_returns,rebalance_freq)
