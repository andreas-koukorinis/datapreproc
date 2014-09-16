#!/usr/bin/python

from init import getLogReturns
import weights 
from getPerfStats import getPerfStats
from numpy import *
import ConfigParser


#TO CORRECT : RISK BASED ON SIMPLE STDDEV OF RETURNS????,EXCESS RETURN ZERO,ADD STATISTICS,ADD LEVERED RP PORTFOLIO


# Returns : Daily returns for the portfolio
# setWeights : The function used to compute weights for the portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# rebalance_freq : after how many days should the portfolio be rebalanced 
# weightfunc_args : contains the arguments given to weight function(can be different depending on the weight function passed).Check config.txt
def computePortfolioResults(Weightsfunc,data,rebalance_freq,weightfunc_args):
    num_days = data.shape[0]
    daily_returns = []

    lookback_risk=weightfunc_args[0]
    lookback_trend=weightfunc_args[1] if(len(weightfunc_args)>1) else 0
     
    start_day = max(lookback_risk*rebalance_freq,lookback_trend)   						        # Start from offset so that historical returns can be used
    
    print 'Net days = %d'%(num_days-start_day)
    if(start_day >=num_days):
        print('Data not sufficient for initial lookback')
		
    for i in range(start_day,num_days,1):
        if((i-start_day)%rebalance_freq ==0):
            w = Weightsfunc(data,i,rebalance_freq,weightfunc_args)						# Compute new weights on every rebalancing day
        daily_returns.append(log(1+ sum(w*(exp(data[i,:])-1))))
    return array(daily_returns).astype(float)


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
daily_returns = computePortfolioResults(Weightsfunc,data,rebalance_freq,weightfunc_args)

# Print Results
getPerfStats(daily_returns)
