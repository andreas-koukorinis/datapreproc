#!/usr/bin/env python

import sys
from numpy import array,sum,log,exp
from init import getLogReturns
import weights
from getPerfStats import getPerfStats
import ConfigParser

#TO CORRECT : RISK BASED ON SIMPLE STDDEV OF RETURNS????,EXCESS RETURN ZERO,ADD STATISTICS,ADD LEVERED RP PORTFOLIO


# Returns : Daily returns for the portfolio
# setWeights : The function used to compute weights for the portfolio
# data : n*k 2d array of log returns where n is the number of trading days and k is the number of instruments
# rebalance_freq : after how many days should the portfolio be rebalanced
# weightfunc_args : contains the arguments given to weight function(can be different depending on the weight function passed). Check config.txt
def computePortfolioResults(Weightsfunc,data,rebalance_freq,weightfunc_args):
    num_days = data.shape[0]
    daily_returns = []

    lookback_risk=weightfunc_args[0]
    lookback_trend=weightfunc_args[1] if(len(weightfunc_args)>1) else 0

    start_day = max(lookback_risk,lookback_trend) # Start from offset so that historical returns can be used

    print 'Total Trading days given = %d'%(num_days)
    print 'Days left for calculations based on past = %d'%(start_day)
    print 'Net days on which returns are calculated = %d'%(num_days-start_day)
    if(start_day >=num_days):
        print('Data not sufficient for initial lookback')

    for current_day in range(start_day,num_days,1):
        if((current_day-start_day)%rebalance_freq ==0):
            print 'Day %d:'%current_day
            w = Weightsfunc(data,current_day,weightfunc_args)	# Compute new weights on every rebalancing day

        # Calculate the daily returns for a particular day and append to array
        # If notional anount of the money at risk in each instrument is M * abs(w_i)
        # then the pnl of the position = sign(w_i) * M * abs(w_i) * ( exp ( logret_i(t) ) - 1 ) = M * w_i * ( exp ( logret_i(t) - 1 ) )
        # Hence the new portfolio value = M + sum ( M * w_i * ( exp ( logret_i(t) - 1 ) ) )
        # Hence logret_day_ = log(1+ sum(w*(exp(data[current_day,:])-1)))
        daily_returns.append ( log ( 1 + sum ( w * ( exp(data[current_day,:]) - 1 ) ) ) )
    return array(daily_returns).astype(float)

# Main script
def __main__() :
    # Read config file
    config = ConfigParser.ConfigParser()
    if len(sys.argv) < 2 :
        print "Arguments: config_file <startdate enddate>"
        sys.exit(0)

    config.readfp(open(sys.argv[1],'r'))
    weightfunc_name = config.get('WeightFunction', 'func')
    weightfunc_args = config.get('WeightFunction', 'args').strip().split(",")
    weightfunc_args = [int(i) for i in weightfunc_args]
    Weightsfunc = getattr(weights, weightfunc_name)
    rebalance_freq = config.getint('Parameters', 'rebalance_freq')
    products = config.get('Products', 'symbols').strip().split(",")
    startdate = config.get('Products', 'startdate')
    enddate = config.get('Products', 'enddate')
    if len ( sys.argv ) >= 4:
        startdate = sys.argv[2]
        enddate = sys.argv[3]

    #Compute/Load Log Returns
    data = getLogReturns(products,startdate,enddate)

    #Compute Portfolio periodic returns
    daily_returns = computePortfolioResults(Weightsfunc,data,rebalance_freq,weightfunc_args)

    # Print Results
    print 'STRATEGY : %s'%weightfunc_name
    performance_stats = getPerfStats(daily_returns)
    print "\nNet_Log_Returns = %.10f \nNet_Percent_Returns = %.10f%%\nAnnualized_Returns = %.10f%% \nAnnualized_Std = %.10f%% \nSharpe_Ratio = %.10f \nMax_drawdown = %.10f%% \nReturn_drawdown_Ratio = %.10f \n" %(performance_stats.net_log_returns, performance_stats.net_percent_returns, performance_stats.annualized_percent_returns, performance_stats.annualized_percent_std, performance_stats.sharpe_percent, performance_stats.max_dd_percent, performance_stats.return_dd_ratio_percent )

__main__();
