#!/usr/bin/python

from numpy import *

# Returns the performance statistics for the return series 'returns' containing periodic returns with a period of 'rebalance_freq'
# returns : n*k array containing log returns of n days for k instruments
# rebalance_freq : the periodicity of returns
def getPerfStats(returns,rebalance_freq):
    net_log_returns = sum(returns)
    net_percent_returns = (exp(net_log_returns)-1)*100
    annualized_log_returns = (250.0/rebalance_freq)*mean(returns)
    annualized_percent_returns = (250.0/rebalance_freq)*(exp(mean(returns))-1)*100
    annualized_log_std = sqrt(250.0/rebalance_freq)*std(returns)
    annualized_percent_std = sqrt(250.0/rebalance_freq)*std(exp(returns)-1)*100
    sharpe_log = (annualized_log_returns/annualized_log_std)
    sharpe_percent = (annualized_percent_returns/annualized_percent_std)
    cum_returns = returns.cumsum()
    max_dd_log = max(maximum.accumulate(cum_returns) - cum_returns)
    max_dd_percent = (exp(max_dd_log)-1)*100  
    return_dd_ratio_log = annualized_log_returns / max_dd_log
    return_dd_ratio_percent = annualized_percent_returns / max_dd_percent
    print "\nNet_Log_Returns = %.10f \nNet_Percent_Returns = %.10f%%\nAnnualized_Returns = %.10f%% \nAnnualized_Std = %.10f%% \nSharpe_Ratio = %.10f \nMax_dd = %.10f%% \nReturn_dd_Ratio = %.10f \n" %(net_log_returns,net_percent_returns,annualized_percent_returns,annualized_percent_std,sharpe_percent,max_dd_percent,return_dd_ratio_percent)

#getPerfStats(array([log(1.02),log(0.99),log(0.98),log(0.97),log(1.03)]),5)
