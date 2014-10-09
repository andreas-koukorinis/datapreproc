#!/usr/bin/python

from numpy import *

# Prints the performance statistics for the daily return series 'returns' 
# returns : n*k array containing log returns of n days for k instruments
def getPerfStats(returns):
    net_log_returns = sum(returns,axis=0) #changed to axis=0 to sum by column
    net_percent_returns = (exp(net_log_returns)-1)*100
    annualized_percent_returns = (exp((250)*mean(returns,axis=0))-1)*100 #brought 250 inside the exp
    annualized_percent_std = ( exp(sqrt(250.0)*std(returns,axis=0)) - 1 )*100
    # this is buggy. please fix to do it element-wise
    if annualized_percent_std > 0 :
        sharpe_percent = 0
    else :
        sharpe_percent = (annualized_percent_returns/annualized_percent_std)
    cum_returns = returns.cumsum()
    max_dd_log = max(maximum.accumulate(cum_returns) - cum_returns)
    max_dd_percent = (exp(max_dd_log)-1)*100
    return_dd_ratio_percent = annualized_percent_returns / max_dd_percent
    print "\nNet_Log_Returns = %.10f \nNet_Percent_Returns = %.10f%%\nAnnualized_Returns = %.10f%% \nAnnualized_Std = %.10f%% \nSharpe_Ratio = %.10f \nMax_drawdown = %.10f%% \nReturn_drawdown_Ratio = %.10f \n" %(net_log_returns,net_percent_returns,annualized_percent_returns,annualized_percent_std,sharpe_percent,max_dd_percent,return_dd_ratio_percent)

#getPerfStats(array([log(1.02),log(0.99),log(0.98),log(0.97),log(1.03)]),5)
