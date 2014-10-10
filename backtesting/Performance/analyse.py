import pickle
import sys
from numpy import *
import scipy.stats as ss
import datetime
import matplotlib.pyplot as plt

def drawdown(returns):
    cum_returns = returns.cumsum()
    return max(maximum.accumulate(cum_returns) - cum_returns)

def rollsum(series,period):
    n = series.shape[0]
    if(n<period):
        return array([])                                                                             #empty array
    return array([sum(series[i:i+period]) for i in xrange(0,n-period+1)]).astype(float)

def meanlowestkpercent(series,k):
    sorted_series = sort(series)
    n = sorted_series.shape[0]
    num = int((k/100.0)*n)
    return mean(sorted_series[0:num])

def PlotPnLVersusDates(dates,log_returns,name):
    num = int(len(dates)/5.0)
    for i in xrange(0,len(dates)):
        if(i%num!=0 and i!= len(dates)-1):
            dates[i]=''
        else:
            dates[i] = dates[i].strftime('%d/%m/%Y')
    cumulative_returns = (exp(log_returns.cumsum())-1)*100.0
    plt.plot(cumulative_returns)
    plt.xticks(range(len(cumulative_returns)),dates)
    plt.xlabel('Date')
    plt.ylabel('Cumulative Returns')
    plt.savefig('Cumulative_Returns_'+name+".png", bbox_inches='tight')


def analyse():
    with open(sys.argv[1], 'rb') as f:
        dates_returns = pickle.load(f)
        daily_log_returns = [i[1] for i in dates_returns]
        daily_log_returns = array(daily_log_returns).astype(float)
        dates = [i[0] for i in dates_returns]
        net_returns = 100.0*(exp(sum(daily_log_returns))-1)
        monthly_log_returns = rollsum(daily_log_returns,21)
        quaterly_log_returns = rollsum(daily_log_returns,63)
        yearly_log_returns = rollsum(daily_log_returns,252)
        monthly_returns = (exp(monthly_log_returns)-1)*100.0
        quaterly_returns = (exp(quaterly_log_returns)-1)*100.0
        yearly_returns = (exp(yearly_log_returns)-1)*100.0
        dml = (exp(meanlowestkpercent(daily_log_returns,10))-1)*100.0
        mml = (exp(meanlowestkpercent(monthly_log_returns,10))-1)*100.0
        qml = (exp(meanlowestkpercent(quaterly_log_returns,10))-1)*100.0
        yml = (exp(meanlowestkpercent(yearly_log_returns,10))-1)*100.0
        annualized_returns = (exp(252.0*mean(daily_log_returns))-1)*100.0
        annualized_stddev_returns = (exp(sqrt(252.0)*std(daily_log_returns))-1)*100.0
        sharpe = annualized_returns/annualized_stddev_returns
        skewness = ss.skew(daily_log_returns)
        kurtosis = ss.kurtosis(daily_log_returns)
        max_dd_log = drawdown(daily_log_returns)
        max_drawdown_percent = (exp(max_dd_log)-1)*100
        return_by_maxdrawdown = annualized_returns/max_drawdown_percent
        PlotPnLVersusDates(dates,daily_log_returns,sys.argv[1].split('.')[0].split('_')[1])

        print "\n-------------RESULTS--------------------\nNet Returns = %.10f%%\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%%\nReturn_drawdown_Ratio = %.10f \n" %(net_returns,annualized_returns,annualized_stddev_returns,sharpe,skewness,kurtosis,dml,mml,qml,yml,max_drawdown_percent,return_by_maxdrawdown)

analyse()
