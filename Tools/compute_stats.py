import pickle
import sys
from numpy import *
import scipy.stats as ss

def drawdown(returns):
    cum_returns = returns.cumsum()
    return -1.0*(max(maximum.accumulate(cum_returns) - cum_returns))

def rollsum(series,period):
    n = series.shape[0]
    if(n<period):
        return array([])                                                                             #empty array
    return array([sum(series[i:i+period]) for i in xrange(0,n-period+1)]).astype(float)

def mean_lowest_k_percent(series,k):
    sorted_series = sort(series)
    n = sorted_series.shape[0]
    _retval=0
    if n <= 0 :
        _retval=0
    else:
        _index_of_worst_k_percent = int((k/100.0)*n)
        if _index_of_worst_k_percent <= 0:
            _retval=sorted_series[0]
        else:
            _retval=mean(sorted_series[0:_index_of_worst_k_percent])
    return _retval

 # Prints the returns for k worst and k best days
def print_extreme_days(_dates_returns, k):
    _sorted_returns = sorted(_dates_returns, key=lambda x: x[1]) # Sort by returns
    _end_index_worst_days = min(len(_sorted_returns), k)
    _start_index_best_days = max(0, len(_sorted_returns) - k)
    if len(_sorted_returns) > 0:
        _worst_days = _sorted_returns[0:_end_index_worst_days]
        _best_days = _sorted_returns[_start_index_best_days:len(_sorted_returns)]
        print '\nWorst %d Days:'%k
        for item in _worst_days:
            print item[0], ' : ', (exp(item[1])-1)*100.0, '%'
        print '\nBest %d Days:'%k
        for item in reversed(_best_days):
            print item[0], ' : ', (exp(item[1])-1)*100.0, '%'

def print_extreme_weeks(_dates, _returns, k):
    _dated_weekly_returns = zip(_dates[0:len(_dates)-k], rollsum(_returns, k))
    _sorted_returns = sorted(_dated_weekly_returns, key=lambda x: x[1]) # Sort by returns
    _end_index_worst_days = min(len(_sorted_returns), k)
    _start_index_best_days = max(0, len(_sorted_returns) - k)
    if len(_sorted_returns) > 0:
        _worst_days = _sorted_returns[0:_end_index_worst_days]
        _best_days = _sorted_returns[_start_index_best_days:len(_sorted_returns)]
        print '\nWorst %d Weeks:'%k
        for item in _worst_days:
            print item[0], ' : ', (exp(item[1])-1)*100.0, '%'
        print '\nBest %d Weeks:'%k
        for item in reversed(_best_days):
            print item[0], ' : ', (exp(item[1])-1)*100.0, '%'

def analyse(_returns_file):
    with open(_returns_file, 'rb') as f:
        dates_returns = pickle.load(f)
        daily_log_returns = array([i[1] for i in dates_returns]).astype(float)
        _dates = array([i[0] for i in dates_returns])
        net_returns = 100.0*(exp(sum(daily_log_returns))-1)
        monthly_log_returns = rollsum(daily_log_returns,21)
        quarterly_log_returns = rollsum(daily_log_returns,63)
        yearly_log_returns = rollsum(daily_log_returns,252)
        _monthly_nominal_returns_percent = (exp(monthly_log_returns)-1)*100.0
        _quarterly_nominal_returns_percent = (exp(quarterly_log_returns)-1)*100.0
        _yearly_nominal_returns_percent = (exp(yearly_log_returns)-1)*100.0
        dml = (exp(mean_lowest_k_percent(daily_log_returns,10))-1)*100.0
        mml = (exp(mean_lowest_k_percent(monthly_log_returns,10))-1)*100.0
        qml = (exp(mean_lowest_k_percent(quarterly_log_returns,10))-1)*100.0
        yml = (exp(mean_lowest_k_percent(yearly_log_returns,10))-1)*100.0
        _annualized_returns_percent = (exp(252.0*mean(daily_log_returns))-1)*100.0
        annualized_stddev_returns = (exp(sqrt(252.0)*std(daily_log_returns))-1)*100.0
        sharpe = _annualized_returns_percent/annualized_stddev_returns
        skewness = ss.skew(daily_log_returns)
        kurtosis = ss.kurtosis(daily_log_returns)
        max_dd_log = drawdown(daily_log_returns)
        max_drawdown_percent = abs((exp(max_dd_log)-1)*100)
        return_by_maxdrawdown = _annualized_returns_percent/max_drawdown_percent
        print_extreme_days(dates_returns, 5)
        print_extreme_weeks(_dates, daily_log_returns, 5)
        print "\nNumber of Tradable Days = %d\n-------------RESULTS--------------------\nNet Returns = %.10f%%\nAnnualized_Returns = %.10f%% \nAnnualized_Std_Returns = %.10f%% \nSharpe Ratio = %.10f \nSkewness = %.10f\nKurtosis = %.10f\nDML = %.10f%%\nMML = %.10f%%\nQML = %.10f%%\nYML = %.10f%%\nMax Drawdown = %.10f%%\nReturn_drawdown_Ratio = %.10f \n" %(len(daily_log_returns),net_returns,_annualized_returns_percent,annualized_stddev_returns,sharpe,skewness,kurtosis,dml,mml,qml,yml,max_drawdown_percent,return_by_maxdrawdown)

def main():
    if len( sys.argv ) > 1:
        for i in range(1,len(sys.argv)):
            print '\n\nStats for %s'%sys.argv[i]
            analyse(sys.argv[i])
    else:
        sys.exit('python compute_stats.py return_file1 return_file2 .. .. returns_filen')

if __name__ == '__main__':
    main()
