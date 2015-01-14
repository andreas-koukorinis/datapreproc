from numpy import sum,exp,mean,std,sqrt,maximum,abs

class PerformanceStats(object):
  """A collection of performance statistics of a strategy"""
  def __init__(self):
    self.net_log_returns = 0
    self.net_percent_returns = 0
    self.annualized_percent_returns = 0
    self.annualized_percent_std = 10 # dummy value
    self.sharpe_percent = 0
    self.cum_returns = 0
    self.max_dd_log = 0.1 # dummy value
    self.max_dd_percent = 10 # dummy value
    self.return_dd_ratio_percent = 0

# Prints the performance statistics for the daily return series 'returns'
# returns : 1D array of daily returns
def getPerfStats(_returns):
    _performance_stats = PerformanceStats()
    _performance_stats.net_log_returns = sum(_returns)
    _performance_stats.net_percent_returns = ( exp ( _performance_stats.net_log_returns ) -1 )*100
    _performance_stats.annualized_percent_returns = ( exp((252)*mean(_returns))-1)*100 #brought 252 inside the exp
    _performance_stats.annualized_percent_std = ( exp( sqrt(252.0)*std(_returns)) - 1 )*100
    if _performance_stats.annualized_percent_std <= 0 :
        _performance_stats.sharpe_percent = 0
    else :
        _performance_stats.sharpe_percent = (_performance_stats.annualized_percent_returns/_performance_stats.annualized_percent_std)
    _performance_stats.cum_returns = _returns.cumsum()
    _performance_stats.max_dd_log = -1.0*max(maximum.accumulate(_performance_stats.cum_returns) - _performance_stats.cum_returns)
    _performance_stats.max_dd_percent = abs(exp(_performance_stats.max_dd_log)-1)*100
    _performance_stats.return_dd_ratio_percent = _performance_stats.annualized_percent_returns / _performance_stats.max_dd_percent
    return ( _performance_stats )
