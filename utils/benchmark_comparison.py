from datetime import timedelta
import numpy as np
import scipy.stats as ss
from dbqueries import fetch_prices
from regular import filter_series
from calculate import convert_daily_to_monthly_returns

benchmarks = { 'VBLTX' : 'daily_prices' , 'VTSMX' : 'daily_prices' }

def compute_daily_log_returns(prices):
    return np.log(prices[1:]/prices[:-1])

def get_monthly_correlation_to_benchmark(dates, daily_log_returns, benchmark_name):
    if daily_log_returns.shape[0] < 1:
        return 0
    benchmark_monthly_returns = get_benchmark_monthly_returns(dates[0], dates[-1], benchmark_name) #TODO start_date off by 1
    strategy_monthly_returns = convert_daily_to_monthly_returns(dates, daily_log_returns)
    filtered_benchmark_monthly_returns, filtered_strategy_monthly_returns = filter_series(benchmark_monthly_returns, strategy_monthly_returns)
    if len(filtered_benchmark_monthly_returns) != len(benchmark_monthly_returns) or len(filtered_benchmark_monthly_returns) != strategy_monthly_returns: # If we skipped some months
        pass#print '%d vs %d vs %d vs %d'%(len(strategy_monthly_returns), len(benchmark_monthly_returns), len(filtered_strategy_monthly_returns), len(filtered_benchmark_monthly_returns))
    if len(filtered_benchmark_monthly_returns) <= 1:
        return 0
    corr = ss.stats.pearsonr(filtered_benchmark_monthly_returns, filtered_strategy_monthly_returns)
    return corr[0]

# For some benchmarks we may have to load monthly returns directly
def get_benchmark_monthly_returns(start_date, end_date, product):
    if benchmarks[product] == 'daily_prices':
        dates, prices = fetch_prices(product, str(start_date), str(end_date))
        daily_log_returns = compute_daily_log_returns(prices)
        return convert_daily_to_monthly_returns(dates[1:], daily_log_returns)
