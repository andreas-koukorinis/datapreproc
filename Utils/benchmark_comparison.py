from datetime import timedelta
import scipy.stats as ss
from DbQueries import fetch_prices
from Tools.compute_correlation import filter_series
from Utils.Calculate import convert_daily_to_monthly_returns

benchmarks = { 'VBLTX' : 'daily_prices' , 'VTSMX' : 'daily_prices' }

def compute_daily_log_returns(prices):
    return prices[1:]/prices[:-1] 

def get_monthly_correlation_to_benchmark(dates_daily_log_returns, benchmark_name)
    benchmark_monthly_returns = get_benchmark_monthly_returns(dates_daily_log_returns[0][0], dates_daily_log_returns[-1][0], benchmark_name) #TODO start_date off by 1
    strategy_monthly_returns = monthly_returns_from_daily_returns(dates_daily_log_returns)
    filtered_benchmark_monthly_returns, filtered_strategy_monthly_returns = filter_series(benchmark_monthly_returns, strategy_monthly_returns)
    if len(filtered_benchmark_monthly_returns) != len(benchmark_monthly_returns) or len(filtered_benchmark_monthly_returns) != strategy_monthly_returns: # If we skipped some months
        print '%d vs %d vs %d'%(strategy_monthly_returns, benchmark_monthly_returns, filtered_benchmark_monthly_returns)
    corr = ss.stats.pearsonr(filtered_benchmark_monthly_returns, filtered_strategy_monthly_returns)
    return corr

# For some benchmarks we may have to load monthly returns directly
def get_benchmark_monthly_returns(start_date, end_date, product):
    if benchmarks[product] == 'dailyprices':
        dates, prices = fetch_prices(start_date, end_date, product)
        daily_log_returns = compute_daily_log_returns(prices)
        return convert_daily_to_monthly_returns(dates[1:], daily_log_returns)
