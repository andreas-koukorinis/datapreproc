# cython: profile=True
import math
import numpy
from datetime import timedelta
from dbqueries import fetch_prices, fetch_monthly_returns_benchmark
from calculate import compute_daily_log_returns, convert_daily_returns_to_yyyymm_monthly_returns_pair, compute_correlation
from performance.performance_utils import drawdown, annualized_returns, annualized_stdev, annualized_returns_from_monthly,  annualized_stdev_from_monthly

benchmarks = { 'VBLTX' : 'daily_prices' , 'VTSMX' : 'daily_prices', 'AQRIX' : 'daily_prices', 'AQMIX':'daily_prices', 'AHLDIV':'monthly_returns', 'BTOP50':'monthly_returns', 'TULIP':'monthly_returns'}

def get_benchmark_stats(dates_strategy, daily_log_returns_strategy, benchmark): # TODO change to combined query for fetching all benchmarks data at once
    '''Returns the following benchmark stats: sharpe, drawdown, correlation to strategy'''
    if benchmarks[benchmark] == 'daily_prices': # If we have daily prices for this benchmark in db
        dates_benchmark, prices_benchmark = fetch_prices(benchmark, str(dates_strategy[0]), str(dates_strategy[-1])) # fetch the dates and corresponding prices
        daily_log_returns_benchmark = compute_daily_log_returns(prices_benchmark) 
        net_returns_benchmark = (math.exp(numpy.sum(daily_log_returns_benchmark)) - 1) * 100.0
        sharpe_benchmark = annualized_returns(daily_log_returns_benchmark) / annualized_stdev(daily_log_returns_benchmark)
        drawdown_benchmark = abs(math.exp(drawdown(daily_log_returns_benchmark)) - 1) * 100.0
        labels_monthly_log_returns_benchmark = convert_daily_returns_to_yyyymm_monthly_returns_pair(dates_benchmark[1:], daily_log_returns_benchmark)
        # skip one date because extra date fetched to compute log_ret on start_date
    elif benchmarks[benchmark] == 'monthly_returns':
        dates_benchmark, rates_benchmark = fetch_monthly_returns_benchmark(benchmark, str(dates_strategy[0]), str(dates_strategy[-1]))
        monthly_log_returns_benchmark = numpy.log(1+0.01*rates_benchmark.astype(float))
        net_returns_benchmark = (math.exp(numpy.sum(monthly_log_returns_benchmark)) - 1) * 100.0
        sharpe_benchmark = annualized_returns_from_monthly(monthly_log_returns_benchmark)/ annualized_stdev_from_monthly(monthly_log_returns_benchmark)
        drawdown_benchmark = abs(math.exp(drawdown(monthly_log_returns_benchmark))-1) * 100.0        
        labels_monthly_log_returns_benchmark = convert_daily_returns_to_yyyymm_monthly_returns_pair(dates_benchmark, monthly_log_returns_benchmark)

    labels_monthly_log_returns_strategy = convert_daily_returns_to_yyyymm_monthly_returns_pair(dates_strategy, daily_log_returns_strategy)
    monthly_correlation = compute_correlation(labels_monthly_log_returns_benchmark, labels_monthly_log_returns_strategy)
    stats = '%s_net_returns = %0.2f\n%s_sharpe = %0.2f\n%s_drawdown = %0.2f\n%s_correlation = %0.2f\n' % (benchmark, net_returns_benchmark, benchmark, sharpe_benchmark, benchmark, drawdown_benchmark, benchmark, monthly_correlation)

    return stats
