import argparse
from datetime import date, datetime
import math
import sys
import numpy as np
import pandas as pd

parse = lambda x: datetime.strptime(x, '%Y%m%d')

# Helper function from perfomance_utils to get stats
def rolling_window(input_series, window_length):
    shape = input_series.shape[:-1] + (input_series.shape[-1] - window_length + 1, window_length)
    strides = input_series.strides + (input_series.strides[-1],)
    return np.lib.stride_tricks.as_strided(input_series, shape=shape, strides=strides)

def get_stdev_annual_returns(daily_log_returns):
    if daily_log_returns.shape[0] < 252:
        stdev_annual_returns = annualized_stdev(daily_log_returns)
    else:
        stdev_annual_log_returns = np.std(np.sum(rolling_window(daily_log_returns, 252), 1))
        stdev_annual_returns = ((math.exp(stdev_annual_log_returns) - 1) + (1 - math.exp(-stdev_annual_log_returns)))/2.0 * 100
        stdev_annual_returns = max(1.0, min(200.0, stdev_annual_returns))
    return stdev_annual_returns   

def annualized_returns(daily_log_returns):
    if daily_log_returns.shape[0] < 1:
        annualized_returns = 1.0
    else:
        annualized_returns = (math.exp(np.mean(daily_log_returns) * 252.0) - 1) * 100
    return annualized_returns

def annualized_stdev(daily_log_returns):
    if daily_log_returns.shape[0] < 2:
        annualized_stdev = 100.0
    else:
        _estimate_of_annual_range = math.sqrt(252.0) * np.std(daily_log_returns) # under normal distribution
        annualized_stdev = ((math.exp(_estimate_of_annual_range) - 1) + (1 - math.exp(-_estimate_of_annual_range)))/2.0 * 100.0 
        annualized_stdev = max(1.0, min(200.0, annualized_stdev))
    return annualized_stdev

def drawdown(returns):
    """Calculates the global maximum drawdown i.e. the maximum drawdown till now"""
    if returns.shape[0] < 2:
        return 0.0
    cum_returns = returns.cumsum()
    return -1.0*max(np.maximum.accumulate(cum_returns) - cum_returns) # Will return a negative value

def get_log_returns_from_prices(prices):
    daily_log_returns = []
    for i in xrange(len(prices)-1):
        daily_log_returns.append(math.log(float(prices[i+1])/float(prices[i])))
    return np.array(daily_log_returns)

def get_date_index(date, dates):
    idx = 0
    while idx < len(dates):
        if dates[idx].date() >= date:
            return idx
        idx += 1
    return -1

def mtd_returns(dates, daily_log_returns):
    _current_date = dates[-1].date()
    _mtd_start_date = date(_current_date.year, _current_date.month, 1)
    _mtd_end_date = _current_date
    _month_start_idx = get_date_index(_mtd_start_date, dates)
    _month_end_idx = get_date_index(_mtd_end_date, dates)
    _mtd_log_returns = np.sum(daily_log_returns[_month_start_idx:_month_end_idx])
    mtd_returns = (math.exp(_mtd_log_returns) - 1) * 100.0 
    return mtd_returns

def ytd_returns(dates, daily_log_returns):
    _current_date = dates[-1].date()
    _ytd_start_date = date(_current_date.year, 1, 1)
    _ytd_end_date = _current_date
    _ytd_start_idx = get_date_index(_ytd_start_date, dates)
    _ytd_end_idx = get_date_index(_ytd_end_date, dates)
    _ytd_log_returns = np.sum(daily_log_returns[_ytd_start_idx:_ytd_end_idx])
    ytd_returns = (math.exp(_ytd_log_returns) - 1) * 100.0
    return ytd_returns

def fiveytd_returns(dates, daily_log_returns):
    _current_date = dates[-1].date()
    _fiveytd_start_date = date(_current_date.year-4, 1, 1)
    _fiveytd_end_date = _current_date
    _fytd_start_idx = get_date_index(_fiveytd_start_date, dates)
    _fytd_end_idx = get_date_index(_fiveytd_end_date, dates)
    _fiveytd_log_returns = np.sum(daily_log_returns[_fytd_start_idx:_fytd_end_idx])
    fiveytd_returns = (math.exp(_fiveytd_log_returns) - 1) * 100.0
    return fiveytd_returns


# Aggragates content from a series of prices, exchange rates, interest rates etc.
# Looks for files in <path_t-file>/<product's first letter>/
# Used first for country wise aggregation of stock, exchange rates and interest rates
def content_for_index(symbol, path_to_file, output_path, lookback):
    
    #df = pd.read_csv(path_to_file+symbol[0]+'/'+symbol+'.csv', index_col=0, parse_dates=['date'], date_parser=parse, names=['date','open','high','low','close','volume','dividend'])
    df = pd.read_csv(path_to_file+symbol+'.csv', index_col=0, parse_dates=['date'], date_parser=parse, names=['date','open','high','low','close','volume','dividend','x','y'])

    dates = df.index[1:].to_pydatetime()
    results = pd.DataFrame(index=dates)
    results.index.name = 'date'
    results['close'] = df['close'].iloc[1:]
    daily_log_returns = get_log_returns_from_prices(df['close'].values)

    mtd = []
    ytd = []
    fytd = []
    sharpe = []
    ret_to_dd = []
    annual_volatility = []
    returns = []

    for i in xrange(1,len(df.index)):
        mtd.append(mtd_returns(dates[:i], daily_log_returns[:i]))
        ytd.append(ytd_returns(dates[:i], daily_log_returns[:i]))
        fytd.append(fiveytd_returns(dates[:i], daily_log_returns[:i]))
        annual_volatility.append(get_stdev_annual_returns(daily_log_returns[:i]))
        if lookback == -1:
            sharpe.append(annualized_returns(daily_log_returns[:i])/annualized_stdev(daily_log_returns[:i]))
            returns.append(annualized_returns(daily_log_returns[:i]))
            if drawdown(daily_log_returns[:i]) == 0 :
                ret_to_dd.append(float('inf'))
            else:
                ret_to_dd.append(annualized_returns(daily_log_returns[:i])/abs((math.exp(drawdown(daily_log_returns[:i])) - 1) * 100))
        else:
            sharpe.append(annualized_returns(daily_log_returns[max(0,i-lookback):i])/annualized_stdev(daily_log_returns[max(0,i-lookback):i]))
            returns.append(annualized_returns(daily_log_returns[max(0,i-lookback):i]))
            if drawdown(daily_log_returns[max(0,i-lookback):i]) == 0 :
                ret_to_dd.append(float('inf'))
            else:
                ret_to_dd.append(annualized_returns(daily_log_returns[max(0,i-lookback):i])/abs((math.exp(drawdown(daily_log_returns[max(0,i-lookback):i])) - 1) * 100))
   
    
    results['returns'] = returns
    results['mtd'] = mtd    
    results['ytd'] = ytd    
    results['fytd'] = fytd    
    results['sharpe'] = sharpe
    results['ret_to_dd'] = ret_to_dd
    results['annual_volatility'] = annual_volatility
    

    results.to_csv(output_path+symbol+'_content.csv')

# Run python countrywise_aggregation -h for help
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('type')
    parser.add_argument('-s','--symbols', nargs='+', help='<Required> List of symbles to find content for', required=True, dest='symbols')
    parser.add_argument('-o','--output_path', type=str, help='Output path for content of symbols', dest='output_path', default='')
    parser.add_argument('-l','--lookback', type=int,dest='lookback', help='lookback period for sharpe, returns and ret_to_dd ratio', default=-1)
    parser.add_argument('-p','--pathtofile', type=str,dest='path_to_file', help='Path where files', default= '/apps/data/csi/CVHISTORICAL_STOCKS/')
    args = parser.parse_args()
    
    if args.type == 'index':
        for symbol in args.symbols:
            content_for_index(symbol, args.path_to_file, args.output_path, args.lookback)
