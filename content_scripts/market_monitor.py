import argparse
from datetime import date, datetime, timedelta
import math
import sys
import numpy as np
import pandas as pd
import MySQLdb

# Helper function from perfomance_utils to get stats
def rolling_window(input_series, window_length):
    shape = input_series.shape[:-1] + (input_series.shape[-1] - window_length + 1, window_length)
    strides = input_series.strides + (input_series.strides[-1],)
    return np.lib.stride_tricks.as_strided(input_series, shape=shape, strides=strides)

def get_stdev_annual_returns(daily_log_returns):
    if daily_log_returns.shape[0] <= 252:
        stdev_annual_returns = annualized_stdev(daily_log_returns)
    else:
        stdev_annual_log_returns = np.std(np.sum(rolling_window(daily_log_returns, 252), 1))
        stdev_annual_returns = ((math.exp(stdev_annual_log_returns) - 1) + (1 - math.exp(-stdev_annual_log_returns)))/2.0 * 100
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

def get_log_returns_from_yields(yields):
    daily_log_returns = []
    for i in xrange(len(yields)-1):
        y1 = float(yields[i])
        y2 = float(yields[i+1])
        daily_log_returns.append(math.log(1 - 1/((1+y2/100.0)**10) + 1/((1+y1/100.0)**10) + y1/100.0/252))
    return np.array(daily_log_returns)

def get_date_index(date, dates):
    idx = 0
    while idx < len(dates):
        if dates[idx] >= date:
            return idx
        idx += 1
    return -1

def mtd_returns(dates, daily_log_returns):
    _current_date = dates[-1]
    _mtd_start_date = date(_current_date.year, _current_date.month, 1)
    _mtd_end_date = _current_date
    _month_start_idx = get_date_index(_mtd_start_date, dates)
    _month_end_idx = get_date_index(_mtd_end_date, dates)
    _mtd_log_returns = np.sum(daily_log_returns[_month_start_idx:_month_end_idx])
    mtd_returns = (math.exp(_mtd_log_returns) - 1) * 100.0 
    return mtd_returns

def ytd_returns(dates, daily_log_returns):
    _current_date = dates[-1]
    _ytd_start_date = date(_current_date.year, 1, 1)
    _ytd_end_date = _current_date
    _ytd_start_idx = get_date_index(_ytd_start_date, dates)
    _ytd_end_idx = get_date_index(_ytd_end_date, dates)
    _ytd_log_returns = np.sum(daily_log_returns[_ytd_start_idx:_ytd_end_idx])
    ytd_returns = (math.exp(_ytd_log_returns) - 1) * 100.0
    return ytd_returns

def fiveytd_returns(dates, daily_log_returns):
    _current_date = dates[-1]
    _fiveytd_start_date = date(_current_date.year-4, 1, 1)
    _fiveytd_end_date = _current_date
    _fytd_start_idx = get_date_index(_fiveytd_start_date, dates)
    _fytd_end_idx = get_date_index(_fiveytd_end_date, dates)
    _fiveytd_log_returns = np.sum(daily_log_returns[_fytd_start_idx:_fytd_end_idx])
    fiveytd_returns = (math.exp(_fiveytd_log_returns) - 1) * 100.0
    return fiveytd_returns

def ly_returns(dates, daily_log_returns):
    _current_date = dates[-1]
    _ly_start_date = date(_current_date.year-1, _current_date.month, _current_date.day)
    _ly_end_date = _current_date
    _ly_start_idx = get_date_index(_ly_start_date, dates)
    _ly_end_idx = get_date_index(_ly_end_date, dates)
    _ly_log_returns = np.sum(daily_log_returns[_ly_start_idx:_ly_end_idx])
    ly_returns = (math.exp(_ly_log_returns) - 1) * 100.0
    return ly_returns

def last_n_days_returns(dates, daily_log_returns, lookback):
    _current_date = dates[-1]
    _last_n_days_start_date = date(_current_date.year, _current_date.month, _current_date.day) - timedelta(days=lookback)
    _last_n_days_end_date = _current_date
    _last_n_days_start_idx = get_date_index(_last_n_days_start_date, dates)
    _last_n_days_end_idx = get_date_index(_last_n_days_end_date, dates)
    last_n_days_log_returns = daily_log_returns[_last_n_days_start_idx:_last_n_days_end_idx]
    return last_n_days_log_returns

def db_connect():
    global db, db_cursor
    try:
        with open('/spare/local/credentials/readonly_credentials.txt') as f:
            credentials = [line.strip().split(':') for line in f.readlines()]
    except IOError:
        sys.exit('No credentials file found')
    try:
        for user_id,password in credentials:
            db = MySQLdb.connect(host='fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com', user=user_id, passwd=password, db='daily_qplum')
            db_cursor = db.cursor(MySQLdb.cursors.DictCursor) 
    except MySQLdb.Error:
        sys.exit("Error in DB connection")

def db_close():
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close() 

def fetch_product_information(products):
    format_strings = ','.join(["'%s'"] * len(products))
    query = "SELECT * FROM products WHERE product IN (" +format_strings + ")"
    query = query%tuple(products)
    products_df = pd.read_sql(query, con=db)
    return products_df

def fetch_latest_prices(products_df, earliest_date):
    cols = {'yield_rates':'rate', 'forex':'close', 'indices':'close'}
    products = products_df['product'].unique()
    tables = products_df['table'].unique()
    columns = ['date', 'product', 'close']
    results_df = pd.DataFrame(data=np.zeros((0,len(columns))), columns=columns)
    for table in tables:
        format_strings = ','.join(["'%s'"] * len(products))
        query = "SELECT date, product, %s as close FROM %s WHERE product IN (" +format_strings + ") AND date >= '%s' ORDER BY date"
        parameters = [cols[table], table] + list(products) + [earliest_date]
        query = query%tuple(parameters)
        result_df = pd.read_sql(query, con=db)
        results_df = results_df.append(result_df, ignore_index=True)
    return results_df    

def prepare_content(results_df, products_df, lookbacks):
    products = products_df['product'].unique()
    results = []
    for product in products:
        df = results_df[results_df['product']==product]
        dates = df['date'].values
        if products_df[products_df['product']==product]['table'].values[0] == 'yield_rates':
            daily_log_returns = get_log_returns_from_yields(df['close'].values) # in maximum lookback period
        else:
            daily_log_returns = get_log_returns_from_prices(df['close'].values) # in maximum lookback period
        mtd_ret = mtd_returns(dates, daily_log_returns)
        ytd_ret = ytd_returns(dates, daily_log_returns)
        last_year_ret = ly_returns(dates, daily_log_returns)
        sharpe = []
        ret_dd = []
        ann_volatility = []
        for lookback in lookbacks:
            _last_n_days_daily_returns = last_n_days_returns(dates, daily_log_returns, lookback)
            _ret = annualized_returns(_last_n_days_daily_returns)
            _dd = drawdown(_last_n_days_daily_returns) 
            _stdev = annualized_stdev(_last_n_days_daily_returns)
            if _dd == 0 :
                sharpe.append(float('inf'))
            else:
                sharpe.append(_ret/_stdev)
            if _dd == 0 :
                ret_dd.append(float('inf'))
            else:
                ret_dd.append(_ret/abs((math.exp(_dd) - 1) * 100))
            ann_volatility.append(_stdev)
        prod_df = products_df[(products_df['product'] == product)]
        result = {'product': product, 'description': prod_df.iloc[0]['name'], 'type': prod_df.iloc[0]['type'],\
                  'mtd_ret': mtd_ret, 'ytd_ret': ytd_ret, 'last_year_ret': last_year_ret, 'sharpe':sharpe, 'ret_dd': ret_dd, 'ann_volatility': ann_volatility}
        results.append(result)
    return results

def get_market_monitor_content(products=None, lookback=[365, 730], fetch_date=None):
    if products == None:
        # Take products required by market monitor as default
        products = ['SPY', 'TCMP', '^MXX', '^BVSP', '^FTSE', '^GDAX', '^N150', '^GU15', '^SSMI', '^N500', '^BSES', '^AXJO', '^NZ50', \
                   'USA10Y', 'CAN10Y', 'GBR10Y', 'DEU10Y', 'FRA10Y', 'ITA10Y', 'CHE10Y', 'JPN10Y', 'IND10Y', 'AUS10Y', 'NZL10Y', \
                   'CADUSD', 'MXNUSD', 'GBPUSD', 'EURUSD', 'CHFUSD', 'JPYUSD', 'INRUSD', 'AUDUSD', 'NZDUSD' ] # MEX10Y
    if fetch_date == None:
        fetch_date = str(date.today()+timedelta(days=-1))

    db_connect()
    products_df = fetch_product_information(products)
    earliest_date = (datetime.strptime(fetch_date, "%Y-%m-%d") + timedelta(days=-max(400,max(lookback)))).strftime("%Y-%m-%d")
    returns_df = fetch_latest_prices(products_df, earliest_date)
    db_close()
    return prepare_content(returns_df, products_df, lookback)

# Run python market_monitor.py -h for help
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--products', nargs='+', help='List of symbols to find content for. Default is Market Monitor product list', dest='products', default=None)
    parser.add_argument('-l','--lookback', nargs='+', type=int, help='lookback periods in days for sharpe, returns and ret_to_dd ratio', dest='lookback', default=[365, 730])
    parser.add_argument('-d', type=str, help='Date\nEg: -d 2014-06-01\n Default is yesterdays date.',default=str(date.today()+timedelta(days=-1)), dest='date')
    args = parser.parse_args()
    print get_market_monitor_content(args.products, args.lookback, args.date)
