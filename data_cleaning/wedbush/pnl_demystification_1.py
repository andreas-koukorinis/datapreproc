import argparse
import os
import pandas as pd
import math
import sys
import smtplib
import numpy
import json
import urllib2
import pandas as pd
import MySQLdb
import scipy.stats as ss
from datetime import datetime, date, timedelta

home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_conversion_factors, db_connect, connect_to_db, fetch_prices, fetch_latest_prices_v1
from performance.performance_utils import annualized_returns, annualized_stdev, drawdown, mean_lowest_k_percent, compute_omega_ratio, compute_sortino, compute_skew, compute_hit_loss_ratio, compute_gain_pain_ratio
from utils.print_stats import get_stats_string

db = None
db_cursor = None
basename_to_currency = {}
conversion_factor = {}
price = {}
sector = {
            '6A' : 'Currency',
            '6C' : 'Currency',
            '6J' : 'Currency',
            '6M' : 'Currency',
            '6N' : 'Currency',
            'ES' : 'Equity',
            'FDAX' : 'Equity',
            'FTI' : 'Equity',
            'SXF' : 'Equity',
            'FESX' : 'Equity',
            'FSMI' : 'Equity',
            'ZN' : 'Fixed_Income',
            'ZF' : 'Fixed_Income',
            'ZB' : 'Fixed_Income',
            'FGBM' : 'Fixed_Income',
            'FGBL' : 'Fixed_Income',
            'CGB' : 'Fixed_Income',
            'LFR' : 'Fixed_Income'
}
# 1. signal_positions : stores number of contracts that each signal wants for products on that day, open_equity and realized pnl if any for that combination, all in home currency
# 2. Currently we don't have data stored for position of each product for each signal, but going forward we will store that to break down signals' return by product
# 3. unsettled_orders : stores all orders that are not settles right now. Sum of # of positions till yesterday should be equal to net position yesterday.
# 3. For today's positions taken, if it isof same sign as net position yesterday, we simply update unsettled_order by appending today's order,
# 3. else, based on chosen LIFO/FIFO, update unsettled orders in DB
# 4. broker_portfolio_stats : using this table to get log returns of portfolio - log(pnl / (portfolio_value - pnl)) ASSUMPTION: any cash transfer is done at the end after calculating pnl for day
# 5. ASSUMPTION: Any foreign currency pnl, if converted to USD (home currency) is done at final reported rate for the day as used in mark to market of portfolio
# 6. Start date in simluation implies all returns post start date till EOD end date. For example, if start date is 2015-05-07 and end date is 2015-05-08, return is change in portfolio on 2015-05-08
# 7. For breakup among products, calculation will be summed only during period when no cash transfer was done

def send_mail(_subject, _body):
    _server = "localhost"
    _from = "nilesh.choubey@tworoads.co.in"
    _to = "nilesh.choubey@tworoads.co.in, sanchit.gupta@tworoads.co.in"
    # Prepare actual message
    message = "From: {0}\nTo: {1}\nSubject: {2}\n\n{3}".format(_from, _to, _subject, _body)
    # Send the mail
    server = smtplib.SMTP(_server)
    server.sendmail(_from, _to, message)
    server.quit()

def get_stats_from_date(start_date, ret_matrix, benchmarks):
    daily_log_returns = numpy.array([])
    dates = numpy.array([])
    for idx in xrange(len(ret_matrix)):
        if ret_matrix[idx,0] >= start_date:
            daily_log_returns = numpy.append(daily_log_returns, ret_matrix[idx,3])
            dates = numpy.append(dates, ret_matrix[idx,0])
    stats = {}
    stats['net_ret'] = (math.exp(numpy.sum(daily_log_returns)) - 1) * 100.0
    stats['ann_ret'] = annualized_returns(daily_log_returns)
    stats['ann_std'] = annualized_stdev(daily_log_returns)
    stats['sharpe'] = stats['ann_ret']/stats['ann_std']
    stats['max_dd'] = abs((math.exp(drawdown(daily_log_returns)) - 1) * 100)
    stats['ret_dd'] = stats['ann_ret']/stats['max_dd'] if stats['max_dd'] > 0 else float('NaN')
    stats['omega'] = compute_omega_ratio(daily_log_returns, 0.0)
    stats['sortino'] = compute_sortino(daily_log_returns)
    stats['skew'] = compute_skew(daily_log_returns)
    stats['kurtosis'] = ss.kurtosis(daily_log_returns)
    stats['d_var'] = (math.exp(mean_lowest_k_percent(daily_log_returns, 10)) - 1)*100.0
    stats['ret_var10'] = stats['ann_ret']/abs(min(-0.0001, stats['d_var']))
    stats['hit_loss'] = compute_hit_loss_ratio(daily_log_returns)
    stats['gain_pain'] = compute_gain_pain_ratio(daily_log_returns)

    out = get_stats_string(stats)
    out += '\nBenchmarks:\n'
    benchmark_stats = {}
    for benchmark in benchmarks:
        benchmark_stats.update(get_benchmark_stats(dates, daily_log_returns, benchmark)) # Returns a string of benchmark stats
    out += get_stats_string(benchmark_stats)
    return out

def get_todays_benchmark_returns(benchmarks, current_date):
    benchmark_returns = []
    for benchmark in benchmarks:
        dates, prices = fetch_prices(benchmark, current_date - timedelta(days=10), current_date)
        if dates[-1] == current_date.date() and len(dates) > 1:
            _return = (prices[-1] - prices[-2])*100.0/prices[-2]
        else:
            _return = 0.0
        benchmark_returns.append((benchmark, _return))
    return benchmark_returns

def get_currency_rates( current_date ):
    currency_rates = {}
    query = "SELECT currency, rate FROM currency_rates WHERE date = '%s'" % ( current_date )
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    for row in rows:
        currency_rates[row['currency']] = float( row['rate'] )
    return currency_rates

def get_product_currencies( ):
    basename_to_currency = {}
    query = "SELECT product, currency FROM commission_rates"
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    for row in rows:
        basename_to_currency[row['product']] = row['currency']
    return basename_to_currency

def get_products_pnl(current_date):
    product_pnl_strategy = {}
    product_pnl_inventory = {}
    query = "SELECT date FROM inventory WHERE date <= '%s' group by 1 ORDER by date desc LIMIT 2" % ( current_date )
    db_cursor.execute(query)
    rows1 = db_cursor.fetchall()
    
    if len(rows1) >= 1: 
        currency_rates = get_currency_rates( rows1[0]['date'] )
        query = "SELECT date, SUBSTRING(product, 1, CHAR_LENGTH(product)-3) as base_product, product, inventory_USD_bal, strategy_bal, inventory_bal, \
                 inventory_ote  FROM inventory WHERE date= '%s'" % ( rows1[0]['date'] )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            basename = row['base_product']
            product = row['product']
            product_pnl_strategy[basename] = product_pnl_strategy.get(basename, 0.0 ) + float(row['strategy_bal']) * currency_rates[basename_to_currency[basename]]
            product_pnl_inventory[basename] = product_pnl_inventory.get(basename, 0.0 ) + float(row['inventory_USD_bal']) + \
                                              ( float(row['inventory_bal']) +float(row['inventory_ote']) ) * currency_rates[basename_to_currency[basename]]
    if len(rows1) == 2: 
        currency_rates = get_currency_rates( rows1[1]['date'] )
        query = "SELECT date, SUBSTRING(product, 1, CHAR_LENGTH(product)-3) as base_product, product,  strategy_bal, inventory_bal, \
                 inventory_ote  FROM inventory WHERE date= '%s'" % ( rows1[1]['date'] )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            basename = row['base_product']
            product = row['product']
            product_pnl_strategy[basename] -= float(row['strategy_bal']) * currency_rates[basename_to_currency[basename]]
            product_pnl_inventory[basename] -= ( float(row['inventory_ote']) + float(row['inventory_bal'])  ) * currency_rates[basename_to_currency[basename]] 
    return product_pnl_strategy, product_pnl_inventory

def get_net_products_pnl(current_date):
    net_product_pnl_strategy = {}
    net_product_pnl_inventory = {}
    currency_rates = get_currency_rates( current_date )
    query = "SELECT SUBSTRING(a.product, 1, CHAR_LENGTH(a.product)-3) as base_product, a.product as product,  strategy_bal, inventory_bal, inventory_USD_bal,\
             inventory_ote  FROM inventory as a JOIN ( SELECT product, max(date) as recent_dt FROM inventory GROUP BY 1) as b WHERE a.product=b.product AND a.date = b.recent_dt"
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    
    for row in rows:
        basename = row['base_product']
        product = row['product']
        net_product_pnl_strategy[basename] = net_product_pnl_strategy.get(basename, 0.0) + float(row['strategy_bal']) * currency_rates[basename_to_currency[basename]]
        net_product_pnl_inventory[basename] = net_product_pnl_inventory.get(basename, 0.0) + float(row['inventory_USD_bal']) +\
                                              ( float(row['inventory_ote']) + float(row['inventory_bal'])  ) * currency_rates[basename_to_currency[basename]] 
    return net_product_pnl_strategy, net_product_pnl_inventory
    

# TODO get from broker
def get_commission( current_date ):
    query = "SELECT SUM(commission) AS comm FROM estimated_portfolio_stats WHERE date <= '%s'" % current_date
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    return float(rows[0]['comm'])

# TODO get from broker
def get_turnover( current_date, portfolio_value ):
    all_currency_rates = {}
    # Get all currency rates
    query = "SELECT date, currency, rate FROM currency_rates WHERE date <= '%s'" % current_date
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    for row in rows:
        if row['date'] not in all_currency_rates.keys():
            all_currency_rates[row['date']] = {}
        all_currency_rates[row['date']][row['currency']] = float(row['rate'])
    
    # Get number of trading days
    query = "SELECT COUNT(*) AS num_days FROM broker_portfolio_stats WHERE date <= '%s'" % current_date
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    num_trading_days = float(rows[0]['num_days']) - 1

    # Get all orders
    # TODO Do not include initial buys
    query = "SELECT date, product, fill_amount, fill_price FROM actual_orders WHERE date <= '%s' AND date > '2015-04-27'" % current_date
    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    long_amount_transacted = 0.0
    short_amount_transacted = 0.0
    for row in rows:
        basename = row['product'][:-3]
        if row['fill_amount'] > 0:
            long_amount_transacted += row['fill_amount'] * conversion_factor[basename] * float(row['fill_price']) * all_currency_rates[row['date']][basename_to_currency[basename]]
        else:
            short_amount_transacted += abs(row['fill_amount']) * conversion_factor[basename] * float(row['fill_price']) * all_currency_rates[row['date']][basename_to_currency[basename]]
        #TODO num days  > 252
    return (252.0*100.0/num_trading_days) * min(long_amount_transacted, short_amount_transacted)/portfolio_value

def get_leverage( current_date, positions, portfolio_value ):
    leverage = 0.0
    currency_rates = get_currency_rates( current_date )
    for product in positions.keys():
        basename = product[:-3]
        leverage += abs(positions[product] * conversion_factor[basename] * price[product] * currency_rates[basename_to_currency[basename]])
    leverage /= portfolio_value
    return leverage

def get_factors( current_date, exchange_symbols ):
    global conversion_factor
    global price
    products = [ exchange_symbol[:-3] + '_1' for exchange_symbol in exchange_symbols ]
    (csidb,csidb_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    csidb_cursor.execute("SELECT product,conversion_factor FROM products WHERE product IN (%s)" % _format_strings,tuple(products))
    rows = csidb_cursor.fetchall()
    for row in rows:
        conversion_factor[row['product'][:-2]] = float(row['conversion_factor'])
    price = fetch_latest_prices_v1(exchange_symbols, current_date, 'future') # Uses exchange symbols

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-cd', type=str, help='PnL-Comparison Date\nEg: -cd 2015-04-27\n',default=None, dest='current_date')
    parser.add_argument('--check', help='Do not send to slack\nEg: --check\n Default is to send to slack', default=False, dest='check', action='store_true')
    args = parser.parse_args()
    benchmarks = ['VBLTX', 'VTSMX', 'AQRIX', 'AQMIX']

    global db
    global db_cursor
    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )
        print traceback.format_exc()

    global basename_to_currency 
    basename_to_currency = get_product_currencies()

    # Fetch starting portfolio value
    query = "SELECT portfolio_value FROM broker_portfolio_stats ORDER BY date ASC LIMIT 1"
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    initial_pv = 0.0
    if len(rows) == 1:
        initial_pv = float(rows[0]['portfolio_value'])

    # Fetch the portfolio value for YDAY and TODAY
    query = "SELECT portfolio_value FROM broker_portfolio_stats WHERE date <= '%s' ORDER BY date DESC LIMIT 2" % ( args.current_date )
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    yday_pv = 100000000.0
    today_pv = 0.0
    if len(rows) >= 1:
        today_pv = float(rows[0]['portfolio_value'])
    if len(rows) == 2:
        yday_pv = float(rows[1]['portfolio_value'])

    # Get sector returns and strategy, inventory pnl
    sector_ret_str = ""
    product_pnl_strategy, product_pnl_inventory = get_products_pnl( args.current_date )
    strategy_pnl = 0.0
    inventory_pnl = 0.0
    strategy_sector_pnl = {}
    inventory_sector_pnl = {}
    for key in product_pnl_strategy.keys():
        strategy_sector_pnl[sector[key]] = strategy_sector_pnl.get( sector[key], 0.0 ) + product_pnl_strategy[key]
        inventory_sector_pnl[sector[key]] = inventory_sector_pnl.get( sector[key], 0.0 ) + product_pnl_inventory[key]
        strategy_pnl += product_pnl_strategy[key]
        inventory_pnl += product_pnl_inventory[key]

    for key in strategy_sector_pnl.keys():
        sector_ret_str += '%s | %0.2f%% | %0.2f%% | %0.2f%%\n' % ( key, 100.0 * strategy_sector_pnl[key]/yday_pv, 100.0 * inventory_sector_pnl[key]/yday_pv,\
                                                             100.0 * ( strategy_sector_pnl[key] + inventory_sector_pnl[key] )/yday_pv )

    products = []
    # Get Product Positions
    positions_str = ""
    net_positions = {}
    query = "SELECT product, net_position, strategy_position, inventory_position FROM inventory WHERE date = '%s'" % ( args.current_date )
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    for row in rows:
        products.append(row['product'])
        net_positions[row['product']] = row['net_position']
        positions_str += '%s | %0.2f | %0.2f | %d\n' % ( row['product'], float(row['strategy_position']), float(row['inventory_position']), row['net_position'])

    get_factors( args.current_date, products )

    # Get net PNL per product
    net_product_pnl_strategy, net_product_pnl_inventory = get_net_products_pnl( args.current_date )
    net_pnl_strategy = 0.0
    net_pnl_inventory = 0.0
    net_sector_ret_str = ""
    net_strategy_sector_pnl = {}
    net_inventory_sector_pnl = {}
    for key in net_product_pnl_strategy.keys():
        net_pnl_strategy += net_product_pnl_strategy[key]
        net_pnl_inventory += net_product_pnl_inventory[key]
        net_strategy_sector_pnl[sector[key]] = net_strategy_sector_pnl.get( sector[key], 0.0 ) + net_product_pnl_strategy[key]
        net_inventory_sector_pnl[sector[key]] = net_inventory_sector_pnl.get( sector[key], 0.0 ) + net_product_pnl_inventory[key]

    for key in net_strategy_sector_pnl.keys():
        net_sector_ret_str += '%s | %0.2f%% | %0.2f%% | %0.2f%%\n' % ( key, 100.0 * net_strategy_sector_pnl[key]/initial_pv, 100.0 * net_inventory_sector_pnl[key]/initial_pv,\
                                                             100.0 * ( net_strategy_sector_pnl[key] + net_inventory_sector_pnl[key] )/initial_pv )

    # Get benchmark returns
    benchmark_returns = get_todays_benchmark_returns(benchmarks, datetime.strptime(args.current_date, '%Y%m%d'))
    _print_benchmark_returns = ''
    for item in benchmark_returns:
        _print_benchmark_returns += '%s : %0.2f%%\t' % (item[0], item[1])
        
    output = '------------------------------------------------------\n*PNL Demystification Report for Date: %s*\n------------------------------------------------------' % (args.current_date)
 
    output += '\n*LTD Pnl ( Return )* :    Net Pnl : $%0.2f (%0.2f %%)   |   Strategy Pnl : $%.2f (%0.2f%%)   |   Inventory Pnl : $%.2f (%0.2f%%)\n' % ( (today_pv - initial_pv), 100.0 * (today_pv - initial_pv)/initial_pv, net_pnl_strategy, 100.0*net_pnl_strategy/initial_pv, net_pnl_inventory, 100.0*net_pnl_inventory/initial_pv )
    output += '\n*Todays Pnl ( Return )* :   Net Pnl : $%0.2f (%0.2f %%)   |   Strategy Pnl : $%.2f (%0.2f%%)   |   Inventory Pnl : $%.2f (%0.2f%%)\n' % ( (today_pv - yday_pv), 100.0 * (today_pv - yday_pv)/yday_pv, strategy_pnl, 100.0 * strategy_pnl/yday_pv, inventory_pnl, 100.0 * inventory_pnl/yday_pv )
    output += '\n*Summary Stats* :   Turnover : %0.2f%%   |   Leverage : %0.2f   |   Commission : $%0.2f\n' % ( get_turnover( args.current_date, today_pv ), \
                get_leverage(args.current_date, net_positions, today_pv), get_commission( args.current_date ) )
    output += "\n*Benchmark Returns:*  %s\n" % _print_benchmark_returns
    output += '\n*Positions*\n'
    output += '*Product  |  Strategy  |  Inventory  |  Net*\n'
    output += positions_str
    output += '\n*Sector Return LTD*\n'
    output += '*Product  |  Strategy  |  Inventory  |  Net*\n'
    output += net_sector_ret_str
    output += '\n*Sector Return Today*\n'
    output += '*Product  |  Strategy  |  Inventory  |  Net*\n'
    output += sector_ret_str

    if args.check:
        print output
    else:
        payload = {"channel": "#portfolio-monitor", "username": "monitor", "text": output}
        req = urllib2.Request('https://hooks.slack.com/services/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')
        response = urllib2.urlopen(req, json.dumps(payload))

if __name__ == '__main__':
    main()
