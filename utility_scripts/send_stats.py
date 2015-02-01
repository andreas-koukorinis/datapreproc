#!/usr/bin/env python
import re
import sys
import os
import subprocess
import argparse
from datetime import datetime, date, timedelta
import smtplib
from email.mime.text import MIMEText
from prettytable import PrettyTable
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from performance.performance_utils import get_all_stats
from utils.benchmark_comparison import get_benchmark_stats
from utils.regular import get_dates_returns
from utils.dbqueries import fetch_prices

def get_date_index(date, dates):
    idx = 0
    while idx < len(dates):
        if dates[idx] >= date:
            return idx
        idx += 1
    return -1

def parse_results(results):
    results = results.split('\n')
    _dict_results = {}
    for result in results:
        if '=' in result:
            _result = result.split('=')
            _name = _result[0].strip()
            _val = _result[1].strip()
            _dict_results[_name] = _val
    _final_order = ['Net Returns', 'Total Tradable Days','Sharpe Ratio', 'Return_drawdown_Ratio','Return Var10 ratio', 'Annualized_Returns', 'Annualized_Std_Returns', 'Initial Capital', 'Net PNL', 'Annualized PNL', 'Annualized_Std_PnL', 'Skewness','Kurtosis','DML','MML','QML','YML','Max Drawdown','Drawdown Period','Drawdown Recovery Period','Max Drawdown Dollar','Annualized PNL by drawdown','Yearly_sharpe','Hit Loss Ratio','Gain Pain Ratio','Max num days with no new high','Losing month streak','Turnover','Leverage','Trading Cost','Total Money Transacted','Total Orders Placed','Worst 5 days','Best 5 days','Worst 5 weeks','Best 5 weeks']
    _benchmarks = ['VBLTX_sharpe','VBLTX_net_returns','VBLTX_drawdown','VBLTX_correlation','VTSMX_sharpe','VTSMX_net_returns','VTSMX_drawdown','VTSMX_correlation','AQRIX_sharpe','AQRIX_net_returns','AQRIX_drawdown','AQRIX_correlation']

    _print_results = ''
    for _elem in _final_order:
        if _elem in _dict_results.keys():
            _print_results += _elem + ' = ' + _dict_results[_elem] + '\n'
    if len(_benchmarks) > 0:
        _print_results += '\nBenchmarks:\n'
        for _elem in _benchmarks:
            if _elem in _dict_results.keys():
                _print_results += _elem + ' = ' + _dict_results[_elem] + '\n'
    return _print_results

def get_todays_benchmark_returns(benchmarks, current_date):
    benchmark_returns = []
    for benchmark in benchmarks:
        dates, prices = fetch_prices(benchmark, current_date - timedelta(days=10), current_date)
        if dates[-1] == current_date and dates.shape[0] > 1:
            _return = (prices[-1] - prices[-2])*100.0/prices[-2]
        else:
            _return = 0.0
        benchmark_returns.append((benchmark, _return))
    return benchmark_returns

def get_dict(_str):
    _str = _str.strip().split(':',1)[1] # Remove name
    _dict_elements = _str.strip().split('   ')
    _new_dict = {}
    #print _dict_elements
    for i in range(0, len(_dict_elements)):
        _key = _dict_elements[i].strip().split(':')[0].strip()
        _val = _dict_elements[i].strip().split(':')[1].strip()
        _new_dict[_key] = _val
    return _new_dict 

def process_snapshot(_snapshot):
    if len(_snapshot) == 0:
        return ''
    _snap = _snapshot.split('\n')
    _header = _snap[0] + '\t' + _snap[1] + '\t' + _snap[2] + '\t' + _snap[3] + '\t' + _snap[8]
    _open_equity = get_dict(_snap[4])
    _positions = get_dict(_snap[5])
    _notional_allocation = get_dict(_snap[6])
    _average_trade_price = get_dict(_snap[7])
    _data = PrettyTable(["Product", "Open_Equity", "Position", "Notional_Allocation", 'Average_Trade_Price'])
    for _product in sorted(_open_equity.keys()):
        _data.add_row([_product,_open_equity[_product],_positions[_product],_notional_allocation[_product],_average_trade_price[_product]])
    _data.align = "c"
    _str = _data.get_string()
    _str = _str.replace(" ","  ").replace('--','---')
    return _header + '\n\n' + _str

def process_placed_orders(_placed_orders):
    if len(_placed_orders) == 0:
        return ''
    _data = PrettyTable(["Product", "Amount"])
    for _str in _placed_orders:
        if _str == '':  continue
        _str = _str.strip().split('   ')
        _data.add_row([_str[2].strip().split(':')[1].strip(), _str[3].strip().split(':')[1].strip()])
    _data.align = "c"
    _str = _data.get_string()
    _str = _str.replace(" ","  ").replace('--','---')
    return _str
            
def process_filled_orders(_filled_orders):
    if len(_filled_orders) == 0:
        return ''
    _data = PrettyTable(["Product", "Amount", "Cost", "Value", "Fill_Price"])
    for _str in _filled_orders:
        if _str == '':  continue
        _str = _str.strip().split('   ')
        _data.add_row([_str[2].strip().split(':')[1].strip(), _str[3].strip().split(':')[1].strip(), _str[4].strip().split(':')[1].strip(), _str[5].strip().split(':')[1].strip(), _str[6].strip().split(':')[1].strip()])
    _data.align = "c"
    _str = _data.get_string()
    _str = _str.replace(" ","  ").replace('--','---')
    return _str

def todays_return(snapshot):
    snap = snapshot.split('\n')
    todays_pnl = float(snap[1].split(':')[1].strip())
    todays_portfolio_value = float(snap[2].split(':')[1].strip())
    yesterdays_portfolio_value = todays_portfolio_value - todays_pnl
    return (todays_pnl*100.0/yesterdays_portfolio_value)

def send_mail(_subject, _body):
    _server = "localhost"
    #_from = "gchak@circulumvite.com"
    #_to = "cvquant@circulumvite.com"
    #_to = "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in"
    # Prepare actual message
    message = "From: {0}\nTo: {1}\nSubject: {2}\n\n{3}".format(_from, _to, _subject, _body)
    # Send the mail
    server = smtplib.SMTP(_server)
    server.sendmail(_from, _to, message)
    server.quit()

def get_positions(_current_date, _config_file):
    positions_file = home_path + '/logs/'+os.path.splitext(_config_file)[0].split('/')[-1]+'/positions.txt'
    placed_orders = re.compile(r'ORDER PLACED ON %s.*'%str(_current_date))
    filled_orders = re.compile(r"ORDER FILLED ON %s.*"%str(_current_date))
    snapshot = re.compile(r"Portfolio snapshot at EndOfDay %s\n.*PnL for today.*\nPortfolio Value.*\nCash.*\nOpen Equity.*\nPositions.*\nNotional Allocation.*\nAverage Trade Price.*\nLeverage.*"%_current_date)
    fp = open(positions_file)
    lines = fp.read()
    fp.close()
    results_snapshot = snapshot.findall(lines)
    results_placed_orders = placed_orders.findall(lines)
    results_filled_orders = filled_orders.findall(lines)
    _result = ''
    _return = 0
    if len(results_snapshot) > 0:
        _result += process_snapshot(results_snapshot[0])
        _return = todays_return(results_snapshot[0])
    if len(results_placed_orders) > 0:
        _result += '\n\nORDERS PLACED\n' + process_placed_orders(results_placed_orders)
    if len(results_filled_orders) > 0:
        _result += '\n\nORDERS FILLED\n' + process_filled_orders(results_filled_orders)
    return _return, _result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('-s', type=int, help='Mail send or print\nEg: -s 0\n will print the mail body without sending mail', default=1,dest='mail_send')
    parser.add_argument('-sd', type=str, help='Sim Start date\nEg: -sd 2014-06-01\n Default is 2014-01-01',default='2014-01-01', dest='sim_start_date')
    parser.add_argument('-ed', type=str, help='Sim End date\nEg: -ed 2015-01-01\n Default is Yesterday date',default=str(date.today() + timedelta(days=-1)), dest='sim_end_date')
    args = parser.parse_args()
    _config_file = sys.argv[1]
    _current_date = datetime.strptime(args.sim_end_date, "%Y-%m-%d").date()
    _ytd_start_date = date(_current_date.year, 1, 1)
    _ytd_end_date = _current_date
    _mtd_start_date = date(_current_date.year, _current_date.month, 1)
    _mtd_end_date = _current_date
    _yday_start_date = date(_current_date.year , _current_date.month, 1)
    _yday_end_date = _current_date

    _sim_start_date = datetime.strptime(args.sim_start_date, "%Y-%m-%d").date()
    _sim_end_date = _current_date

    benchmarks = ['VBLTX', 'VTSMX', 'AQRIX']
    performance_stats = []

    weekday = date.weekday
    if (weekday == 5 or weekday == 6) and args.mail_send == 1: # For saturday or sunday dont send mail
        sys.exit()

    proc = subprocess.Popen(['python', '-W', 'ignore', 'run_simulator.py', _config_file, str(_sim_start_date), str(_sim_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.communicate()
    performance_stats.append('------------------------------------------------\nYDAY Performance:')
    todays_return, todays_performance = get_positions(_current_date, _config_file)
    performance_stats.append(todays_performance)
   
    returns_file = home_path + '/logs/'+os.path.splitext(_config_file)[0].split('/')[-1]+'/returns.txt' 
    dates, returns = get_dates_returns(returns_file) 
    performance_stats.append('------------------------------------------------\nMTD Performance:')
    _month_start_idx = get_date_index(_mtd_start_date, dates)
    _month_end_idx = len(dates)
    mtd_performance = get_all_stats(dates[_month_start_idx:_month_end_idx], returns[_month_start_idx:_month_end_idx])
    mtd_performance += '\nBenchmarks:\n'
    for benchmark in benchmarks:
        mtd_performance += get_benchmark_stats(dates[_month_start_idx:_month_end_idx], returns[_month_start_idx:_month_end_idx], benchmark) # Returns a string of benchmark stats
    performance_stats.append(mtd_performance)

    performance_stats.append('------------------------------------------------\nYTD Performance:')
    _year_start_idx = get_date_index(_ytd_start_date, dates)
    _year_end_idx = len(dates)
    ytd_performance = get_all_stats(dates[_year_start_idx:_year_end_idx], returns[_year_start_idx:_year_end_idx])
    ytd_performance += '\nBenchmarks:\n'
    for benchmark in benchmarks:
        ytd_performance += get_benchmark_stats(dates[_year_start_idx:_year_end_idx], returns[_year_start_idx:_year_end_idx], benchmark) # Returns a string of benchmark stats
    performance_stats.append(ytd_performance)

    benchmark_returns = get_todays_benchmark_returns(benchmarks, _current_date)
    _print_benchmark_returns = ''
    for item in benchmark_returns:
        _print_benchmark_returns += '%s : %0.2f%%\t' % (item[0],item[1])

    subject = '%s up %0.2f%% on %s' % ('_'.join(_config_file.rsplit('/')[-1].split('_')[0:2]), todays_return, _yday_end_date)
    body = 'Config File: %s\nBenchmark Returns on %s: %s\n\n'%(_config_file.rsplit('/')[-1], _current_date, _print_benchmark_returns)  + '\n\n'.join(performance_stats)

    if args.mail_send == 1:
        send_mail(subject, body)
    else:
        print subject, body

if __name__ == '__main__':
    main()
