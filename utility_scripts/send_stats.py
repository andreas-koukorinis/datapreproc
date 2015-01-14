#!/usr/bin/env/python
import re
import sys
import os
import subprocess
from datetime import date, timedelta
import smtplib
from email.mime.text import MIMEText
from prettytable import PrettyTable

home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/utils/')
from dbqueries import fetch_prices

def parse_results(results):
    results = results.split('\n')
    _dict_results = {}
    for result in results:
        if '=' in result:
            _result = result.split('=')
            _name = _result[0].strip()
            _val = _result[1].strip()
            _dict_results[_name] = _val
    _final_order = ['Net Returns', 'Total Tradable Days','Sharpe Ratio', 'Return_drawdown_Ratio','Return Var10 ratio','Correlation to VBLTX', 'Correlation to VTSMX', 'Annualized_Returns', 'Annualized_Std_Returns', 'Initial Capital', 'Net PNL', 'Annualized PNL', 'Annualized_Std_PnL', 'Skewness','Kurtosis','DML','MML','QML','YML','Max Drawdown','Drawdown Period','Drawdown Recovery Period','Max Drawdown Dollar','Annualized PNL by drawdown','Yearly_sharpe','Hit Loss Ratio','Gain Pain Ratio','Max num days with no new high','Losing month streak','Turnover','Leverage','Trading Cost','Total Money Transacted','Total Orders Placed','Worst 5 days','Best 5 days','Worst 5 weeks','Best 5 weeks']
    _print_results = ''
    for _elem in _final_order:
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
    _from = "sanchit.gupta@tworoads.co.in"
    _to = "cvquant@circulumvite.com"
    #_to = "sanchit.gupta@tworoads.co.in"
     # Prepare actual message
    message = "From: {0}\nTo: {1}\nSubject: {2}\n\n{3}".format(_from, _to, _subject, _body)
    # Send the mail
    server = smtplib.SMTP(_server)
    server.sendmail(_from, _to, message)
    server.quit()

def get_positions(_current_date, _config_file):
    positions_file = '/spare/local/logs/'+os.path.splitext(_config_file)[0].split('/')[-1]+'/positions.txt'
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
    if len(sys.argv) < 2:
        print "Arguments needed: config_file"
        sys.exit(0)
    _config_file = sys.argv[1]
    _current_date = date.today() + timedelta(days=-1)
    #print _current_date
    _ytd_start_date = date(_current_date.year, 1, 1)
    _ytd_end_date = _current_date
    _mtd_start_date = date(_current_date.year, _current_date.month, 1)
    _mtd_end_date = _current_date
    _yday_start_date = date(_current_date.year , _current_date.month, 1)
    _yday_end_date = _current_date

    benchmarks = ['VBLTX', 'VTSMX']
    performance_stats = []

    proc = subprocess.Popen(['python', '-W', 'ignore', 'run_simulator.py', _config_file, str(_yday_start_date), str(_yday_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.communicate()
    performance_stats.append('------------------------------------------------\nYDAY Performance : %s to %s'%(_yday_start_date,_yday_end_date))
    todays_return, todays_performance = get_positions(_current_date, _config_file)
    performance_stats.append(todays_performance)
   
    proc = subprocess.Popen(['python', '-W', 'ignore', 'run_simulator.py', _config_file, str(_mtd_start_date), str(_mtd_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append('------------------------------------------------\nMTD Performance : %s to %s'%(_mtd_start_date,_mtd_end_date))
    performance_stats.append(parse_results(proc.communicate()[0]))

    proc = subprocess.Popen(['python', '-W', 'ignore', 'run_simulator.py', _config_file, str(_ytd_start_date), str(_ytd_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append('------------------------------------------------\nYTD Performance : %s to %s'%(_ytd_start_date,_ytd_end_date))
    performance_stats.append(parse_results(proc.communicate()[0]))
 
    benchmark_returns = get_todays_benchmark_returns(benchmarks, _current_date)
    _print_benchmark_returns = ''
    for item in benchmark_returns:
        _print_benchmark_returns += '%s : %0.2f%%\t' % (item[0],item[1])

    subject = '%s up %0.2f%% on %s' % ('_'.join(_config_file.rsplit('/')[-1].split('_')[0:2]), todays_return, _yday_end_date)
    body = 'Config File: %s\nBenchmark Returns on %s: %s\n\n'%(_config_file.rsplit('/')[-1], _current_date, _print_benchmark_returns)  + '\n\n'.join(performance_stats)
    #print subject, body
    send_mail(subject, body)

if __name__ == '__main__':
    main()
