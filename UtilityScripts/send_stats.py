#!/usr/bin/env/python
import re
import sys
import os
import subprocess
from datetime import date, timedelta
import smtplib
from email.mime.text import MIMEText
from prettytable import PrettyTable

def parse_results(_results):
    return _results

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
    _data = PrettyTable(["Product", "Amount", "Cost", "Value", "Fill_Price"])
    for _str in _filled_orders:
        if _str == '':  continue
        _str = _str.strip().split('   ')
        _data.add_row([_str[2].strip().split(':')[1].strip(), _str[3].strip().split(':')[1].strip(), _str[4].strip().split(':')[1].strip(), _str[5].strip().split(':')[1].strip(), _str[6].strip().split(':')[1].strip()])
    _data.align = "c"
    _str = _data.get_string()
    _str = _str.replace(" ","  ").replace('--','---')
    return _str


def send_mail(_body, _config_file, _current_date):
    _server = "localhost"
    _from = "sanchit.gupta@tworoads.co.in"
    _to = "cvquant@circulumvite.com"
    #_to = "sanchit.gupta@tworoads.co.in"
    _subject = "Summary stats on %s for config:%s"%(_current_date,_config_file)
     # Prepare actual message
    message = "From: {0}\nTo: {1}\nSubject: {2}\n\n{3}".format(_from, _to, _subject, _body)
    # Send the mail
    server = smtplib.SMTP(_server)
    server.sendmail(_from, _to, message)
    server.quit()

def get_positions(_current_date, _config_file):
    positions_file = 'logs/'+os.path.splitext(_config_file)[0].split('/')[-1]+'/positions.txt'
    placed_orders = re.compile(r'ORDER PLACED ON %s.*'%str(_current_date))
    filled_orders = re.compile(r"ORDER FILLED ON %s.*"%str(_current_date))
    snapshot = re.compile(r"Portfolio snapshot at EndOfDay %s\n.*PnL for today.*\nPortfolio Value.*\nCash.*\nOpen Equity.*\nPositions.*\nNotional Allocation.*\nAverage Trade Price.*\nLeverage.*"%_current_date)
    fp = open(positions_file)
    lines = fp.read()
    fp.close()
    if len(snapshot.findall(lines)) > 0:
        return process_snapshot(snapshot.findall(lines)[0]) + '\n\nORDERS PLACED\n' + process_placed_orders(placed_orders.findall(lines)) + '\n\nORDERS FILLED\n' + process_filled_orders(filled_orders.findall(lines))
    else:
        return 'Not a tradable day or Data not present'

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
    _yday_start_date = date(_current_date.year , 1, 1)
    _yday_end_date = _current_date

    performance_stats = []

    proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', _config_file, str(_ytd_start_date), str(_ytd_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append('------------------------------------------------\nYTD Performance : %s to %s'%(_ytd_start_date,_ytd_end_date))
    performance_stats.append(parse_results( proc.communicate()[0] ) )

    proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', _config_file, str(_mtd_start_date), str(_mtd_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append('------------------------------------------------\nMTD Performance : %s to %s'%(_mtd_start_date,_mtd_end_date))
    performance_stats.append(parse_results( proc.communicate()[0] ) )

    proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', _config_file, str(_yday_start_date), str(_yday_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append('------------------------------------------------\nYDAY Performance : %s to %s'%(_yday_start_date,_yday_end_date))
    performance_stats.append(get_positions(_current_date, _config_file))
    send_mail('\n\n'.join(performance_stats),_config_file,_current_date)
    #print performance_stats

if __name__ == '__main__':
    main()
