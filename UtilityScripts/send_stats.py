#!/usr/bin/env/python
import re
import sys
import os
import subprocess
from datetime import date, timedelta
import smtplib
from email.mime.text import MIMEText

def parse_results( _results ):
    return _results

def send_mail(_body, _config_file, _current_date):
    _server = "localhost"
    _from = "sanchit.gupta@tworoads.co.in"
    _to = "cvquant@circulumvite.com"
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
    snapshot = re.compile(r"Portfolio snapshot at EndOfDay %s\n.*PnL for today.*\nPortfolio Value.*\nCash.*\nPositions.*\nMoney Allocation.*\n"%_current_date)
    fp = open(positions_file)
    lines = fp.read()
    fp.close()
    if len(snapshot.findall(lines)) > 0:
        return snapshot.findall(lines)[0] + '\nORDERS\n' + '\n'.join(placed_orders.findall(lines)) + '\n' + '\n'.join(filled_orders.findall(lines))
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
    _yday_start_date = date(_current_date.year , _current_date.month, 1)
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
