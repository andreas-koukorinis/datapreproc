#!/usr/bin/env/python

import sys
import os
import subprocess
from datetime import date, timedelta
import smtplib

def parse_results( _results ):
    #_results = filter( None, _results.split( '\n' ) )
    #print _results
    #_ret = dict( [ ( stat.split('=')[0].strip(' '), float( stat.split('=')[1].strip(' %') ) ) for stat in _results if stat != ''] )
    return _results

def send_mail(_body):
    _server = "localhost"
    _from = "sanchit.gupta@tworoads.co.in"
    _to = ["sanchit.gupta@tworoads.co.in"]
    _subject = "Summary stats IVWAS"
    #_body = "This message was sent with Python's smtplib."

    # Prepare actual message
    _message = """\
    From: %s
    To: %s
    Subject: %s

    %s
    """ % (_from, ", ".join(_to), _subject,_body)
    # Send the mail
    server = smtplib.SMTP(_server)
    server.sendmail(_from, _to, _message)
    server.quit()

def main():
    if len(sys.argv) < 2:
        print "Arguments needed: config_file"
        sys.exit(0)
    lag = timedelta(days=-60)
    _config_file = sys.argv[1]
    _ytd_start_date = date(date.today().year, 1, 1)
    _ytd_end_date = date.today()
    _mtd_start_date = date(date.today().year, date.today().month, 1)
    _mtd_end_date = date.today()
    _yday_start_date = date(date.today().year-2, date.today().month, 1)
    _yday_end_date = date.today()

    performance_stats = []

    proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', _config_file, str(_ytd_start_date), str(_ytd_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append(parse_results( proc.communicate()[0] ) )

    proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', _config_file, str(_mtd_start_date), str(_mtd_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append(parse_results( proc.communicate()[0] ) )

    proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', _config_file, str(_yday_start_date), str(_yday_end_date) ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    performance_stats.append(parse_results( proc.communicate()[0] ) )

    send_mail('\n\n'.join(performance_stats))

if __name__ == '__main__':
    main()
