import argparse
import os
import pandas as pd
import math
import sys
import subprocess
import smtplib
import numpy
import json
import urllib2
import MySQLdb
import scipy.stats as ss
import datetime
import time
from dateutil import tz

home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_conversion_factors, db_connect, connect_to_db, fetch_prices, fetch_latest_prices_v1

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
# 1. Get products, conversion_factor and # of contracts from last updated record in DB
# 2. Get currency_factors from last updated record in DB # TODO when moving from demo to live CQG prices, we will start using live currency rates
# 3. Fetch latest prices, if NA, then use yesterday's close, i.e., take change on ote to be zero for now TODO
# 4. Compute current mark_to_market and change from yesterday
# 5. Compute sector wise contribution to change from yesterday
# 6. Send notification on slack

def send_mail(_subject, _body):
    _server = "localhost"
    _from = "nilesh.choubey@tworoads.co.in"
    _to = "nilesh.choubey@tworoads.co.in"
    # Prepare actual message
    message = "From: {0}\nTo: {1}\nSubject: {2}\n\n{3}".format(_from, _to, _subject, _body)
    # Send the mail
    server = smtplib.SMTP(_server)
    server.sendmail(_from, _to, message)
    server.quit()

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

def get_factors( current_date, exchange_symbols ):
    global conversion_factor
    products = [ exchange_symbol[:-3] + '_1' for exchange_symbol in exchange_symbols ]
    (csidb,csidb_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    csidb_cursor.execute("SELECT product,conversion_factor FROM products WHERE product IN (%s)" % _format_strings,tuple(products))
    rows = csidb_cursor.fetchall()
    for row in rows:
        conversion_factor[row['product'][:-2]] = float(row['conversion_factor'])



def repeat_to_length(string_to_expand, length):
    return (string_to_expand * ((length/len(string_to_expand))+1))[:length]

def float_len(number, decimal):
    digits = 0
    if number > 0:
        digits = int(math.log10(number)) + decimal + 2
    elif number == 0:
        digits = decimal + 2
    else:
        digits = int(math.log10(-number)) + decimal + 3
    return digits

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', help='Do not send to slack\nEg: --check\n Default is to send to slack', default=False, dest='check', action='store_true')
    args = parser.parse_args()

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

    # Fetch the portfolio value for YDAY
    query = "SELECT date, portfolio_value FROM broker_portfolio_stats ORDER BY date DESC LIMIT 1"
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    yday_pv = 100000.0
    if len(rows) >= 1:
        yday_pv = float(rows[0]['portfolio_value'])
        last_day = rows[0] ['date']

    # Get product, positions, close_price on last day
    query = "SELECT product, broker_position, broker_close_price FROM positions WHERE date = '%s'" % last_day
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    yday_positions = {}
    yday_close = {}
    products = []
    for row in rows:
        product = row['product']
        products.append(product)
        yday_positions[product] = int(row['broker_position'])
        yday_close[product] = float(row['broker_close_price'])

    # Get latest_price, conversion factor from CQG
    if len(products) < 1:
        sys.exit("No product in portfolio!!")
    else:
        file_path = os.path.expanduser('~/datapreproc/data_cleaning/wedbush/cqg/get_prices.py')
        product_string = "', '".join(products)
        price_file = os.path.expanduser('~/datapreproc/data_cleaning/wedbush/cqg/prices.csv')
        proc = subprocess.Popen(['python3', str(file_path), '-o', str(price_file), '-p' ] + products, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        proc.communicate()

    df = pd.read_csv(price_file)
    latest_price = pd.Series(df.Current_Price.values, index = df.Product).to_dict()
    cqg_conversion = pd.Series(df.Conversion_Factor.values, index = df.Product).to_dict()
 
    # Compute today's PnL, portfolio value, sector wise PnL contribution
    product_value_yday = {}
    product_value_now = {}
    product_pnl = {}
    sector_pnl = {}
    total_pnl = 0.0
    get_factors( last_day, products )
    currency_rates = get_currency_rates( last_day )
    for product in products:
        product_value_yday[product] = yday_positions[product] * yday_close[product] * conversion_factor[product[:-3]] * currency_rates[basename_to_currency[product[:-3]]]
        product_value_now[product] = yday_positions[product] * latest_price[product] * cqg_conversion[product] * currency_rates[basename_to_currency[product[:-3]]]
        product_pnl[product] = product_value_now[product] - product_value_yday[product]
        total_pnl += product_pnl[product]
        sector_pnl[sector[product[:-3]]] = sector_pnl.get(sector[product[:-3]], 0.0) + product_pnl[product]

    separator = '-----------------------------------------------------------------------------------'
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    utc = datetime.datetime.fromtimestamp(time.time())
    utc = utc.replace(tzinfo=from_zone)
    eastern = utc.astimezone(to_zone)

    sector_ret_str = '%s\n*Sector wise Return on %s EST*\n%s\n' % (separator, eastern.strftime('%Y-%m-%d %H:%M:%S'), separator)
    sector_ret_str += 'Sector %s | Return ($) %s | Return (%%)\n' % (repeat_to_length(' ', int(52-2.5*len('Sector'))), repeat_to_length(' ', (17-len('Return ($)'))))
    sector_ret_str += '%s\n' % separator
    for key in sector_pnl.keys():
        sector_ret_str += '%s %s | %0.2f %s | %0.2f%%\n' % ( key, repeat_to_length(' ', int(52-2.5*len(key))),  sector_pnl[key], repeat_to_length(' ', 17 - float_len(sector_pnl[key], 2)), 100.0 * sector_pnl[key]/yday_pv)
    sector_ret_str += '%s\n' % separator
    sector_ret_str += '%s %s | %0.2f %s | %0.2f%%\n' % ( 'Portfolio', repeat_to_length(' ', int(55-2.5*len('Portfolio'))),  total_pnl, repeat_to_length(' ', 17 - float_len(total_pnl, 2)), 100.0 * total_pnl/yday_pv)
    sector_ret_str += '%s\n' % separator
    if args.check:
        print sector_ret_str
    else:
        payload = {"channel": "#portfolio-monitor", "username": "monitor", "text": sector_ret_str}
        req = urllib2.Request('https://hooks.slack.com/services/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')
        response = urllib2.urlopen(req, json.dumps(payload))
        #send_mail(subject, output)

if __name__ == '__main__':
    main()
