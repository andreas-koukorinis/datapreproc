#!/usr/bin/env python
import sys
import os
import argparse
import json
import urllib2
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_latest_currency_and_conversion_factors, fetch_latest_prices

def send_mail( err, msg ):
  server = smtplib.SMTP( "localhost" )
  server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", \
      'EXCEPTION %s %s' % ( err, msg ) )

def get_mark_to_market(products, product_type, cash, price, average_trade_price, currency_factor, conversion_factor, num_shares, map_product_to_contract):
    mark_to_market = cash
    for product in products:
        if  product_type == 'future':
            mark_to_market += (price[map_product_to_contract[product]] - average_trade_price[product]) * conversion_factor[product] * num_shares[product] * currency_factor[product]
        else:
            mark_to_market += price[map_product_to_contract[product]] * num_shares[product] * conversion_factor[product] * currency_factor[product]
    return mark_to_market

def get_factors(current_date, data_source, products, product_type):
    price = {}
    currency_factor = {}
    conversion_factor = {}
    products_db = [product+'_1' for product in products]
    products_db.extend([product+'_2' for product in products])
    curr_date_str = current_date.strftime("%Y-%m-%d")
    # If data source is CSI get prices and factors from db
    if data_source == 'csi':
        conversion_factor_all, currency_factor_currencywise, product_to_currency = get_latest_currency_and_conversion_factors(products_db, curr_date_str)
        price = fetch_latest_prices(products_db, current_date)
    for prod in products:
        if product_type == 'future':
            conversion_factor[prod] = conversion_factor_all['f'+prod+'_1']
            currency_factor[prod] = currency_factor_currencywise[product_to_currency['f'+prod+'_1']]
        else:
            conversion_factor[prod] = conversion_factor_all[prod]
            currency_factor[prod] = currency_factor_currencywise[product_to_currency[prod]]

    return price, conversion_factor, currency_factor

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('current_date')
    args = parser.parse_args()
    current_date = args.current_date
    parser.add_argument('-d', type=str, help='Data source for prices and rates\nEg: -d csi\n Default is CSI',default='csi', dest='data_source')
    parser.add_argument('-t', type=str, help='Type  for products being ETFs\nEg: -t etf\n Default is future i.e. trading futures',default='future', dest='product_type')
    args = parser.parse_args()
    product_type = args.product_type
    
    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )

    # Get positions, average trade price, yday portfolio value
    try:
        # TODO update if already present
        query = "SELECT product, estimated_average_trade_price, estimated_position, estimated_average_trade_price FROM positions WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)

    except Exception, err:
        send_mail( err, 'Could not find estimated position data in db' )

    # Initialize with default variables
    cash = 0
    products = []
    average_trade_price = {}
    num_shares = {}
    map_product_to_contract = {}
    for row in rows:
        if product_type == 'future':
            products.append( row['product'][:-3] )
            map_product_to_contract[products[-1]] = row['product'][:-3]
        else:
            products.append( row['product'] )
            map_product_to_contract[products[-1]] = products[-1]
        num_shares[product[-1]] = row['estimated_position']
        average_trade_price[product[-1]] = row['estimated_average_trade_price']

    try:
        # TODO update if already present
        query = "SELECT estimated_cash FROM portfolio_stats WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 1:
            cash = rows[0]['estimated_cash']
        else:
            send_mail( err, 'Could not find estimated cash data in db' )
            sys.exit( 'Could not load cash' )
    except Exception, err:
        send_mail( err, 'Could not find estimated cash data in db' )
        sys.exit( 'Could not load cash' )
    
    price, conversion_factor, currency_factor = get_factors( current_date, args.data_source, products, product_type )
    current_worth = get_mark_to_market(products, product_type, cash, price, average_trade_price, currency_factor, conversion_factor, num_shares, map_product_to_contract)

    try:
        query = "Update portfolio_stats SET estimated_portfolio_value = '%s' WHERE date = '%s'" % ( current_worth, current_date )
        db_cursor.execute(query)
        db.commit()
    except Exception, err:
        send_mail( err, 'Could not update estimated portfolio value in db' )
        sys.exit( 'Could not load cash' )

if __name__ == '__main__':
    main()
