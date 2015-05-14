#!/usr/bin/env python
import sys
import os
import argparse
import smtplib
import MySQLdb
import pandas as pd
import traceback
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import connect_to_db

order_shortcode_map = {'EP':'ES','CA6':'6C','TYA':'ZN','DSX':'FESX','QGA':'LFR', 'MX6': '6M', 'DL' : 'FGBM', 'USA' : 'ZB', 'JY6' : '6J', 'TP' : 'SXF', 'CB' : 'CGB', 'FVA': 'ZF', 'DB' : 'FGBL'}
cqg_multiplier_map = {'ES': 100.0, '6C': 10000.0, 'ZN' : 1000.0, 'FESX' : 10.0, 'LFR' : 100.0, '6M' : 1000000.0, 'FGBM': 1000.0, 'ZB' : 1000.0, '6J' : 10000.0, 'SXF': 100.0, 'CGB': 100.0, 'ZF': 1000.0, 'FGBL': 100.0 } # TODO looks like this can change

def send_mail( err, msg ):
    server = smtplib.SMTP( "localhost" )
    server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION %s %s' % ( err, msg ) )

#'4/30/2015 11:59'
def parse_dt ( dt_str ):
    dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M")
    return dt.date(), dt.strftime( '%H:%M')

def sign(value):
    ret_value = 1 if value >= 0 else -1
    return ret_value

def main():
    # TODO update realized pnl
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('order_file')
    parser.add_argument('-t', type=str, help='Type  for products being ETFs\nEg: -t etf\n Default is future i.e. trading futures',default='future', dest='product_type')
    args = parser.parse_args()
    product_type = args.product_type

    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )
        print traceback.format_exc()

    # Update `date`,`product`, `place_time`, `fill_time`, `place_amount`, `fill_amount`, `order_price`, `fill_price`, `commission`
    order_df = pd.DataFrame.from_csv( args.order_file, index_col='Order #' )
    try:
        for fc in order_df.index:
            place_dt_str = order_df.loc[fc]['Place Time']
            place_date, place_time = parse_dt( place_dt_str )
            fill_dt_str = order_df.loc[fc]['Fill Time']
            fill_date, fill_time = parse_dt( fill_dt_str )
            order_symbol = str( order_df.loc[fc]['Symbol'][:-2] )
            basename = order_shortcode_map[order_symbol]  
            exchange_symbol = basename + order_df.loc[fc]['Symbol'][-2] + place_date.strftime('%y')
            buy_or_sell = order_df.loc[fc]['B/S']
            sign_amount = 1 if buy_or_sell == 'Buy' else -1
            place_amount = order_df.loc[fc]['Qty'] * sign_amount
            fill_amount = order_df.loc[fc]['Filled'] * sign_amount
            order_price = order_df.loc[fc]['Order Price']/cqg_multiplier_map[basename]
            fill_price = order_df.loc[fc]['Fill Price']/cqg_multiplier_map[basename]
            #print place_date, exchange_symbol, place_time, fill_time, place_amount, fill_amount, order_price, fill_price
            try:
                query = "INSERT INTO actual_orders (date, product, place_time, fill_time, place_amount, fill_amount, order_price, fill_price ) VALUES('%s','%s','%s','%s','%d', '%d', '%s', '%s')" \
                        % ( place_date, exchange_symbol, place_time, fill_time, place_amount, fill_amount, order_price, fill_price )
                db_cursor.execute(query)
                db.commit()
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not insert order into db' )
                print traceback.format_exc()

    except Exception, err:
        print traceback.format_exc()
        send_mail( err, 'Some issue in reading order file' )

if __name__ == '__main__':
    main()
