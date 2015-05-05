#!/usr/bin/env python
import sys
import os
import argparse
import smtplib
import MySQLdb
import pandas as pd
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import connect_to_db, get_latest_currency_and_conversion_factors, fetch_latest_prices

order_shortcode_map = {'EP':'ES','CA6':'6C','TYA':'ZN','DSX':'FESX','QGA':'LFR'}
cqg_multiplier_map = {'ES': 100.0, '6C': 10000.0, 'ZN' : 1000.0, 'FESX' : 10.0, 'LFR' : 100.0} # TODO looks like this can change

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

    current_date = datetime.strptime( os.path.splittext(order_file)[0][2:], '%Y%m%d' )
    
    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )

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
            sign_amount = 1 if buy_or_sell == 'Sell' else -1
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
                try:
                    # get the current position and average trade price
                    query = "SELECT estimated_position, estimated_average_trade_price FROM positions WHERE product = '%s' AND date < '%s' ORDER BY date DESC limit 1" % ( exchange_symbol, current_date )
                    db_cursor.execute(query)
                    rows = db_cursor.fetchall()
                    if len(rows) == 0:
                        estimated_position = fill_amount
                        estimated_average_trade_price = fill_price
                    else:
                        estimated_position = rows[0]['estimated_position']
                        estimated_average_trade_price = rows[0]['estimated_average_trade_price']
                        direction_switched = (abs(estimated_position) > 0 and abs(estimated_position + fill_amount) > 0) and \
                                             ( not (sign(estimated_position) == sign(estimated_position + fill_amount) ) )
                        if direction_switched or ( abs(estimated_position + fill_amount ) < abs(estimated_position) ):
                            estimated_average_trade_price = fill_price
                        else:
                            estimated_average_trade_price = ( fill_amount * fill_price + estimated_position * estimated_average_trade_price ) / ( estimated_position + fill_amount )
                        estimated_position += fill_amount
                    try:
                        # Update the current position and average trade price
                        query = "Update positions SET estimated_position = '%d', estimated_average_trade_price = '%s' WHERE product = '%s' AND date = '%s'" \
                                 % (  estimated_position,  estimated_average_trade_price, exchange_symbol, current_date )
                        db_cursor.execute(query)
                        db.commit()
                    except Exception, err:
                        db.rollback()
                        send_mail( err, 'Could not insert updated estimated_position, estimated_average_trade_price into db' )
                except Exception, err:
                    send_mail( err, 'Could not calculate estimated positions based on orders placed' )
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not insert order into db' )

    except Exception, err:
        send_mail( err, 'Some issue in reading order file' )

if __name__ == '__main__':
    main()
