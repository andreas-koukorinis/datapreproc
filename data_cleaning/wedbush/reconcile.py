#!/usr/bin/env python
import sys
import os
import argparse
import MySQLdb
import smtplib
import urllib2
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_latest_currency_and_conversion_factors, fetch_latest_prices

def send_mail( err, msg ):
  server = smtplib.SMTP( "localhost" )
  server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", \
      'EXCEPTION %s %s' % ( err, msg ) )

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('current_date')
    args = parser.parse_args()
    current_date = args.current_date
    
    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )

    # Reconcile cash, mark to market, TODO commission, pnl
    try:
        query = "SELECT broker_cash, estimated_cash, broker_portfolio_value, estimated_portfolio_value, broker_pnl, estimated_pnl FROM portfolio_stats WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 0:
            send_mail( err, 'No record in portfolio_stats to reconcile for today % ' current_date )
        else:
            is_cash_consistent = rows[0]['broker_cash'] == rows[0]['estimated_cash']
            is_mtm_consistent = rows[0]['broker_portfolio_value'] == rows[0]['estimated_portfolio_value']
            is_pnl_consistent = rows[0]['broker_pnl'] == rows[0]['estimated_pnl']
            # TODO notification to slack and email

    # Reconcile positions, average trade price
        query = "SELECT estimated_position, broker_position, estimated_average_trade_price, broker_average_trade_price FROM positions WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 0:
            send_mail( err, 'No record in positions to reconcile for today % ' current_date )
        else:
            for row in rows:
                is_position_consistent = row['broker_position'] == row['estimated_position'] # TODO string comparison ??
                is_atp_consistent = row['broker_average_trade_price'] == row['estimated_average_trade_price']
                # TODO notification to slack and email
            
if __name__ == '__main__':
    main()
