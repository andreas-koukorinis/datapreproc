#!/usr/bin/env python
import sys
import os
import argparse
import MySQLdb
import smtplib
import traceback
import json
import urllib2
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import connect_to_db

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
        print traceback.format_exc()
        send_mail( err, 'Could not connect to db' )
        sys.exit()

    # Reconcile PNL and Mark To Market
    output = "*Reconciliation for Date : %s*\n\n" % current_date
    try:
        query = "SELECT portfolio_value, pnl FROM broker_portfolio_stats WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        broker_portfolio_value = rows[0]['portfolio_value']
        broker_pnl = rows[0]['pnl']
        query = "SELECT portfolio_value, pnl FROM estimated_portfolio_stats WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        estimated_portfolio_value = rows[0]['portfolio_value']
        estimated_pnl = rows[0]['pnl']
        is_pnl_consistent = broker_pnl == estimated_pnl
        is_mtm_consistent = broker_portfolio_value == estimated_portfolio_value
        if is_pnl_consistent:
            output += "_PNL Reconciliation Status_ : *SUCCESS*\nEstimated PNL | Broker PNL \n%s | %s \n" % ( estimated_pnl, broker_pnl )
        else:
            output += "_PNL Reconciliation Status_ : *FAILURE*\nEstimated PNL | Broker PNL \n%s | %s \n" % ( estimated_pnl, broker_pnl )

        if is_mtm_consistent:
            output += "\n_Mark To Market Reconciliation Status_ : *SUCCESS*\nEstimated MTM | Broker MTM \n%s | %s \n" % ( estimated_portfolio_value, broker_portfolio_value )
        else:
            output += "\n_Mark To Market Reconciliation Status_ : *FAILURE*\nEstimated MTM | Broker MTM \n%s | %s \n" % ( estimated_portfolio_value, broker_portfolio_value )
        
    except Exception, err:
        print traceback.format_exc()
        send_mail( err, 'RECONCILIATION ISSUE: Some error while reconciling PNL and Mark To Market %s ' % current_date )

    # Reconcile positions, average trade price
    try:
        query = "SELECT product, estimated_position, broker_position, estimated_average_trade_price, broker_average_trade_price FROM positions WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 0:
            send_mail( err, 'No record in positions to reconcile for today %s ' % current_date )
            sys.exit( 'No record in positions to reconcile for today' )
        else:
            all_good = True
            output += "\n_Position Reconciliation_ :\nProduct | Estimated Position | Broker Position | Status\n"
            for row in rows:
                is_position_consistent = row['broker_position'] == row['estimated_position'] # string comparison
                if is_position_consistent:
                    output += "%s | %s | %s | *SUCCESS*\n" % ( row['product'], row['estimated_position'], row['broker_position'] )
                else:
                    output += "%s | %s | %s | *FAILURE*\n" % ( row['product'], row['estimated_position'], row['broker_position'] )
                #is_atp_consistent = row['broker_average_trade_price'] == row['estimated_average_trade_price']
    except Exception, err:
        print traceback.format_exc()
        send_mail( err, 'RECONCILIATION ISSUE: Some error while reconciling Positions %s ' % current_date )

    print output

    payload = {"channel": "#portfolio-monitor", "username": "monitor", "text": output}
    req = urllib2.Request('https://hooks.slack.com/services/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')
    response = urllib2.urlopen(req, json.dumps(payload))
            
if __name__ == '__main__':
    main()