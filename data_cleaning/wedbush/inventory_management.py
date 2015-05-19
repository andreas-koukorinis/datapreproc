#!/usr/bin/env python
import sys
import os
import traceback
import argparse
import MySQLdb
import smtplib
import urllib2
import numpy
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import connect_to_db, db_connect, db_close
from utility_scripts.generate_orders import get_desired_positions
from collections import deque
from math import ceil

def send_mail( err, msg ):
    server = smtplib.SMTP( "localhost" )
    server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", \
        'EXCEPTION %s %s' % ( err, msg ) )

def fetch_latest_prices(exchange_symbols, required_date, product_type):
    open_price_2 = {}
    close_price_1 = {}
    close_price_2 = {}
    tables = {}
    (db,db_cursor) = db_connect()
    for exchange_symbol in exchange_symbols:
        if product_type == 'future':
            shortcode = exchange_symbol[:-3] + '_1'
            query = "SELECT `table` FROM products WHERE product = '%s'" % shortcode
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            query = "SELECT date, specific_ticker, open, close FROM %s WHERE specific_ticker = '%s' AND date <= '%s' ORDER BY date DESC LIMIT 2" % ( rows[0]['table'],\
                     exchange_symbol, required_date)
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
        else:
            query = "SELECT `table` FROM products WHERE product = '%s'" % exchange_symbol
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            query = "SELECT date, product, open, close FROM %s WHERE product = '%s' AND date <= '%s' ORDER BY date DESC LIMIT 2" % ( rows[0]['table'], exchange_symbol, required_date )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
        close_price_2[exchange_symbol] = float(rows[0]['close'])
        close_price_1[exchange_symbol] = float(rows[1]['close'])
        open_price_2[exchange_symbol] = float(rows[0]['open'])
    db_close(db)
    return open_price_2, close_price_1, close_price_2

def get_factors(current_date, data_source, exchange_symbols, product_type):
    conversion_factor = {}
    products = [ exchange_symbol[:-3] + '_1' for exchange_symbol in exchange_symbols ]
    (csidb,csidb_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    query = "SELECT product,conversion_factor FROM products WHERE product IN (%s)" % _format_strings
    csidb_cursor.execute(query, tuple(products))
    rows = csidb_cursor.fetchall()
    for row in rows:
        conversion_factor[row['product'][:-2]] = float(row['conversion_factor'])
  
    # If data source is CSI get prices and factors from db
    if data_source == 'csi':
        new_symbols = []
        for i in range(len(exchange_symbols)):
            basename = exchange_symbols[i][:-3]
            if basename == "ES": # For ES we want to use SP prices
                new_symbols.append( 'SP' + exchange_symbols[i][-3:] )
            else:
                new_symbols.append( exchange_symbols[i] )
        open_price_2, close_price_1, close_price_2 = fetch_latest_prices(new_symbols, current_date, product_type) # Uses exchange symbols
        for key in open_price_2.keys():
            if key[:-3] == "SP":
                open_price_2['ES' + key[-3:]] = open_price_2[key]
                close_price_1['ES' + key[-3:]] = close_price_1[key]
                close_price_2['ES' + key[-3:]] = close_price_2[key]
    return open_price_2, close_price_1, close_price_2, conversion_factor

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('current_date')
    parser.add_argument('config_file')
    parser.add_argument('portfolio_file')
    parser.add_argument('-d', type=str, help='Data source for prices and rates\nEg: -d csi\n Default is CSI',default='csi', dest='data_source')
    parser.add_argument('-t', type=str, help='Type  for products being ETFs\nEg: -t etf\n Default is future i.e. trading futures',default='future', dest='product_type')
    parser.add_argument('-read_positions',type=str, help='Read desired startegy positons from file\nEg: -read_positions desired_pos_20150101.txt\n Default is to use gen orders', default=None, dest='read_positions')
    args = parser.parse_args()
    current_date = datetime.strptime(args.current_date, '%Y%m%d')
    product_type = args.product_type
    
    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )
        print traceback.format_exc()

    # Get current worth
    try:    
        query = "SELECT portfolio_value FROM broker_portfolio_stats WHERE date < '%s' ORDER BY date DESC limit 1" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        current_worth = float(rows[0]['portfolio_value'])
    except:
        sys.exit('Could not get current worth')

    # First update positions based on actual orders for today
    net_positions = {}  
    inventory_positions = {}  
    strategy_positions = {}  
    latest_inventory_date = date(2050, 1, 1)
    products = []
    try:
        # Get the current position 
        try:    
            # Get YDAY position    
            query = "SELECT date FROM inventory WHERE date < '%s' ORDER BY date DESC limit 1" % ( current_date )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            latest_inventory_date = rows[0]['date']
            query = "SELECT product, net_position, inventory_position, strategy_position FROM inventory WHERE date = '%s' ORDER BY date DESC" % ( latest_inventory_date )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            for row in rows:
                try:
                    products.append(row['product'])
                    net_positions[row['product']] = int(row['net_position'])
                    inventory_positions[row['product']] = float(row['inventory_position'])
                    strategy_positions[row['product']] = float(row['strategy_position'])
                except Exception, err:
                    print traceback.format_exc()
                    print 'Could not find position for some product for previous day'
        except Exception, err:
            print traceback.format_exc()
            print 'Could not find position for previous day'

        query = "SELECT product, fill_amount FROM actual_orders WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            products.append(row['product'])
            product = row['product']
            net_positions[product] = net_positions.get( product, 0 ) + int(row['fill_amount'])
            inventory_positions[product] = inventory_positions.get( product, 0 ) + int(row['fill_amount'])
        for product in net_positions.keys():
            # Update the current position
            query = "UPDATE inventory SET net_position='%s' WHERE date='%s' AND product='%s'" \
                     % ( net_positions[product], current_date, product )
            db_cursor.execute(query)
        db.commit()
    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not calculate net positions based on orders placed' )
        print traceback.format_exc()

    strategy_desired_positions = {}
    if args.read_positions is not None:
        with open(os.path.expanduser(args.read_positions)) as f:
            positions_file_lines = [ln.strip('\n') for ln in f.readlines()]
            if len(positions_file_lines) < 1:
                sys.exit("Empty positions file!")
            else:
                for i in xrange(0,len(positions_file_lines)):
                    prod_wt_price = positions_file_lines[i].split(' ')
                    product = prod_wt_price[0]
                    strategy_desired_positions[product] = float(prod_wt_price[1]) 
    else:
        strategy_desired_positions = get_desired_positions(args.config_file, args.portfolio_file, float(current_worth), '1995-01-01', current_date.strftime('%Y-%m-%d')) #TODO

    # Initialize with default variables
    products = list(set(products + strategy_desired_positions.keys()))
    #Get outstanding positions for yday
    outstanding_bal_col = ['inventory_USD_bal', 'net_bal', 'strategy_bal', 'inventory_bal']
    outstanding_ote_col = ['inventory_ote']
    outstanding_bal = {}
    outstanding_ote = {}

    for product in products:
        strategy_positions[product] = strategy_positions.get(product, 0.0)
        inventory_positions[product] = inventory_positions.get(product, 0.0)
        net_positions[product] = net_positions.get(product, 0.0)
        outstanding_bal[product] = {}
        outstanding_ote[product] = {}
        for col in outstanding_bal_col:
            outstanding_bal[product][col] = 0.0   
        for col in outstanding_ote_col:
            outstanding_ote[product][col] = 0.0   

    try:
        query = "SELECT a.product, net_bal, strategy_bal, inventory_bal FROM inventory AS a JOIN (SELECT product, max(date) AS date FROM inventory WHERE date < '%s' GROUP BY 1) AS b \
                 ON a.product = b.product AND a.date = b.date" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) != 0:
            for row in rows:
                product = row['product']
                for key in ['net_bal', 'strategy_bal', 'inventory_bal']:
                    try:
                        outstanding_bal[product][key] = float( row[key] )
                    except:
                        pass
    except Exception, err:
        send_mail( err, 'Could not find net,startegy,inventory bal in db for prior date' )
        print traceback.format_exc()
        sys.exit( 'Could not find net,startegy,inventory bal in db for prior date' )


    open_price_2, close_price_1, close_price_2, conversion_factor = get_factors( current_date, args.data_source, products, product_type) #TODO
    # Get commission rates
    try:
        commission_rates = {}
        query = "SELECT product, currency, exchange_fee, broker_fee, clearing_fee, nfa_fee FROM commission_rates" 
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            commission_rates[row['product']] = { 'currency' : row['currency'],
                                                 'exchange_fee' : float(row['exchange_fee']),
                                                 'broker_fee' : float(row['broker_fee']),
                                                 'clearing_fee' : float(row['clearing_fee']),
                                                 'nfa_fee' : float(row['nfa_fee'])
                                               }
    except Exception, err:
        send_mail( err, 'Could not fetch commission rates data from db' )
        print traceback.format_exc()
        sys.exit( 'Could not load commission rates data' )

    # Load todays Orders 
    todays_orders = {}
    unsettled_idx1 = {}
    unsettled_idx2 = {}
    try:
        query = "SELECT id, date, product, fill_amount, fill_price FROM actual_orders WHERE date = '%s' ORDER BY date ASC, fill_price ASC" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            product = row['product']
            fill_amount = int(row['fill_amount'])
            fill_price = float(row['fill_price'])
            order_date = row['date']
            order_id = int(row['id'])
            if product not in todays_orders.keys():
                todays_orders[product] = [ [ order_date, fill_amount, fill_price, order_id ] ]
                unsettled_idx1[product] = [0,0] # First idx is for negative, second for positive
                unsettled_idx2[product] = [0,0] # First idx is for negative, second for positive
            else:
                todays_orders[product].append( [ order_date, fill_amount, fill_price, order_id ] )
    except Exception, err:
        send_mail( err, 'Could not load todays orders from db' )
        print traceback.format_exc()
        sys.exit( 'Could not load todays orders from db' )

    # Load unsettled Orders 
    unsettled_orders = {}
    try:
        query = "SELECT unsettled_date FROM unsettled_orders WHERE unsettled_date < '%s' ORDER BY unsettled_date DESC LIMIT 1" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 1:
            query = "SELECT id, date, product, fill_amount, fill_price FROM unsettled_orders WHERE unsettled_date = '%s' ORDER BY date ASC, fill_price ASC" % ( rows[0]['unsettled_date'] )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            for row in rows:
                product = row['product']
                fill_amount = int(row['fill_amount'])
                fill_price = float(row['fill_price'])
                order_date = row['date']
                order_id = int(row['id'])
                if product not in unsettled_orders.keys():
                    unsettled_orders[product] = [ [ order_date, fill_amount, fill_price, order_id ] ]
                else:
                    unsettled_orders[product].append( [ order_date, fill_amount, fill_price, order_id ] )
    except Exception, err:
        send_mail( err, 'Could not load unsettled orders from db' )
        print traceback.format_exc()
        sys.exit( 'Could not load unsettled orders from db' )

    # Fake trades between strategy and inventory
    # Update positions as well
    for product in products:
        basename = product[:-3]
        outstanding_bal[product]['strategy_bal'] += conversion_factor[basename] * ( strategy_positions[product] * ( close_price_2[product] - close_price_1[product]) + \
                                                   ( strategy_desired_positions[product] - strategy_positions[product] ) * ( close_price_2[product] - open_price_2[product] ) )
        outstanding_bal[product]['inventory_bal'] -= conversion_factor[basename] * ( strategy_positions[product] * ( close_price_2[product] - close_price_1[product]) + \
                                                   ( strategy_desired_positions[product] - strategy_positions[product] ) * ( close_price_2[product] - open_price_2[product] ) )

        inventory_positions[product] -= ( strategy_desired_positions[product] - strategy_positions[product] )
        strategy_positions[product] += ( strategy_desired_positions[product] - strategy_positions[product] )
         
    for product in todays_orders.keys():
        basename = product[:-3]
        for order in todays_orders[product]:
            # First add the commission
            fill_amount = order[1]
            outstanding_bal[product]['inventory_USD_bal'] -= abs( fill_amount ) * commission_rates[basename]['broker_fee']
            outstanding_bal[product]['inventory_USD_bal'] -= abs( fill_amount ) * commission_rates[basename]['nfa_fee']
            outstanding_bal[product]['inventory_bal'] -= abs( fill_amount ) * ( commission_rates[basename]['exchange_fee'] \
                                                       + commission_rates[basename]['clearing_fee'] )

    # Try day trade offset
    for product in todays_orders.keys():
        basename = product[:-3]
        while True:
            while unsettled_idx1[product][0] < len(todays_orders[product]) and todays_orders[product][unsettled_idx1[product][0]][1] >= 0:
                unsettled_idx1[product][0] += 1 
            while unsettled_idx1[product][1] < len(todays_orders[product]) and todays_orders[product][unsettled_idx1[product][1]][1] <= 0:
                unsettled_idx1[product][1] += 1 
            if unsettled_idx1[product][0] == len(todays_orders[product]) or unsettled_idx1[product][1] == len(todays_orders[product]):
                break
            closed_amount = min( abs(todays_orders[product][unsettled_idx1[product][0]][1]), abs(todays_orders[product][unsettled_idx1[product][1]][1]) )
            buy_price = todays_orders[product][unsettled_idx1[product][1]][2]
            sell_price = todays_orders[product][unsettled_idx1[product][0]][2]
            todays_orders[product][unsettled_idx1[product][0]][1] += closed_amount
            todays_orders[product][unsettled_idx1[product][1]][1] -= closed_amount

            outstanding_bal[product]['inventory_bal'] += closed_amount * conversion_factor[basename] * ( sell_price - buy_price )

    # Try FIFO offset against unsettled orders from previous days
    for product in todays_orders.keys():
        basename = product[:-3]
        if product not in unsettled_orders.keys():
            continue
        while True:
            while unsettled_idx2[product][0] < len(unsettled_orders[product]) and unsettled_orders[product][unsettled_idx2[product][0]][1] >= 0:
                unsettled_idx2[product][0] += 1 
            while unsettled_idx2[product][1] < len(unsettled_orders[product]) and unsettled_orders[product][unsettled_idx2[product][1]][1] <= 0:
                unsettled_idx2[product][1] += 1 
            while unsettled_idx1[product][0] < len(todays_orders[product]) and todays_orders[product][unsettled_idx1[product][0]][1] >= 0:
                unsettled_idx1[product][0] += 1 
            while unsettled_idx1[product][1] < len(todays_orders[product]) and todays_orders[product][unsettled_idx1[product][1]][1] <= 0:
                unsettled_idx1[product][1] += 1 

            if ( ( unsettled_idx2[product][0] == len(unsettled_orders[product]) ) or ( unsettled_idx1[product][1] == len(todays_orders[product]) ) ) and \
               ( ( unsettled_idx2[product][1] == len(unsettled_orders[product]) ) or ( unsettled_idx1[product][0] == len(todays_orders[product]) ) ):
                break
            elif ( ( unsettled_idx2[product][1] < len(unsettled_orders[product]) ) and ( unsettled_idx1[product][0] < len(todays_orders[product]) ) ):
                closed_amount = min( abs(todays_orders[product][unsettled_idx1[product][0]][1]), abs(unsettled_orders[product][unsettled_idx2[product][1]][1]) )
                sell_price = todays_orders[product][unsettled_idx1[product][0]][2]
                buy_price = unsettled_orders[product][unsettled_idx2[product][1]][2]
                todays_orders[product][unsettled_idx1[product][0]][1] += closed_amount
                unsettled_orders[product][unsettled_idx2[product][1]][1] -= closed_amount
                outstanding_bal[product]['inventory_bal'] += closed_amount * conversion_factor[basename] * ( sell_price - buy_price )
            elif ( ( unsettled_idx2[product][0] < len(unsettled_orders[product]) ) and ( unsettled_idx1[product][1] < len(todays_orders[product]) ) ):
                closed_amount = min( abs(todays_orders[product][unsettled_idx1[product][1]][1]), abs(unsettled_orders[product][unsettled_idx2[product][0]][1]) )
                sell_price = unsettled_orders[product][unsettled_idx2[product][0]][2]
                buy_price = todays_orders[product][unsettled_idx1[product][1]][2]
                todays_orders[product][unsettled_idx1[product][1]][1] -= closed_amount
                unsettled_orders[product][unsettled_idx2[product][0]][1] += closed_amount
                outstanding_bal[product]['inventory_bal'] += closed_amount * conversion_factor[basename] * ( sell_price - buy_price )
                 
    for product in unsettled_orders.keys():
        unsettled_orders[product] = [ order for order in unsettled_orders[product] if order[1] != 0 ] 

    for product in todays_orders.keys():
        for order in todays_orders[product]:
            if order[1] != 0:
                if product not in unsettled_orders.keys():
                    unsettled_orders[product] = [order]
                else:
                    unsettled_orders[product].append(order)  
    
    # Update open equity using estimated open positions
    for product in unsettled_orders.keys():
        basename = product[:-3]
        currency = commission_rates[basename]['currency']
        for order in unsettled_orders[product]:
            outstanding_ote[product]['inventory_ote'] += order[1] * conversion_factor[basename] * (close_price_2[product] - order[2])

    try:
        for product in outstanding_bal.keys():
            query = "INSERT INTO inventory (date, product, net_position, strategy_position, inventory_position, inventory_USD_bal, strategy_bal, inventory_bal, net_bal, inventory_ote) VALUES('%s','%s','%d','%s','%s','%s','%s', '%s','%s','%s')" % ( current_date, product, net_positions[product], strategy_positions[product], inventory_positions[product], outstanding_bal[product]['inventory_USD_bal'], outstanding_bal[product]['strategy_bal'], outstanding_bal[product]['inventory_bal'], outstanding_bal[product]['strategy_bal'] + outstanding_bal[product]['inventory_bal'], outstanding_ote[product]['inventory_ote']  )
            db_cursor.execute(query)
            db.commit()
    except Exception, err:
        send_mail( err, 'Could not update inventory data in db' )
        print traceback.format_exc()
        sys.exit( 'Could not update inventory data in db' )

if __name__ == '__main__':
    main()
