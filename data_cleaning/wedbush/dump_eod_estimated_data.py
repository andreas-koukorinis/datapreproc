#!/usr/bin/env python
import sys
import os
import traceback
import argparse
import MySQLdb
import smtplib
import urllib2
from datetime import datetime, date, timedelta
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_latest_currency_and_conversion_factors_v1, fetch_latest_prices_v1, connect_to_db, db_connect
from collections import deque
from math import ceil

def send_mail( err, msg ):
    server = smtplib.SMTP( "localhost" )
    server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", \
        'EXCEPTION %s %s' % ( err, msg ) )

def get_match_idx( order_list, order_sign ):
    idx = len(order_list) - 1
    while idx >= 0:
        if sign(order_list[idx][1]) != order_sign:
            return idx
        idx -= 1
    return -1

def sign(value):
    ret_value = 1 if value >= 0 else -1
    return ret_value

def get_currency_from_column( column ):
    currency = column.split('_')[1]
    if currency != 'USD':
        currency += 'USD'
    return currency

def get_column_from_currency( currency, end ):
    currency = currency[0:3]
    column = 'secured_' + currency + '_' + end
    return column

def get_mark_to_market(outstanding_amounts_ote, outstanding_amounts_bal, currency_rates):
    converted_total_bal = 0
    converted_total_ote = 0 
    mark_to_market = 0
    for key in outstanding_amounts_bal.keys():
        if outstanding_amounts_bal[key] != 0:
            currency = get_currency_from_column(key)
            converted_total_bal += outstanding_amounts_bal[key] * currency_rates[currency]
            mark_to_market += outstanding_amounts_bal[key] * currency_rates[currency]
    for key in outstanding_amounts_ote.keys():
        if outstanding_amounts_ote[key] != 0:
            currency = get_currency_from_column(key)
            converted_total_ote += outstanding_amounts_ote[key] * currency_rates[currency]
            mark_to_market += outstanding_amounts_ote[key] * currency_rates[currency]
    return (converted_total_bal, converted_total_ote, mark_to_market)

def get_factors(current_date, data_source, exchange_symbols, product_type, wedbush_db_cursor):
    prices = {}
    conversion_factor = {}
    products = [ exchange_symbol[:-3] + '_1' for exchange_symbol in exchange_symbols ]
    (csidb,csidb_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    csidb_cursor.execute("SELECT product,conversion_factor FROM products WHERE product IN (%s)" % _format_strings,tuple(products))
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
        #conversion_factor, currency_factor = get_latest_currency_and_conversion_factors_v1(basenames, curr_date_str, product_type)
        prices = fetch_latest_prices_v1(new_symbols, current_date, product_type) # Uses exchange symbols
        for key in prices.keys():
            if key[:-3] == "SP":
                prices['ES' + key[-3:]] = prices[key]

    elif data_source == 'wedbush':
        _format_strings = ','.join(['%s'] * len(exchange_symbols))
        query = "SELECT product, broker_close_price FROM positions WHERE product IN (%s) and date = '%s'" % ( _format_strings, current_date.strftime("%Y-%m-%d") )
        wedbush_db_cursor.execute(query, tuple(exchange_symbols))
        rows = wedbush_db_cursor.fetchall()
        for row in rows:
            prices[row['product']] = float( row['broker_close_price'] )
    return prices, conversion_factor

def dump_eod_estimated_data(current_date, product_type='future', data_source='csi'):
    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        send_mail( err, 'Could not connect to db' )
        print traceback.format_exc()

    # First update positions based on actual orders for today
    positions = {}  
    try:
        # Get the current position 
        try:    
            # Get YDAY position    
            query = "SELECT date FROM positions WHERE date < '%s' ORDER BY date DESC limit 1" % ( current_date )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            query = "SELECT product, estimated_position FROM positions WHERE date = '%s' ORDER BY date DESC" % ( rows[0]['date'] )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            for row in rows:
                positions[row['product']] = int(row['estimated_position'])
        except Exception, err:
            print traceback.format_exc()
            print 'Could not fnd position for previous day'

        query = "SELECT product, fill_amount FROM actual_orders WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            product = row['product']
            positions[product] = positions.get( product, 0 ) + int(row['fill_amount'])
        for product in positions.keys():
            # Update the current position
            query = "UPDATE positions SET estimated_position='%s' WHERE date='%s' AND product='%s'" \
                     % ( positions[product], current_date, product )
            db_cursor.execute(query)
        db.commit()
    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not calculate estimated positions based on orders placed' )
        print traceback.format_exc()

    # Initialize with default variables
    cash = 0
    products = []
    #Get outstanding positions for yday
    outstanding_currencies_bal = ['segregated_USD_bal', 'secured_USD_bal', 'secured_GBP_bal', 'secured_EUR_bal', 'secured_CAD_bal']
    outstanding_amounts_bal = dict.fromkeys(outstanding_currencies_bal, 0.0)

    columns = ','.join( outstanding_currencies_bal)
    try:
        query = "select %s from estimated_portfolio_stats where date < '%s' order by date desc limit 1" % ( columns, current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 0:
            try:
                query = "SELECT segregated_USD_bal from broker_portfolio_stats where date < '%s' order by date desc limit 1" % ( current_date )
                db_cursor.execute(query)
                rows = db_cursor.fetchall()
                if len(rows) > 0:
                    outstanding_amounts_bal['segregated_USD_bal'] = float( rows[0]['segregated_USD_bal'] ) 
                else:
                    send_mail( '', 'Something wrong with estimated segregated/secured data in db for broker while estimation' )
                    print traceback.format_exc()
                    sys.exit( 'Could not fetch segregated_USD_bal broker data for estimation' )
            except Exception, err:
                send_mail( err, 'Could not find estimated segregated/secured data in db 1' )
                print traceback.format_exc()
                sys.exit( 'Could not load estimated segregated/secured data from db 1' )
        else:
            for key in outstanding_amounts_bal.keys():
                outstanding_amounts_bal[key] = float( rows[0][key] )
    except Exception, err:
        send_mail( err, 'Could not find estimated segregated/secured data in db 2' )
        print traceback.format_exc()
        sys.exit( 'Could not load estimated segregated/secured data from db 2' )

    # Make product list using positions till yday and orders for today
    try:
        query = "SELECT date FROM positions WHERE date < '%s' ORDER BY date DESC LIMIT 1" % current_date
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) == 1:
            query = "SELECT product FROM positions WHERE date = '%s'" % ( rows[0]['date'] )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            for row in rows:
                products.append( row['product'] )
    except Exception, err:
        send_mail( err, 'Could not find estimated position data in db' )
        print traceback.format_exc()
    try:
        query = "SELECT product FROM actual_orders WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            products.append( row['product'] )
    except Exception, err:
        send_mail( err, 'Could not find estimated position data in db' )
        print traceback.format_exc()
        sys.exit( 'Could not load estimated position data from db' )

    products = list( set ( products ) )
    price, conversion_factor = get_factors( current_date, data_source, products, product_type, db_cursor )

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

    product_to_currency = {}
    currency_rates = {}
    try:
        query = "SELECT currency, rate FROM currency_rates WHERE date = '%s'" % ( current_date )
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        for row in rows:
            currency_rates[row['currency']] = float( row['rate'] )

        for product in commission_rates.keys():
            currency = commission_rates[product]['currency']
            product_to_currency[product] = currency 

    except Exception, err:
        send_mail( err, 'Could not load currency rates from db' )
        print traceback.format_exc()
        sys.exit( 'Could not load currency rates from db' )

    # If we reach here, this means yday cash is available
    # Now estimate todays cash
    commission = 0
    realized_pnl = 0

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
        query = "SELECT unsettled_date FROM unsettled_orders WHERE unsettled_date < '%s' ORDER BY unsettled_date DESC LIMIT 1" % current_date
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        query = "SELECT id, date, product, fill_amount, fill_price FROM unsettled_orders WHERE unsettled_date = '%s' ORDER BY date ASC, fill_price ASC" % rows[0]['unsettled_date']
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

    for product in todays_orders.keys():
        basename = product[:-3]
        for order in todays_orders[product]:
            # First add the commission
            fill_amount = order[1]
            commission += abs( fill_amount ) * ( ( commission_rates[basename]['broker_fee'] + commission_rates[basename]['nfa_fee'] ) + \
                          ( commission_rates[basename]['exchange_fee'] + commission_rates[basename]['clearing_fee'] ) * currency_rates[product_to_currency[basename]] )

            outstanding_amounts_bal['segregated_USD_bal'] -= abs( fill_amount ) * commission_rates[basename]['broker_fee']
            if product_to_currency[basename] == 'USD':
                outstanding_amounts_bal['segregated_USD_bal'] -= abs( fill_amount ) * commission_rates[basename]['nfa_fee']
            else:
                outstanding_amounts_bal['secured_USD_bal'] -= abs( fill_amount ) * commission_rates[basename]['nfa_fee']

            if product_to_currency[basename] == 'USD':
                outstanding_amounts_bal['segregated_USD_bal'] -= abs( fill_amount ) * ( commission_rates[basename]['exchange_fee'] \
                                                                                      + commission_rates[basename]['clearing_fee'] )
            else:
                outstanding_amounts_bal[get_column_from_currency(product_to_currency[basename], 'bal')] -= abs( fill_amount ) * ( commission_rates[basename]['exchange_fee'] \
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

            if product_to_currency[basename] == 'USD':
                outstanding_amounts_bal['segregated_USD_bal'] += closed_amount * conversion_factor[basename] * ( sell_price - buy_price )
            else:
                outstanding_amounts_bal[get_column_from_currency(product_to_currency[basename], 'bal')] += closed_amount * conversion_factor[basename] * \
                                                                                                                       ( sell_price - buy_price )

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

                if product_to_currency[basename] == 'USD':
                    outstanding_amounts_bal['segregated_USD_bal'] += closed_amount * conversion_factor[basename] * ( sell_price - buy_price )
                else:
                    outstanding_amounts_bal[get_column_from_currency(product_to_currency[basename], 'bal')] += closed_amount * conversion_factor[basename] * \
                                                                                                               ( sell_price - buy_price )
            elif ( ( unsettled_idx2[product][0] < len(unsettled_orders[product]) ) and ( unsettled_idx1[product][1] < len(todays_orders[product]) ) ):
                closed_amount = min( abs(todays_orders[product][unsettled_idx1[product][1]][1]), abs(unsettled_orders[product][unsettled_idx2[product][0]][1]) )
                sell_price = unsettled_orders[product][unsettled_idx2[product][0]][2]
                buy_price = todays_orders[product][unsettled_idx1[product][1]][2]
                todays_orders[product][unsettled_idx1[product][1]][1] -= closed_amount
                unsettled_orders[product][unsettled_idx2[product][0]][1] += closed_amount

                if product_to_currency[basename] == 'USD':
                    outstanding_amounts_bal['segregated_USD_bal'] += closed_amount * conversion_factor[basename] * ( sell_price - buy_price )
                else:
                    outstanding_amounts_bal[get_column_from_currency(product_to_currency[basename], 'bal')] += closed_amount * conversion_factor[basename] * \
                                                                                                               ( sell_price - buy_price )
     
    for product in unsettled_orders.keys():
        unsettled_orders[product] = [ order for order in unsettled_orders[product] if order[1] != 0 ] 

    for product in todays_orders.keys():
        for order in todays_orders[product]:
            if order[1] != 0:
                if product not in unsettled_orders.keys():
                    unsettled_orders[product] = [order]
                else:
                    unsettled_orders[product].append(order)  

    # Update unsettled orders in db
    for product in unsettled_orders.keys():
        for order in unsettled_orders[product]:
            try:
                query = "INSERT INTO unsettled_orders(id, date, unsettled_date, product, fill_amount, fill_price) VALUES('%s', '%s','%s', '%s','%s','%s')" % ( order[3], order[0], current_date, product, order[1], order[2])
                db_cursor.execute(query)
                db.commit()
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not add unsettled orders to db' )
                print traceback.format_exc()
            
    # Fecth oter fees from broker portfolio stats
    try:
        query = "SELECT other_fees FROM broker_portfolio_stats WHERE date = '%s'" % current_date
        db_cursor.execute(query)
        rows=db_cursor.fetchall()
        other_fees = rows[0]['other_fees']     
    except Exception, err:
        send_mail( err, 'Could not fetch other fees from db' )
        print traceback.format_exc()
    outstanding_amounts_bal['segregated_USD_bal'] += other_fees

    # Update open equity using estimated open positions
    outstanding_currencies_ote = ['segregated_USD_ote', 'secured_USD_ote', 'secured_GBP_ote', 'secured_EUR_ote', 'secured_CAD_ote']
    outstanding_amounts_ote = dict.fromkeys(outstanding_currencies_ote, 0.0)
    for product in unsettled_orders.keys():
        basename = product[:-3]
        currency = commission_rates[basename]['currency']
        for order in unsettled_orders[product]:
            if currency == 'USD':
                outstanding_amounts_ote['segregated_USD_ote'] += order[1] * conversion_factor[basename] *\
                                                                 (price[product] - order[2])
                                                                 #((ceil( price[product]*100000)/100000) - (ceil(order[2]*100000)/100000 ))
            else:
                outstanding_amounts_ote[get_column_from_currency(currency, 'ote')] += order[1] * conversion_factor[basename]* \
                                                                 (price[product] - order[2])
                                                                 #((ceil( price[product]*100000)/100000) - (ceil(order[2]*100000)/100000 ))

    converted_total_bal, converted_total_ote, mark_to_market = get_mark_to_market(outstanding_amounts_ote, outstanding_amounts_bal, currency_rates)

    yday_portfolio_value = 1000000
    try:
        query = "SELECT portfolio_value FROM estimated_portfolio_stats WHERE date < '%s' ORDER BY date DESC LIMIT 1" % current_date
        db_cursor.execute(query)
        rows=db_cursor.fetchall()
        yday_portfolio_value = float(rows[0]['portfolio_value'])        
    except Exception, err:
        send_mail( err, 'Could not fetch last day portfolio value from db' )
        print traceback.format_exc()

    pnl = mark_to_market - yday_portfolio_value

    try:
        query = "INSERT INTO estimated_portfolio_stats (date, converted_total_bal, converted_total_ote, portfolio_value, commission, pnl, other_fees) VALUES('%s','%0.2f','%0.2f','%0.2f','%0.2f','%0.2f','%0.2f')" % ( current_date, converted_total_bal, converted_total_ote, mark_to_market, commission, pnl, other_fees )
        db_cursor.execute(query)
        db.commit()
    except Exception, err:
        send_mail( err, 'Could not update estimated portfolio data in db' )
        print traceback.format_exc()
        sys.exit( 'Could not update estimated portfolio data in db' )

    # Update outstanding balance in DB
    try:
        for key in outstanding_amounts_bal.keys():
            query = "UPDATE estimated_portfolio_stats SET %s = '%0.2f' WHERE date = '%s' " % ( key, outstanding_amounts_bal[key], current_date )
            db_cursor.execute( query )
        db.commit()
    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not insert outstanding amounts bal data into db' )
        print traceback.format_exc()

    # Update outstanding ote in DB
    try:
        for key in outstanding_amounts_ote.keys():
            query = "UPDATE estimated_portfolio_stats SET %s = '%0.2f' WHERE date = '%s' " % ( key, outstanding_amounts_ote[key], current_date )
            db_cursor.execute( query )
            db.commit()
    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not insert outstanding amounts ote data into db' )
        print traceback.format_exc()

    try:
        query = "SELECT date from estimated_portfolio_stats WHERE date = '%s'" % current_date
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) > 0:
            return True
        else:
            return False
    except Exception, err:
        send_mail( err, 'Could not insert into db' )
        print traceback.format_exc()

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('current_date')
    parser.add_argument('-d', type=str, help='Data source for prices and rates\nEg: -d csi\n Default is CSI',default='csi', dest='data_source')
    parser.add_argument('-t', type=str, help='Type  for products being ETFs\nEg: -t etf\n Default is future i.e. trading futures',default='future', dest='product_type')
    args = parser.parse_args()
    current_date = datetime.strptime(args.current_date, '%Y%m%d')
    product_type = args.product_type

    dump_eod_estimated_data(current_date, product_type, args.data_source)
