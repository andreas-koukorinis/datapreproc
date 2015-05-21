import argparse
import traceback
import os
import pandas as pd
import sys
import smtplib
import MySQLdb

home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_conversion_factors, connect_to_db

# TODO move to a separate class
future_code_mappings = { 'ES':'ES','C1':'6C', '21':'ZN', 'SF':'FESX', 'RH':'LFR', 'MP':'6M', 'BC' : 'FGBM', '17':'ZB', 'J1': '6J', 'SX' : 'SXF', 'CG' : 'CGB', '25': 'ZF', 'BM':'FGBL'}
  
month_codes = {'01':'F','02':'G','03':'H','04':'J','05':'K', '06':'M','07':'N','08':'Q','09':'U', '10':'V', '11':'X', '12':'Z'}

def send_mail( err, msg ):
  server = smtplib.SMTP( "localhost" )
  server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", \
      'EXCEPTION %s %s' % ( err, msg ) )

def dump_statement_data(date):
    # mny20150427.csv  mtdvolfeed20150427.csv  pos20150427.csv  prltrades2_20150427.csv  st420150427.csv  statement.pdf
    dir_path = '/apps/wedbush/' + date + '/'
    order_file = dir_path + 'st4' + date + '.csv'
    positions_file = dir_path + 'pos' + date + '.csv'
    commission_file = dir_path + 'mtdvolfeed' + date + '.csv'
    money_file = dir_path + 'mny' + date + '.csv'

    # Connect to db
    try:
        db = connect_to_db("fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", "live_trading", 'w')
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except Exception, err:
        print traceback.format_exc()
        send_mail( err, 'Could not connect to db' )

    # Fetch yday outstanding currency allocations
    outstanding_currencies_bal = ['segregated_USD_bal', 'secured_USD_bal', 'secured_GBP_bal', 'secured_EUR_bal', 'secured_CAD_bal']
    outstanding_currencies_ote = ['segregated_USD_ote', 'secured_USD_ote', 'secured_GBP_ote', 'secured_EUR_ote', 'secured_CAD_ote']
    outstanding_amounts_bal = dict.fromkeys(outstanding_currencies_bal, 0.0)
    outstanding_amounts_ote = dict.fromkeys(outstanding_currencies_ote, 0.0)
    try:
        # Update the outsatnding amounts in each currency
        money_df = pd.read_csv( money_file )
        money_df = money_df[money_df['MRECID'] == 'M'] # TODO check
        for i in range(len(money_df)):
            currency = str(money_df.iloc[i]['MCURAT']).strip()
            if money_df.iloc[i]['MATYPE'] == 'US':
                outstanding_amounts_bal['segregated_USD_bal'] = money_df.iloc[i]['MBAL']
                outstanding_amounts_ote['segregated_USD_ote'] = money_df.iloc[i]['MOTE']
            else:
                key = 'secured_' + currency
                outstanding_amounts_bal[key+'_bal'] = money_df.iloc[i]['MBAL']
                outstanding_amounts_ote[key+'_ote'] = money_df.iloc[i]['MOTE']
    except Exception, err:
        print traceback.format_exc()
        send_mail( err, 'Could not fetch outstanding currency from file' )
        
    # Fetch the currency rates from money file
    currency_rates = {}
    try:
        money_df = pd.read_csv( money_file )
        for i in range(len(money_df)):
            currency = money_df.iloc[i]['MCURAT']
            if currency != 'USD':
                currency = currency + 'USD'
            currency_rates[currency] = money_df.iloc[i]['MCVTFB']
    except Exception, err:
        print traceback.format_exc()
        send_mail( err, 'Could not process money file for currency data.EXITING' )
        #sys.exit()

    # Insert the currency rates into DB
    for currency in currency_rates.keys():
        try:
            query = "INSERT INTO currency_rates( date, currency, rate ) VALUES('%s','%s','%s')" % ( date, currency, currency_rates[currency] )
            db_cursor.execute( query )
            db.commit()
        except Exception, err:
            print traceback.format_exc()
            db.rollback()
            send_mail( err, 'Could not insert broker currency data.EXITING' )
            #sys.exit()

    try:
        # TODO broker realized pnl
        # Process money file
        money_df = pd.DataFrame.from_csv( money_file )
        # Assuming the last row of this file has all information we need
        last_idx = len( money_df.index ) - 1 
        converted_total_bal = money_df.iloc[last_idx]['MBAL']
        converted_total_ote = money_df.iloc[last_idx]['MOTE'] 
        portfolio_value = money_df.iloc[last_idx]['MLQVAL'] # Portfolio Value
        maintenance_margin = money_df.iloc[last_idx]['MFMR'] 
        initial_margin = money_df.iloc[last_idx]['MFIR']
        commission = 0
        try:
            commissions_df = pd.DataFrame.from_csv( commission_file )
            commissions_df = commissions_df[commissions_df['WDATID']=='M'] # TODO WHY
            columns = ['COMMISSION', 'CLEARING_FEE', 'NFA_FEE', 'EXCHANG_EFE', 'WIR_FEE', 'OTH_FEE']
            # TODO currency wise
            for column in columns:
                for i in range(len(commissions_df)):
                    currency = str(commissions_df.iloc[i][column + '_C']).strip()
                    if currency != 'USD':
                        currency += 'USD'
                        rate = currency_rates[currency]
                        commission += commissions_df.iloc[i][column] * rate
                    else:
                        commission += commissions_df.iloc[i][column]
        except IOError:
            commission = 0
            print traceback.format_exc()
        except Exception, err:
            send_mail( err, 'Could not process commission file' )
            print traceback.format_exc()
        try:
            query = "INSERT INTO broker_portfolio_stats ( date, converted_total_bal, converted_total_ote, portfolio_value, maintainence_margin, initial_margin,commission ) VALUES('%s','%s','%s','%s','%s','%s', '%0.2f')" % ( date, converted_total_bal, converted_total_ote, portfolio_value, maintenance_margin, initial_margin, abs( commission ) )
            db_cursor.execute( query )
            db.commit()
            # Update PNL
            try:
                query = "SELECT date, portfolio_value FROM broker_portfolio_stats WHERE date < %s ORDER BY date DESC limit 1" % ( date )
                db_cursor.execute( query )
                rows = db_cursor.fetchall()
                if len( rows ) == 1:
                   yday_portfolio_value = float( rows[0]['portfolio_value'] )
                   today_portfolio_value = portfolio_value
                   pnl = today_portfolio_value - yday_portfolio_value
                   try:
                       query = "UPDATE broker_portfolio_stats SET pnl = '%s' WHERE date = %s " % ( pnl, date )
                       db_cursor.execute( query )
                       db.commit()
                   except Exception, err:
                       db.rollback()
                       send_mail( err, 'Could not insert broker pnl data into db' )
                       print traceback.format_exc()
                else:
                    send_mail( '', 'Something weird while getting yday portfolio value in dump_statement_data.py line 117' ) 
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not calculate broker pnl' )
                print traceback.format_exc()

            # Update outstanding balance
            try:
                for key in outstanding_amounts_bal.keys():
                    query = "UPDATE broker_portfolio_stats SET %s = '%s' WHERE date = %s " % ( key, outstanding_amounts_bal[key], date )
                    db_cursor.execute( query )
                    db.commit()
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not insert outstanding amounts bal data into db' )
                print traceback.format_exc()

            # Update outstanding ote
            try:
                for key in outstanding_amounts_ote.keys():
                    query = "UPDATE broker_portfolio_stats SET %s = '%s' WHERE date = %s " % ( key, outstanding_amounts_ote[key], date )
                    db_cursor.execute( query )
                    db.commit()
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not insert outstanding amounts ote data into db' )
                print traceback.format_exc()

        except Exception, err:
            db.rollback()
            send_mail( err, 'Could not insert broker money file data into db' )
            print traceback.format_exc()
    except Exception, err:
        send_mail( err, 'Could not process money file' )
        print traceback.format_exc()

    try:
        # Process positions file
        positions_df = pd.DataFrame.from_csv( positions_file, index_col='PFC' )
        products = []

        for fc in positions_df.index:
            products.append(future_code_mappings[fc]+'_1') # TODO how will we handle VIX

        conversion_factors = get_conversion_factors(products)
        positions = []

        for fc in positions_df.index:
            contract_year_month_code = str(positions_df.loc[fc]['PCTYM'])
            month = contract_year_month_code[-2:]
            year = contract_year_month_code[2:4]
            product_symbol = future_code_mappings[fc] + month_codes[month] + year
            amount = positions_df.loc[fc]['PBQTY'] - positions_df.loc[fc]['PSQTY']
            broker_close_price = positions_df.loc[fc]['PCLOSE'] * positions_df.loc[fc]['PMULTF'] / conversion_factors[future_code_mappings[fc]+'_1']
            broker_average_trade_price = positions_df.loc[fc]['PTPRIC'] * positions_df.loc[fc]['PMULTF'] / conversion_factors[future_code_mappings[fc] + '_1']
            query = "SELECT * FROM positions WHERE date ='%s' AND product = '%s'" % ( date, product_symbol )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            if len(rows) > 0:             
                try:
                    query = "UPDATE positions SET broker_position = '%d', broker_average_trade_price = '%s', broker_close_price = '%s' WHERE date = '%s' AND product = '%s'" \
                             % ( amount, broker_average_trade_price, broker_close_price, date, product_symbol )
                    db_cursor.execute(query)
                    db.commit()
                except Exception, err:
                    db.rollback()
                    send_mail( err, 'Could not insert broker position file data into db' )
                    print traceback.format_exc()
            else:
                try:
                    query = "INSERT INTO positions ( date, product, broker_position, broker_average_trade_price, broker_close_price ) VALUES('%s','%s','%d','%s','%s')" \
                            % ( date, product_symbol, amount, broker_average_trade_price, broker_close_price )
                    db_cursor.execute(query)
                    db.commit()
                except Exception, err:
                    db.rollback()
                    send_mail( err, 'Could not insert broker position file data into db' )
                    print traceback.format_exc()
    except Exception, err:
        send_mail( err, 'Could not process positions file' )
        print traceback.format_exc()

    # TODO process commission file

    # Process order file
    try:
        order_df = pd.DataFrame.from_csv( order_file, index_col='POFFIC' )
        order_df = order_df[ order_df['PRECID'] == 'T']
        conversion_factors = get_conversion_factors(products)
        for i in range( len (order_df ) ):
            contract_year_month_code = str(order_df.iloc[i]['PCTYM'])
            month = contract_year_month_code[-2:]
            year = contract_year_month_code[2:4]
            symbol = str(order_df.iloc[i]['PFC'])
            product_symbol = future_code_mappings[symbol] + month_codes[month] + year
            buy_or_sell = order_df.iloc[i]['PBS']
            if buy_or_sell == 1:
                amount = order_df.iloc[i]['PQTY'] # TODO what is PPRTQ
            else:
                amount = -1*order_df.iloc[i]['PQTY']
            trade_price = order_df.iloc[i]['PPRICE'] # TODO some issue with ZN, and is PPRICE correct
            commission = 0
            columns = ['PCOMM','PFEE1','PFEE2','PFEE3','PFEE4','PFEE5','PFEE6','PFEE7','PFEE8','PFEE9','PMWIRE'] #PCOMM ->  POVNC ?? same
            # TODO Depends on currency
            try:
                query = "INSERT INTO broker_orders ( date, product, amount, trade_price, commission ) VALUES('%s','%s','%d','%s','%s')" \
                        % ( date, product_symbol, amount, trade_price, commission )
                db_cursor.execute(query)
                db.commit()
            except Exception, err:
                db.rollback()
                send_mail( err, 'Could not insert broker order data into db' )
                print traceback.format_exc()
   
    except Exception, err:
        send_mail( err, 'Could not process order file' )
        print traceback.format_exc()
    
    try:
        query = "SELECT date from broker_portfolio_stats WHERE date = '%s')" % date
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
    # Statement file: includes trades and cash transfers: st420150428.csv
    # Positions of positions in portfolio: pos20150428.csv
    # Commission paid on each trade: mtdvolfeed20140428.csv
    # Has information on cash, open equity, margin etc: mny20150428.csv
    parser.add_argument('date')
    args = parser.parse_args()
    dump_statement_data(args.date)
