import argparse
import os
import pandas as pd
import sys
import smtplib
import MySQLdb

home_path = os.path.expanduser("~")
sys.path.append(home_path + '/stratdev/')
from utils.dbqueries import get_conversion_factors, connect_to_db

# TODO move to a separate class
future_code_mappings = {'RH':'LFR','SF':'FESX','21':'ZN','ES':'ES','C1':'6C'}
month_codes = {'01':'F','02':'G','03':'H','04':'J','05':'K', '06':'M','07':'N','08':'Q','09':'U', '10':'V', '11':'X', '12':'Z'}

def send_mail( err, msg ):
  server = smtplib.SMTP( "localhost" )
  server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", \
      'EXCEPTION %s %s' % ( err, msg ) )

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    #parser.add_argument('order_file') # Statement file: includes trades and cash transfers: st420150428.csv
    #parser.add_argument('positions_file') # Positions of positions in portfolio: pos20150428.csv
    #parser.add_argument('commission_file') # Commission paid on each trade: mtdvolfeed20140428.csv
    #parser.add_argument('money_file') # Has information on cash, open equity, margin etc: mny20150428.csv
    parser.add_argument('date')
    args = parser.parse_args()
    date = args.date

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
        send_mail( err, 'Could not connect to db' )

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
            broker_close = positions_df.loc[fc]['PCLOSE'] * positions_df.loc[fc]['PMULTF'] / conversion_factors[future_code_mappings[fc]+'_1']
            broker_average_trade_price = positions_df.loc[fc]['PTPRIC'] * positions_df.loc[fc]['PMULTF'] / conversion_factors[future_code_mappings[fc] + '_1']
   
    except Exception, err:
        send_mail( err, 'Could not process positions file' )

    try:
        # TODO update if already present
        query = "INSERT INTO positions ( date, product, broker_position, broker_average_trade_price, broker_close_price ) VALUES('%s','%s','%d','%s','%s')" \
                % ( date, product_symbol, amount, broker_average_trade_price, broker_close )
        db_cursor.execute(query)
        db.commit()

    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not insert broker position file data into db' )

    try:
        # TODO realized pnl and commission check
        # Process money file
        money_df = pd.DataFrame.from_csv( money_file )
        # Assuming the last row of this file has all information we need
        last_idx = len( money_df.index ) - 1 
        broker_cash = money_df.iloc[last_idx]['MBAL'] # Cash in $
        broker_portfolio_value = money_df.iloc[last_idx]['MLQVAL'] # Portfolio Value
        maintenance_margin = money_df.iloc[last_idx]['MFMR'] 
        initial_margin = money_df.iloc[last_idx]['MFIR']
        commission = 0
        try:
            commissions_df = pd.DataFrame.from_csv( commission_file )
            commissions_df = commissions_df[commissions_df['WDATID']=='M']
            columns = ['COMMISSION', 'CLEARING_FEE', 'NFA_FEE', 'EXCHANG_EFE', 'WIR_FEE', 'OTH_FEE']
            for column in columns:
                commission += sum(commissions_df[column])
        except IOError:
            commission = 0
        except Exception, err:
            send_mail( err, 'Could not process commission file' )
    except Exception, err:
        send_mail( err, 'Could not process money file' )

    try:
        # TODO update if already present
        query = "INSERT INTO portfolio_stats ( date, broker_cash, broker_portfolio_value, maintainence_margin, initial_margin, commission ) VALUES('%s','%s','%s','%s','%s', '%s')" \
                % ( date, broker_cash, broker_portfolio_value, maintenance_margin, initial_margin, commission )
        db_cursor.execute( query )
        db.commit()

    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not insert broker money file data into db' )

    try:
        # TODO update if already present
        query = "SELECT date, broker_portfolio_value FROM portfolio_stats WHERE date < %s ORDER BY date DESC limit 1" % ( date )
        db_cursor.execute( query )
        rows = db_cursor.fetchall()
        if len( rows ) == 1:
           yday_broker_portfolio_value = rows[0]['broker_portfolio_value']
           broker_pnl = broker_portfolio_value - yday_broker_portfolio_value
           try:
               query = "UPDATE portfolio_stats SET broker_pnl = '%s' WHERE date = %s " % ( broker_pnl, date )
               db_cursor.execute( query )
               db.commit()
           except Exception, err:
               db.rollback()
               send_mail( err, 'Could not insert broker pnl data into db' )
        else:
           send_mail( '', 'Something wierd while getting yday portfolio value in dump_statement_data.py line 117' ) 

    except Exception, err:
        db.rollback()
        send_mail( err, 'Could not insert broker money file data into db' )
    
    # TODO process commission file

    # TODO process order file

if __name__ == '__main__':
    main()
