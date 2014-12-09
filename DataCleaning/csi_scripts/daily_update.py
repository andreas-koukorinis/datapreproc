#!/usr/bin/env python

import sys
import os
import imp
import gzip
import MySQLdb
import subprocess
import pandas as pd
from datetime import datetime,timedelta,date
#from exchange_symbol_manager import ExchangeSymbolManager 

table = {}
product_type = {}
db_cursor = None
db = None
mappings = {'AD1':'6A','BP1':'6B','CD1':'6C','CU1':'6E','JY1':'6J','MP':'6M','EX':'E7','ES':'ES','FDX':'FDAX','SXE':'FESX','EBL':'FGBL','EBM':'FGBM','JT':'J7','FLG':'LFR','NK':'NKD','SXF':'SXF','ZF':'ZF','TY':'ZN'}
exchange_symbol_mamager = None

def FloatOrZero(value):
    try:
        return float(value)
    except:
        print 'ALERT! Empty string being converted to float'
        return 0.0

def IntOrZero(value):
    try:
        return int(value)
    except:
        print 'ALERT! Empty string being converted to int'
        return 0

def db_connect():
    global db,db_cursor
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="daily_qplum")
        db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    except MySQLdb.Error:
        sys.exit("Error In DB Connection")

def product_to_table_map():
    global table,product_type
    query = 'SELECT * FROM products'
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    for row in rows:
        table[row['product']] = row['table']
        product_type = row['type']      

def get_exchange_specific(YYMM):
    YY,MM = YYMM[0:2],YYMM[2:4]
    month_codes = {'01':'F','02':'G','03':'H','04':'J','05':'K','06':'M','07':'N','08':'Q','09':'U','10':'V','11':'X','12':'Z'}
    return month_codes[MM]+YY

# TODO Check if it is a past contract: should not be
def get_contract_number(date, _base_symbol, YYMM ):
    _exchange_symbol = _base_symbol + get_exchange_specific(YYMM)
    num=1
    while _exchange_symbol != exchange_symbol_manager.get_exchange_symbol( date, _base_symbol + '_' + str(num) ) and num < 20:   
        num+=1
    if num == 20:
        num=0 
    return num

def get_file(filename,k):
    filename = filename +'.' + (datetime.now() - timedelta(days=k)).strftime('%Y%m%d')
    print filename
    path = '/home/cvdev/stratdev/DataCleaning/'
    if not os.path.isfile(path+filename): #If the file is not present,download it
        _file = filename+'.gz'
        is_in_s3 = subprocess.Popen(['s3cmd', 'ls', 's3://cvquantdata/csi/rawdata/'+_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
        if len(is_in_s3) <= 0:
            #sys.exit('File %s not in s3'%_file)
            print 'File %s not in s3'%_file
            return '0'
        subprocess.call(['s3cmd','get','s3://cvquantdata/csi/rawdata/'+_file]) 
        inF = gzip.open(_file, 'rb')
        outF = open(filename, 'wb')
        outF.write( inF.read() )
        inF.close()
        outF.close()
    return filename

def add_stock_quote(date,record):
    product, open, high, low, close, volume = record[1], FloatOrZero(record[3]), FloatOrZero(record[4]), FloatOrZero(record[5]), FloatOrZero(record[6]), IntOrZero(record[8])*100
    try:
        db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date < '%s' ORDER BY date DESC LIMIT 1"%(table[product],product,date))
        rows = db_cursor.fetchall()
        if len(rows) < 1:
            current_dividend_factor = 1.0
        else:
            current_dividend_factor = float(rows[0]['forward_adjusted_close'])/float(rows[0]['close'])
        forward_adjusted_close = close*current_dividend_factor
        query = "INSERT INTO %s ( date, product, open, high,low, close, backward_adjusted_close, forward_adjusted_close, volume, dividend ) VALUES('%s','%s','%0.2f','%0.2f','%0.2f','%0.2f','%0.2f','%0.2f','%d','0.0')" % ( table[product], date, product, open, high, low, close, close, forward_adjusted_close, volume)
        print query
        db_cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        sys.exit('EXCEPTION in add_stock_quote %s'%record)
    
def add_fund_quote(date,record):
    product, csi_num, close, asking_price = record[1], IntOrZero(record[2]), FloatOrZero(record[3]), FloatOrZero(record[4])    
    try:
        db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date < '%s' ORDER BY date DESC LIMIT 1"%(table[product],product,date))
        rows = db_cursor.fetchall()
        if len(rows) < 1:
            current_dividend_factor = 1.0
        else:
            current_dividend_factor = float(rows[0]['forward_adjusted_close'])/float(rows[0]['close'])
        forward_adjusted_close = close*current_dividend_factor
        query = "INSERT INTO %s ( date, product, close, asking_price, backward_adjusted_close, forward_adjusted_close, dividend, capital_gain ) VALUES('%s','%s','%0.2f','%0.2f','%0.2f','%0.2f','0.0','0.0')" % ( table[product], date, product, close, asking_price, close, forward_adjusted_close)
        print query
        db_cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        sys.exit('EXCEPTION in add_fund_quote %s'%record)

def add_future_quote(date,record,future_someday_total_volume,future_someday_total_oi,future_volume_date,future_oi_date):
    try:
        #print record
        product, csi_num, YYMM, open1, high, low, close, prev_close, future_someday_volume, future_someday_oi = record[1], IntOrZero(record[2]), record[3], FloatOrZero(record[4]), FloatOrZero(record[6]), FloatOrZero(record[7]), FloatOrZero(record[8]), FloatOrZero(record[9]), IntOrZero(record[10]), IntOrZero(record[11])   
        if product in mappings.keys():
            _base_symbol = mappings[product]
        else:
            _base_symbol = product
        _last_trading_date = exchange_symbol_manager.get_last_trading_date(datetime.strptime(date, '%Y-%m-%d').date(), _base_symbol + '_1')
        if str(_last_trading_date) == date:
            is_last_trading_day = 1.0
        else:
            is_last_trading_day = 0.0
        #print str(_last_trading_date),date,_base_symbol
        contract_number = get_contract_number(datetime.strptime(date, '%Y-%m-%d').date(), _base_symbol, YYMM)
        specific_ticker = _base_symbol + get_exchange_specific( YYMM )
        generic_ticker = _base_symbol + '_' + str( contract_number )
        if contract_number in [1,2]: # TODO Should change to mapping per product 
            try:
                query = "INSERT INTO %s ( date, product, specific_ticker, open, high, low, close, is_last_trading_day, contract_volume, contract_oi, total_volume, total_oi ) VALUES('%s','%s','%s','%f','%f','%f','%f','%0.1f','0','0','0','0')" % ( table[generic_ticker], date, generic_ticker, specific_ticker, open1, high, low, close, is_last_trading_day )
                print query
                db_cursor.execute(query)
                db.commit()
            except:
                db.rollback()
                sys.exit('EXCEPTION in add_future_quote %s'%record)
        if contract_number in [0,1,2]:
            try:
                query = "UPDATE %s SET contract_volume='%d',total_volume='%d' WHERE date='%s' AND specific_ticker='%s'" % (table[_base_symbol+'_1'], future_someday_volume, future_someday_total_volume, future_volume_date, specific_ticker)
                print query
                db_cursor.execute(query)
                query = "UPDATE %s SET contract_oi='%d',total_oi='%d' WHERE date='%s' AND specific_ticker='%s'" % (table[_base_symbol+'_1'], future_someday_oi, future_someday_total_oi, future_oi_date, specific_ticker)
                print query
                db_cursor.execute(query)
                db.commit()
            except:
                db.rollback()
                sys.exit('EXCEPTION in add_future_quote %s'%record)
    except:
        sys.exit('EXCEPTION in add_future_quote %s'%record)

# ASSUMPTION: dividend quote will be sequenced after price quote
def dividend_quote(date,record):
    if len(record) > 5: # ASSUME CAPITAL GAIN ONLY IF RECORD LEN > 5
        product, csi_num, ex_date, dividend, capital_gain = record[1], IntOrZero(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'),FloatOrZero(record[4]), FloatOrZero(record[5])
    else:
        product, csi_num, ex_date, dividend, capital_gain = record[1], IntOrZero(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'), FloatOrZero(record[4]),0.0
    try:
        if table[product] == 'funds':
            query = "UPDATE %s SET dividend='%f',capital_gain='%f' WHERE product='%s' AND date='%s'" % ( table[product], dividend, capital_gain, product, ex_date)
            print query
            db_cursor.execute(query)
            query = "SELECT * FROM %s WHERE product='%s' AND date='%s'"%(table[product],product,ex_date)
            print query
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            print rows  
            if len(rows) < 1:
                sys.exit('Price quote did not preceed dividend quote')
            else:
                ex_close = float(rows[0]['close'])
            dividend_factor = 1 + (dividend+capital_gain)/ex_close
            query = "UPDATE %s SET backward_adjusted_close = backward_adjusted_close/%f WHERE product='%s' AND date < '%s'" % ( table[product], dividend_factor, product, ex_date)
            print query
            db_cursor.execute(query)
            query = "UPDATE %s SET forward_adjusted_close = forward_adjusted_close*%f WHERE product='%s' AND date >= '%s'" % ( table[product], dividend_factor, product, ex_date)
            print query
            db_cursor.execute(query)
        else:
            query = "UPDATE %s SET dividend='%f' WHERE product='%s' AND date='%s'" % ( table[product], dividend,  product, ex_date)
            print query
            db_cursor.execute(query)
            query = "SELECT * FROM %s WHERE product='%s' AND date='%s'"%(table[product],product,ex_date)
            print query
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            print rows
            if len(rows) < 1:
                sys.exit('Price quote did not preceed dividend quote')
            else:
                ex_close = float(rows[0]['close'])
            dividend_factor = 1 + (dividend)/ex_close
            query = "UPDATE %s SET backward_adjusted_close = backward_adjusted_close/%f WHERE product='%s' AND date < '%s'" % ( table[product], dividend_factor, product, ex_date)
            print query
            db_cursor.execute(query)
            query = "UPDATE %s SET forward_adjusted_close = forward_adjusted_close*%f WHERE product='%s' AND date >= '%s'" % ( table[product], dividend_factor, product, ex_date)
            print query
            db_cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        sys.exit('EXCEPTION in dividend_quote %s'%record)

def split_quote(date,record):
    product, csi_num, ex_date, new_shares, old_shares = record[1], IntOrZero(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'), FloatOrZero(record[4]), FloatOrZero(record[5])
    try:
        split_factor = new_shares/old_shares
        if product_type[product] == 'fund':
            query = "UPDATE %s SET close=close/%f, asking_price=asking_price/%f, backward_adjusted_close=backward_adjusted_close/%f, forward_adjusted_close=forward_adjusted_close/%f, dividend=dividend/%f, capital_gain=capital_gain/%f WHERE product='%s' AND date < '%s'" % ( table[product], split_factor, split_factor, split_factor, split_factor, split_factor, split_factor, product, ex_date)
        else:
            query = "UPDATE %s SET open=open/%f, high=high/%f, low=low/%f, close=close/%f, backward_adjusted_close=backward_adjusted_close/%f, forward_adjusted_close=forward_adjusted_close/%f, volume=volume*%d, dividend=dividend/%f WHERE product='%s' AND date < '%s'" % ( table[product], split_factor, split_factor, split_factor, split_factor, split_factor, split_factor, IntOrZero(split_factor), split_factor, product, ex_date)
        print query
        db_cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        sys.exit('EXCEPTION in split_quote %s'%record)

def error_correction_quote(date,record):
    print 'IN ERROR CORRECTION'
    #update_record(date,record)

def delete_quote(date,record):
    date_to_be_deleted, product, csi_num, delivery_YYMM, option_flag, strike_price = datetime.strptime(record[1], '%Y%m%d').strftime('%Y-%m-%d'),record[2], IntOrZero(record[3]), record[4], record[5], FloatOrZero(record[6])
    try:
        query = "DELETE FROM %s WHERE date='%s' AND product='%s'"
        db_cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        sys.exit('EXCEPTION in delete_quote %s'%record)

def daily_update(filename,products):
    f = open(filename)
    records = f.readlines()
    f.close()      
    for item in records:
        record = item.strip().split(',')
        if len(record) > 1:
            symbol = record[1]
        if record[0]=='00': #Header/Trailer
            portfolio_identifier = record[1]
            file_type = record[2]
            record_count = IntOrZero(record[3])
            date = datetime.strptime(record[4], '%Y%m%d').strftime('%Y-%m-%d')
            day = record[5]
            volume_date = datetime.strptime(record[6], '%Y%m%d').strftime('%Y-%m-%d')
            oi_date = datetime.strptime(record[7], '%Y%m%d').strftime('%Y-%m-%d')

        elif record[0]=='01': # Future header
            future_symbol, future_csi_num, option_flag, future_total_volume,future_total_oi, future_total_est_volume = record[1], IntOrZero(record[2]), IntOrZero(record[3]), IntOrZero(record[4]), IntOrZero(record[5]), IntOrZero(record[6])
            if len(record) > 8:
                #print record
                try:
                    future_volume_date = datetime.strptime(record[8], '%Y%m%d').strftime('%Y-%m-%d')
                except:
                    future_volume_date = '1500-01-01'
            else:
                future_volume_date = volume_date
            if len(record) > 9:
                try:
                    future_oi_date = datetime.strptime(record[9], '%Y%m%d').strftime('%Y-%m-%d')
                except:
                    future_oi_date = '1500-01-01'
            else:
                future_oi_date = oi_date

        elif record[0]=='02': # Future price record
            if len(products)==0 or symbol in products:
                add_future_quote(date,record,future_total_volume,future_total_oi,future_volume_date,future_oi_date)

        elif record[0]=='03': #Stock Price record
            if len(products)==0 or symbol in products:
                add_stock_quote(date,record)
            
        elif record[0]=='06': #Mutual fund record
            if len(products)==0 or symbol in products:
                add_fund_quote(date,record)

        elif record[0]=='07': #Dividen/Capital Gain record
            if len(products)==0 or symbol in products:
                dividend_quote(date,record)

        elif record[0]=='08': #Stock Split record
            if len(products)==0 or symbol in products:
                split_quote(date,record)

        elif record[0]=='09': #Error Correction record
            symbol = record[2]
            if len(products)==0 or symbol in products:
                error_correction_quote(date,record)            

        elif record[0]=='11': #Fact sheet modifications
            continue

        elif record[0]=='13': #Delete past day of data
            if len(products)==0 or symbol in products:
                delete_quote(date,record)

        elif record[0]=='15': #Fact table entry
            continue

        elif record[0]=='32': # Future price record
            if len(products)==0 or symbol in products:
                add_future_quote(date,record,future_total_volume,future_total_oi,future_volume_date,future_oi_date)

        elif record[0]=='33': #Type '03' with prices in decimal
            if len(products)==0 or symbol in products:
                add_stock_quote(date,record)
        
        elif record[0]=='36': #Type '06' with prices in decimal
            if len(products)==0 or symbol in products:
                add_fund_quote(date,record)

        elif record[0]=='37': #Type '07' with prices in decimal
            if len(products)==0 or symbol in products:
                dividend_quote(date, record)

def update_last_trading_day(k):
    _date = date.today() + timedelta(days=-k)
    for product in mappings.keys():
        _base_symbol = mappings[product]
        _last_trading_date = exchange_symbol_manager.get_last_trading_date(_date, _base_symbol + '_1')
        if _last_trading_date != _date:
            continue
        _contract_numbers = [1,2] # TODO should have mapping for this
        for _contract_number in _contract_numbers:
            generic_ticker = _base_symbol + '_' + str( _contract_number )
            try:
                query = "SELECT * FROM %s WHERE product='%s' AND date <= '%s' ORDER BY date DESC LIMIT 1"%(table[generic_ticker],generic_ticker,_date)
                print query
                db_cursor.execute(query)
                rows = db_cursor.fetchall()
                if len(rows) < 1:
                    print 'Unhandled case %s in update_last_trading_day'%(generic_ticker) 
                else:
                    _new_last_trading_date= rows[0]['date']
                    delta = _date - _new_last_trading_date
                    if delta.days >= 7:
                        print 'Seems to be a problem in update_last_trading_day %s'%generic_ticker
                    else:
                        query = "UPDATE %s SET is_last_trading_day=1.0 WHERE product='%s' AND date='%s'"%(table[generic_ticker],generic_ticker,_new_last_trading_date)
                        print query
                        db_cursor.execute(query)
                db.commit()
            except:
                db.rollback()
                sys.exit('EXCEPTION in update_last_trading_day %s'%generic_ticker)

def __main__() :
    if len( sys.argv ) > 1:
        filename = sys.argv[1]
        products = []
        for i in range(3,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python daily_update.py file:canada/f-indices/funds/futures/indices/uk-stocks/us-stocks delay product1 product2 ... productn'
        sys.exit(0)
    global exchange_symbol_manager
    module = imp.load_source('exchange_symbol_manager', '../exchange_symbol_manager.py')
    exchange_symbol_manager = module.ExchangeSymbolManager()
    filename = get_file(filename, int(sys.argv[2]))
    #print filename
    #sys.exit()
    db_connect()
    product_to_table_map()
    if filename != '0':
        daily_update(filename, products)
    update_last_trading_day(int(sys.argv[2]))

if __name__ == '__main__':
    __main__()
