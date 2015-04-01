#!/usr/bin/env python
import traceback
import sys
import os
from sets import Set
import imp
import gzip
import smtplib
import MySQLdb
from datetime import datetime,timedelta,date
#from exchange_symbol_manager import ExchangeSymbolManager 

dividend_adjust_products = Set()
futures_contract_list = {'VX':[1,2,3,4,5,6,7]}
indices = ['VIX']
table = {}
product_type = {}
server = None
db_cursor = None
db = None
mappings = {'TU':'ZT','FV':'ZF','TY':'ZN','US':'ZB','NK':'NKD','NIY':'NIY','ES':'ES','EMD':'EMD','NQ':'NQ','YM':'YM','AD':'6A','BP':'6B','CD':'6C','CU1':'6E','JY':'6J','MP':'6M','NE2':'6N','SF':'6S','GC':'GC','SI':'SI','HG':'HG','PL':'PL','PA':'PA','LH':'LH','ZW':'ZW','ZC':'ZC','ZS':'ZS','ZM':'ZM','ZL':'ZL','EBS':'FGBS','EBM':'FGBM','EBL':'FGBL','SXE':'FESX','FDX':'FDAX','SMI':'FSMI','SXF':'SXF','CGB':'CGB','FFI':'LFZ','FLG': 'LFR','AEX': 'FTI','KC':'KC','CT':'CT','CC':'CC','SB':'SB','JTI':'TOPIX','JGB':'JGBL','JNI':'JNK','SIN':'SIN','SSG':'SG','HCE':'HHI','HSI':'HSI','ALS':'ALSI','YAP':'SPI','MFX':'MFX','KOS':'KOSPI','VX':'VX'}

forex_mappings = { 'US$_46' : ('JPYUSD',True) ,'US$_39' : ('CADUSD',True), 'GB2_60' : ('GBPUSD',False), 'EU2_60' : ('EURUSD',False), 'US$_37': ('AUDUSD', True), 'US$_51': ('NZDUSD', True), 'US$_53': ('CHFUSD', True), 'US$_58': ('SEKUSD', True), 'US$_49': ('NOKUSD', True), 'FX1_39': ('TRYUSD', True), 'US$_56': ('MXNUSD', True), 'US$_57': ('ZARUSD', True), 'U2$_55': ('ILSUSD', True), 'US$_52': ('SGDUSD', True), 'US$_44': ('HKDUSD', True), 'U2$_53': ('TWDUSD', True) }

exchange_symbol_mamager = None

def FloatOrZero(value):
    try:
        return float(value)
    except Exception, err:
        print traceback.format_exc()
        print 'ALERT! Empty string being converted to float'
        return 0.0

def IntOrZero(value):
    try:
        return int(value)
    except Exception, err:
        print traceback.format_exc()
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
        product_type[row['product']] = row['type']      

def get_exchange_specific(YYMM):
    YY,MM = YYMM[0:2],YYMM[2:4]
    month_codes = {'01':'F','02':'G','03':'H','04':'J','05':'K','06':'M','07':'N','08':'Q','09':'U','10':'V','11':'X','12':'Z'}
    return month_codes[MM]+YY

def compare_ticker(ticker1, ticker2):
    month_codes_rev = {'F':'01','G':'02','H':'03','J':'04','K':'05','M':'06','N':'07','Q':'08','U':'09','V':'10','X':'11','Z':'12'}
    code1 = convert_to_year_month(ticker1[-2:] + month_codes_rev[ticker1[-3]])
    code2 = convert_to_year_month(ticker2[-2:] + month_codes_rev[ticker2[-3]])
    return code1 > code2

def convert_to_year_month(YYMM):
    yy = int(YYMM[0:2])
    mm = int(YYMM[2:4])
    if yy >50:
        year = 1900 + yy
    else:
        year = 2000 + yy
    return year*100 + mm

# TODO Check if it is a past contract: should not be
def get_contract_number(date, _base_symbol, YYMM ):
    _exchange_symbol = _base_symbol + get_exchange_specific(YYMM)
    first_contract = exchange_symbol_manager.get_exchange_symbol( date, _base_symbol + '_1')
    if compare_ticker(first_contract,_exchange_symbol):
        return 0
    num=1
    # 
    while _exchange_symbol != exchange_symbol_manager.get_exchange_symbol( date, _base_symbol + '_' + str(num) ) and num < len(futures_contract_list.get(_base_symbol,[1,2]))+1:
        #print _exchange_symbol, exchange_symbol_manager.get_exchange_symbol( date, _base_symbol + '_' + str(num))   
        num+=1
    if num > len(futures_contract_list.get(_base_symbol,[1,2])):
        num=-1 
    return num

def get_file(filename,k):
    filename = filename +'.' + (datetime.now() - timedelta(days=k)).strftime('%Y%m%d')
    print filename
    path = '/apps/data/csi/'
    if not os.path.isfile(filename): #If the file is not present,download it
        _file = path+filename+'.gz'
        # is_in_s3 = subprocess.Popen(['s3cmd', 'ls', 's3://cvquantdata/csi/rawdata/'+_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
        # if len(is_in_s3) <= 0:
        #     #sys.exit('File %s not in s3'%_file)
        #     print 'File %s not in s3'%_file
        #     return None
        # subprocess.call(['s3cmd','get','s3://cvquantdata/csi/rawdata/'+_file]) 
        inF = gzip.open(_file, 'rb')
        outF = open(filename, 'wb')
        outF.write( inF.read() )
        inF.close()
        outF.close()
    return filename

def add_stock_quote(date, record, error_correction):
    product, open, high, low, close, volume = record[1], FloatOrZero(record[3]), FloatOrZero(record[4]), FloatOrZero(record[5]), FloatOrZero(record[6]), IntOrZero(record[8])*100
    #print record
    if product not in indices:
        product, open, high, low, close, volume = record[1], FloatOrZero(record[3]), FloatOrZero(record[4]), FloatOrZero(record[5]), FloatOrZero(record[6]), IntOrZero(record[8])*100
        if error_correction:
            try:
                db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date = '%s'"%(table[product],product,date))
                rows = db_cursor.fetchall()
                if float(rows[0]['dividend']) > 0.0:
                    dividend_adjust_products.add(product)
            except Exception, err:
                print traceback.format_exc()
                print('EXCEPTION in add_stock_quote in error corection with dividend: %s'%record)
                server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_stock_quote in error corection with dividend: %s'%record) 
        try:
            db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date < '%s' ORDER BY date DESC LIMIT 1"%(table[product],product,date))
            rows = db_cursor.fetchall()
            if len(rows) < 1:
                current_dividend_factor = 1.0
            else:
                current_dividend_factor = float(rows[0]['forward_adjusted_close'])/float(rows[0]['close'])
            forward_adjusted_close = close*current_dividend_factor
            forward_adjusted_open = open*current_dividend_factor
            if error_correction:
                db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date > '%s' ORDER BY date LIMIT 1"%(table[product],product,date))
                rows = db_cursor.fetchall()
                if len(rows) < 1:
                    current_dividend_factor = 1.0
                else:
                    current_dividend_factor = float(rows[0]['backward_adjusted_close'])/float(rows[0]['close'])
                backward_adjusted_close = close*current_dividend_factor
                backward_adjusted_open = open*current_dividend_factor
                query = "UPDATE %s SET open='%f', high='%f', low='%f', close ='%f', backward_adjusted_close='%f', forward_adjusted_close='%f', backward_adjusted_open='%f', forward_adjusted_open='%f', volume='%d' WHERE date='%s' and product='%s'" \
                        % (table[product], open, high, low, close, backward_adjusted_close, forward_adjusted_close, backward_adjusted_open, forward_adjusted_open, volume, date, product)
            else:
                query = "INSERT INTO %s ( date, product, open, high,low, close, backward_adjusted_close, forward_adjusted_close, backward_adjusted_open, forward_adjusted_open, volume, dividend ) VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%d','0.0')" % ( table[product], date, product, open, high, low, close, close, forward_adjusted_close, open, forward_adjusted_open, volume)
            print query
            db_cursor.execute(query)
            db.commit()
        except Exception, err:
            print traceback.format_exc()
            db.rollback()
            print('EXCEPTION in add_stock_quote %s'%record)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_stock_quote %s'%record)
    else:
        product, open, high, low, close = record[1], FloatOrZero(record[3]), FloatOrZero(record[4]), FloatOrZero(record[5]), FloatOrZero(record[6])
        try:
            if error_correction:
                query = "UPDATE %s SET open='%f', high='%f', low='%f', close ='%f' WHERE date='%s' and product='%s'"%(table[product], open, high, low, close, date, product)
            else:
                query = "INSERT INTO %s (date, product, open, high, low, close) VALUES('%s','%s','%f','%f','%f','%f')" % (table[product], date, product, open, high, low, close)
            print query
            db_cursor.execute(query)
            db.commit()
        except Exception, err:
            print traceback.format_exc()
            db.rollback()
            print('EXCEPTION in add_stock_quote(index) %s'%record)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_stock_quote(index) %s'%record)


def add_fund_quote(date, record, error_correction):
    product, csi_num, close, asking_price = record[1], IntOrZero(record[2]), FloatOrZero(record[3]), FloatOrZero(record[4])    
    if error_correction:
        try:
            db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date = '%s'"%(table[product],product,date))
            rows = db_cursor.fetchall()
            if float(rows[0]['dividend']) > 0.0 or float(rows[0]['capital_gain']) > 0.0:
                dividend_adjust_products.add(product)
        except Exception, err:
            print traceback.format_exc()
            print('EXCEPTION in add_fund_quote in error corection with dividend: %s'%record)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_fund_quote in error corection with dividend: %s'%record) 
    try:
        db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date < '%s' ORDER BY date DESC LIMIT 1"%(table[product],product,date))
        rows = db_cursor.fetchall()
        if len(rows) < 1:
            current_dividend_factor = 1.0
        else:
            current_dividend_factor = float(rows[0]['forward_adjusted_close'])/float(rows[0]['close'])
        forward_adjusted_close = close*current_dividend_factor

        if error_correction:
            db_cursor.execute("SELECT * FROM %s WHERE product='%s' AND date > '%s' ORDER BY date LIMIT 1"%(table[product],product,date))
            rows = db_cursor.fetchall()

            if len(rows) < 1:
                current_dividend_factor = 1.0
            else:
                current_dividend_factor = float(rows[0]['backward_adjusted_close'])/float(rows[0]['close'])
            backward_adjusted_close = close*current_dividend_factor
            query = "UPDATE %s SET close='%f', asking_price='%f', backward_adjusted_close='%f', forward_adjusted_close='%f' WHERE date='%s' and product='%s'" % \
                    (table[product], close, asking_price, backward_adjusted_close, forward_adjusted_close, date, product)
        else:
            query = "INSERT INTO %s ( date, product, close, asking_price, backward_adjusted_close, forward_adjusted_close, dividend, capital_gain ) VALUES('%s','%s','%f','%f','%f','%f','0.0','0.0')" % ( table[product], date, product, close, asking_price, close, forward_adjusted_close)

        print query
        db_cursor.execute(query)
        db.commit()
    except Exception, err:
        print traceback.format_exc()
        db.rollback()
        print('EXCEPTION in add_fund_quote %s'%record)
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_fund_quote %s'%record)

def add_forex_quote(date, forex_tuple , record, error_correction):
    open1, high, low, close = FloatOrZero(record[4]), FloatOrZero(record[6]), FloatOrZero(record[7]), FloatOrZero(record[8])
    product = forex_tuple[0]
    if forex_tuple[1]: # If quotes need to be inverted
        _open1 = 1.0/open1
        _high = 1.0/low
        _low = 1.0/high
        _close = 1.0/close
    else:
        _open1, _high, _low, _close = open1, high, low, close
    try:
        if error_correction:
            query = "UPDATE %s SET open='%f', high='%f', low='%f', close='%f' WHERE date='%s' AND product='%s'" % (table[product], _open1, _high, _low, _close, date, product)
        else:
            query = "INSERT INTO %s ( date, product, open, high, low, close ) VALUES('%s','%s','%f', '%f', '%f', '%f')" % (table[product], date, product, _open1, _high, _low, _close)
        print query
        db_cursor.execute(query)
        db.commit()
    except Exception, err:
        print traceback.format_exc()
        db.rollback()
        print('EXCEPTION in add_forex_quote %s'%record)
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_forex_quote %s'%record)

def add_future_quote(date, record, future_someday_total_volume, future_someday_total_oi, future_volume_date, future_oi_date, error_correction):
    try:
        #print record
        product, csi_num, YYMM, open1, high, low, close, future_someday_volume, future_someday_oi = record[1], IntOrZero(record[2]), record[3], FloatOrZero(record[4]), FloatOrZero(record[6]), FloatOrZero(record[7]), FloatOrZero(record[8]), IntOrZero(record[10]), IntOrZero(record[11])   
        if product in mappings.keys():
            _base_symbol = mappings[product]
        else:
            _base_symbol = product
        
        #print str(_last_trading_date),date,_base_symbol
        contract_number = get_contract_number(datetime.strptime(date, '%Y-%m-%d').date(), _base_symbol, YYMM)
        #print contract_number
        specific_ticker = _base_symbol + get_exchange_specific( YYMM )
        generic_ticker = _base_symbol + '_' + str( contract_number )
        # get dict for VX and number of contracts
        if contract_number in futures_contract_list.get(_base_symbol,[1,2]): 
            try:
                if error_correction:
                    if (future_someday_volume > 0 and future_someday_oi > 0): 
                        query = "UPDATE %s SET open='%f', high='%f', low='%f', close='%f', contract_volume='%d', contract_oi='%d' WHERE date='%s' AND specific_ticker='%s'" % \
                        (table[generic_ticker], open1, high, low, close, future_someday_volume, future_someday_oi, date, specific_ticker)
                    else:
                        query = "UPDATE %s SET open='%f', high='%f', low='%f', close='%f' WHERE date='%s' AND specific_ticker='%s'" % (table[generic_ticker], open1, high, low, close, date, specific_ticker)
                else:
                    query = "INSERT INTO %s ( date, product, specific_ticker, open, high, low, close, is_last_trading_day, contract_volume, contract_oi, total_volume, total_oi ) VALUES('%s','%s','%s','%f','%f','%f','%f','0.0','0','0','0','0')" % ( table[generic_ticker], date, generic_ticker, specific_ticker, open1, high, low, close)
                print query
                db_cursor.execute(query)
                db.commit()
            except Exception, err:
                print traceback.format_exc()
                db.rollback()
                print('EXCEPTION in add_future_quote block 3 %s %s'% (record, date))
                server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_future_quote block 3 %s %s'% (record, date))
        else:
            print "Contract not being traded %s %d"%(_base_symbol, contract_number)   
        if contract_number in [0]+futures_contract_list.get(_base_symbol,[1,2]):
            try:
                if not error_correction:
                    query = "UPDATE %s SET contract_volume='%d',total_volume='%d' WHERE date='%s' AND specific_ticker='%s'" % (table[_base_symbol+'_1'], future_someday_volume, future_someday_total_volume, future_volume_date, specific_ticker)
                    print query
                    db_cursor.execute(query)
                    db.commit()    
                    query = "UPDATE %s SET contract_oi='%d',total_oi='%d' WHERE date='%s' AND specific_ticker='%s'" % (table[_base_symbol+'_1'], future_someday_oi, future_someday_total_oi, future_oi_date, specific_ticker)
                    print query
                    db_cursor.execute(query)
                    db.commit()
            except Exception, err:
                print traceback.format_exc()
                db.rollback()
                server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_future_quote block 2 %s %s'% (record, date))
                print('EXCEPTION in add_future_quote block 2 %s %s'% (record, date))
    except Exception, err:
        print traceback.format_exc()
        print('EXCEPTION in add_future_quote block 1  %s %s'% (record, date))
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in add_future_quote block 1 %s %s'% (record, date))

# ASSUMPTION: dividend quote will be sequenced after price quote
def dividend_quote(date, record):
    if len(record) > 5: # ASSUME CAPITAL GAIN ONLY IF RECORD LEN > 5
        product, csi_num, ex_date, dividend, capital_gain = record[1], IntOrZero(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'),FloatOrZero(record[4]), FloatOrZero(record[5])
    else:
        product, csi_num, ex_date, dividend, capital_gain = record[1], IntOrZero(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'), FloatOrZero(record[4]),0.0
    try:
        if table[product] == 'funds':
            query = "UPDATE %s SET dividend='%f',capital_gain='%f' WHERE product='%s' AND date='%s'" % ( table[product], dividend, capital_gain, product, ex_date)
            print query
            db_cursor.execute(query)           
        else:
            query = "UPDATE %s SET dividend='%f' WHERE product='%s' AND date='%s'" % ( table[product], dividend,  product, ex_date)
            print query
            db_cursor.execute(query)
        db.commit()
    except Exception, err:
        print traceback.format_exc()
        db.rollback()
        print('EXCEPTION in dividend_quote %s'%record)
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in dividend_quote %s'%record)
    dividend_adjust_products.add(product)
    # Peform dividend adjust on entire dataset just to be sure we are doing this corectly 
    # because CSI might change date of dividend distribution during error correction
    #dividend_adjust(date, product, record)

def dividend_adjust(product):
    try:
        if table[product] == 'funds':
            query = "UPDATE funds SET backward_adjusted_close=close, forward_adjusted_close=close WHERE product='%s'"%(product)
            print query
            db_cursor.execute(query)
            db.commit()
            query = "SELECT * FROM funds WHERE product='%s' AND (dividend > 0 OR capital_gain > 0) ORDER BY date"%(product)
            print query
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            for row in rows:
                ex_date = row['date']
                ex_close = float(row['close'])
                dividend_factor = 1 + (float(row['dividend'])+float(row['capital_gain']))/ex_close
                query = "UPDATE %s SET backward_adjusted_close = backward_adjusted_close/%f WHERE product='%s' AND date < '%s'" % ( table[product], dividend_factor, product, ex_date)
                print query
                db_cursor.execute(query)
                db.commit()
                query = "UPDATE %s SET forward_adjusted_close = forward_adjusted_close*%f WHERE product='%s' AND date >= '%s'" % ( table[product], dividend_factor, product, ex_date)
                print query
                db_cursor.execute(query)
                db.commit()
        elif table[product] == 'etfs':
            query = "UPDATE etfs SET backward_adjusted_close=close, forward_adjusted_close=close, backward_adjusted_open=open, forward_adjusted_open=open WHERE product='%s'"%(product)
            print query
            db_cursor.execute(query)
            db.commit()
            query = "SELECT * FROM etfs WHERE product='%s' AND dividend > 0 ORDER BY date" %(product)
            print query
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            #print rows  
            for row in rows:
                ex_date = row['date']
                ex_close = float(row['close'])
                dividend_factor = 1 + float(row['dividend'])/ex_close
                query = "UPDATE %s SET backward_adjusted_close = backward_adjusted_close/%f,backward_adjusted_open = backward_adjusted_open/%f WHERE product='%s' AND date < '%s'" % ( table[product], dividend_factor, dividend_factor, product, ex_date)
                print query
                db_cursor.execute(query)
                db.commit()
                query = "UPDATE %s SET forward_adjusted_close = forward_adjusted_close*%f,forward_adjusted_open = forward_adjusted_open*%f WHERE product='%s' AND date >= '%s'" % ( table[product], dividend_factor, dividend_factor, product, ex_date)
                print query
                db_cursor.execute(query)
                db.commit()
    except Exception, err:
        print traceback.format_exc()
        db.rollback()
        print('EXCEPTION in dividend adjust %s')
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in dividend adjust %s')


def split_quote(date, record):
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
    except Exception, err:
        print traceback.format_exc()
        db.rollback()
        print('EXCEPTION in split_quote %s'%record)
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in split_quote %s'%record)

# def error_correction_quote(date,record):
#     print 'IN ERROR CORRECTION'
#     update_record(date,record)

def delete_quote(date, record):
    date_to_be_deleted, product, csi_num, delivery_YYMM, option_flag, strike_price = datetime.strptime(record[1], '%Y%m%d').strftime('%Y-%m-%d'),record[2], IntOrZero(record[3]), record[4], record[5], FloatOrZero(record[6])
    try:
        query = "DELETE FROM %s WHERE date='%s' AND product='%s'"
        db_cursor.execute(query)
        db.commit()
    except Exception, err:
        print traceback.format_exc()
        db.rollback()
        print('EXCEPTION in delete_quote %s'%record)
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in delete_quote %s'%record)

def daily_update(filename, products):
    f = open(filename)
    records = f.readlines()
    f.close()
    
    header = records[0].strip().split(',')      
    if header[0] == '00': #Header/Trailer
        portfolio_identifier = header[1]
        file_type = header[2]
        record_count = IntOrZero(header[3])
        date = datetime.strptime(header[4], '%Y%m%d').strftime('%Y-%m-%d')
        record_date = date
        day = header[5]
        volume_date = datetime.strptime(header[6], '%Y%m%d').strftime('%Y-%m-%d')
        oi_date = datetime.strptime(header[7], '%Y%m%d').strftime('%Y-%m-%d')
    
    error_correction = False
    
    for item in records[1:]:
        if error_correction:
            # Reset ecord date and date which might have changed due to error correction
            date = record_date
        
        record = item.strip().split(',')
        error_correction = False        
        if record[0]=='09': # Correct error
            if record[3] in products:
                date = datetime.strptime(record[1], '%Y%m%d').strftime('%Y-%m-%d')
                record = record[2:]
                error_correction = True
                print "IN ERROR CORRECTION"

        if len(record) > 1:
            symbol = record[1]

        if record[0]=='01': # Future header
            future_symbol, future_csi_num, option_flag, future_total_volume,future_total_oi, future_total_est_volume = record[1], IntOrZero(record[2]), IntOrZero(record[3]), IntOrZero(record[4]), IntOrZero(record[5]), IntOrZero(record[6])
            if len(record) > 7:
                #print record
                try:
                    future_volume_date = datetime.strptime(record[7], '%Y%m%d').strftime('%Y-%m-%d')
                except Exception, err:
                    print traceback.format_exc()
                    future_volume_date = '1500-01-01'
            else:
                future_volume_date = volume_date
            if len(record) > 8:
                try:
                    future_oi_date = datetime.strptime(record[8], '%Y%m%d').strftime('%Y-%m-%d')
                except Exception, err:
                    print traceback.format_exc()
                    future_oi_date = '1500-01-01'
            else:
                future_oi_date = oi_date

        elif record[0]=='02': # Future price record
            if len(record) > 3:
                num = str(int(record[3])%100)
                _candidate_forex_identifier = symbol + '_' + num
                if _candidate_forex_identifier in forex_mappings.keys() and forex_mappings[_candidate_forex_identifier][0] in products:
                    add_forex_quote(date, forex_mappings[_candidate_forex_identifier], record, error_correction)
                else:
                    if len(products)==0 or symbol in products:
                        if error_correction:
                            add_future_quote(date, record, 0, 0, 0, 0, error_correction)
                        else:
                            add_future_quote(date, record, future_total_volume, future_total_oi, future_volume_date, future_oi_date, error_correction)

            else:
                if len(products)==0 or symbol in products:
                    if error_correction:
                        add_future_quote(date, record, 0, 0, 0, 0, error_correction)
                    else:
                        add_future_quote(date, record, future_total_volume, future_total_oi, future_volume_date, future_oi_date, error_correction)

        elif record[0]=='03': #Stock Price record
            if len(products)==0 or symbol in products:
                add_stock_quote(date, record, error_correction)
            
        elif record[0]=='06': #Mutual fund record
            if len(products)==0 or symbol in products:
                add_fund_quote(date, record, error_correction)

        elif record[0]=='07': #Dividen/Capital Gain record
            if len(products)==0 or symbol in products:
                dividend_quote(date, record)

        elif record[0]=='08': #Stock Split record
            if len(products)==0 or symbol in products:
                split_quote(date, record)

        elif record[0]=='11': #Fact sheet modifications
            continue

        elif record[0]=='13': #Delete past day of data
            if len(products)==0 or symbol in products:
                delete_quote(date, record)

        elif record[0]=='15': #Fact table entry
            continue

        elif record[0]=='32': # Future price record
            if len(record) > 3:
                num = str(int(record[3])%100)
                _candidate_forex_identifier = symbol + '_' + num
                if _candidate_forex_identifier in forex_mappings.keys() and forex_mappings[_candidate_forex_identifier][0] in products:
                    add_forex_quote(date, forex_mappings[_candidate_forex_identifier], record, error_correction)
                else:
                    if len(products)==0 or symbol in products:
                        if error_correction:
                            add_future_quote(date, record, 0, 0, 0, 0, error_correction)
                        else:
                            add_future_quote(date, record, future_total_volume, future_total_oi, future_volume_date, future_oi_date, error_correction)
            else:
                if len(products)==0 or symbol in products:
                    if error_correction:
                        add_future_quote(date, record, 0, 0, 0, 0, error_correction)
                    else:
                        add_future_quote(date, record, future_total_volume, future_total_oi, future_volume_date, future_oi_date, error_correction)

        elif record[0]=='33': #Type '03' with prices in decimal
            if len(products)==0 or symbol in products:
                add_stock_quote(date, record, error_correction)
        
        elif record[0]=='36': #Type '06' with prices in decimal
            if len(products)==0 or symbol in products:
                add_fund_quote(date, record, error_correction)

        elif record[0]=='37': #Type '07' with prices in decimal
            if len(products)==0 or symbol in products:
                dividend_quote(date, record)

def update_last_trading_day(k):
    _date = date.today() + timedelta(days=-k)
    for product in mappings.keys():
        _base_symbol = mappings[product]
        min_last_trading_date = datetime(2050,12,31).date()
        _last_trading_date = exchange_symbol_manager.get_last_trading_date(_date, _base_symbol + '_1')
        if _last_trading_date != _date:
            continue
        _contract_numbers = futures_contract_list.get(_base_symbol,[1,2]) # TODO should have mapping for this
        try:
            #query = "SELECT date, count(*) AS c FROM %s WHERE product LIKE '%s\_%%' GROUP BY date HAVING c=%d ORDER BY date DESC LIMIT 1"%(table[_base_symbol+'_1'], _base_symbol, len(_contract_numbers))
            query = "SELECT a.date, count(*) as count from %s as a, (SELECT date from %s where product LIKE '%s\_%%' ORDER BY date DESC LIMIT 1) as b WHERE a.date = b.date AND a.product LIKE '%s\_%%';"%\
                    (table[_base_symbol+'_1'],table[_base_symbol+'_1'], _base_symbol, _base_symbol) 
            print query
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            if rows[0]['count'] < len(_contract_numbers):
                print "EXCEPTION in update_last_trading_day : rows < nontract_numbers"
                server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in update_last_trading_day : rows < nontract_numbers')
                continue
            else:
                min_last_trading_date = rows[0]['date']
                delta = _date - min_last_trading_date
                if delta.days >= 7:
                    print 'Seems to be a problem in update_last_trading_day %s %s'%(_base_symbol, _date) #ADD MAIL
                    server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'Seems to be a problem in update_last_trading_day %s'%(generic_ticker))
                    continue
        except Exception, err:
            print traceback.format_exc()
            print 'EXCEPTION in update_last_trading_day %s on %s'%(_base_symbol, _date)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in update_last_trading_day %s'%generic_ticker)
            continue
        try:
            query = "UPDATE %s SET is_last_trading_day=1.0 WHERE product like '%s\_%%' AND date='%s'"%(table[_base_symbol+'_1'], _base_symbol, min_last_trading_date)
            print query
            db_cursor.execute(query)
            db.commit()
        except Exception, err:
            print traceback.format_exc()
            print "EXCEPTION in update_last_trading_day Tried to update last trading day after finding date but couldn't"
            db.rollback()
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in update_last_trading_day %s'%generic_ticker)

def __main__() :
    if len( sys.argv ) > 1:
        file_type = sys.argv[1]
        delay = int(sys.argv[2]) # Difference between today and file's date
        products = []
        for i in range(3,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python daily_update.py file:canada/f-indices/funds/futures/indices/uk-stocks/us-stocks delay product1 product2 ... productn'
        sys.exit(0)
    global exchange_symbol_manager
    module = imp.load_source('exchange_symbol_manager', '../exchange_symbol_manager.py')
    exchange_symbol_manager = module.ExchangeSymbolManager()
    filename = get_file(file_type, delay)
    print filename
    global server
    server = smtplib.SMTP("localhost")
    #sys.exit()
    db_connect()
    product_to_table_map()
    #print table, product_type
    if filename is not None:
        daily_update(filename, products)
    if file_type == 'futures':
        update_last_trading_day(delay)
    elif not (file_type == 'indices' or file_type=='f-indices'):
        for product in dividend_adjust_products:
            dividend_adjust(product)
    server.quit()

if __name__ == '__main__':
    __main__()
