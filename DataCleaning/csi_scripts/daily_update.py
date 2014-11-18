#!/usr/bin/env python

import sys
import os
import gzip
import MySQLdb
import subprocess
import pandas as pd
from datetime import datetime,timedelta

table = {}
product_type = {}
db_cursor = None
db = None

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

def get_file( filename,k ):
    filename = filename +'.' + (datetime.now() - timedelta(days=k)).strftime('%Y%m%d')
    print filename
    path = '/home/cvdev/stratdev/DataCleaning/'
    if not os.path.isfile(path+filename): #If the file is not present,download it
        _file = filename+'.gz'
        is_in_s3 = subprocess.Popen(['s3cmd', 'ls', 's3://cvquantdata/csi/rawdata/'+_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
        if len(is_in_s3) <= 0:
            sys.exit('File %s not in s3'%_file)
        subprocess.call(['s3cmd','get','s3://cvquantdata/csi/rawdata/'+_file]) 
        inF = gzip.open(_file, 'rb')
        outF = open(filename, 'wb')
        outF.write( inF.read() )
        inF.close()
        outF.close()
    return filename

def add_stock_quote(date,record):
    product, open, high, low, close, volume = record[1], float(record[3]), float(record[4]), float(record[5]), float(record[6]), int(record[8])*100
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
    product, csi_num, close, asking_price = record[1], int(record[2]), float(record[3]), float(record[4])    
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

# ASSUMPTION: dividend quote will be sequenced after price quote
def dividend_quote(date,record):
    if len(record) > 5: # ASSUME CAPITAL GAIN ONLY IF RECORD LEN > 5
        product, csi_num, ex_date, dividend, capital_gain = record[1], int(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'), float(record[4]), float(record[5])
    else:
        product, csi_num, ex_date, dividend, capital_gain = record[1], int(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'), float(record[4]),0.0
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
    product, csi_num, ex_date, new_shares, old_shares = record[1], int(record[2]), datetime.strptime(record[3], '%Y%m%d').strftime('%Y-%m-%d'), float(record[4]), float(record[5])
    try:
        split_factor = new_shares/old_shares
        if product_type[product] == 'fund':
            query = "UPDATE %s SET close=close/%f, asking_price=asking_price/%f, backward_adjusted_close=backward_adjusted_close/%f, forward_adjusted_close=forward_adjusted_close/%f, dividend=dividend/%f, capital_gain=capital_gain/%f WHERE product='%s' AND date < '%s'" % ( table[product], split_factor, split_factor, split_factor, split_factor, split_factor, split_factor, product, ex_date)
        else:
            query = "UPDATE %s SET open=open/%f, high=high/%f, low=low/%f, close=close/%f, backward_adjusted_close=backward_adjusted_close/%f, forward_adjusted_close=forward_adjusted_close/%f, volume=volume*%d, dividend=dividend/%f WHERE product='%s' AND date < '%s'" % ( table[product], split_factor, split_factor, split_factor, split_factor, split_factor, split_factor, int(split_factor), split_factor, product, ex_date)
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
    date_to_be_deleted, product, csi_num, delivery_YYMM, option_flag, strike_price = datetime.strptime(record[1], '%Y%m%d').strftime('%Y-%m-%d'),record[2], int(record[3]), record[4], record[5], float(record[6])
    try:
        query = "DELETE FROM %s WHERE date='%s' AND product='%s'"
        db_cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        sys.exit('EXCEPTION in delete_quote %s'%record)

def daily_update(filename,products,k):
    filename = get_file( filename ,k)
    db_connect()
    product_to_table_map()
    f = open(filename)
    records = f.readlines()
    f.close()      
    for item in records:
        record = item.strip().split(',')
        symbol = record[1]
        if record[0]=='00': #Header/Trailer
            portfolio_identifier = record[1]
            file_type = record[2]
            record_count = int(record[3])
            date = datetime.strptime(record[4], '%Y%m%d').strftime('%Y-%m-%d')
            day = record[5]
            volume_date = record[6]
            oi_date = record[7]

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

        elif record[0]=='33': #Type '03' with prices in decimal
            if len(products)==0 or symbol in products:
                add_stock_quote(date,record)
        
        elif record[0]=='36': #Type '06' with prices in decimal
            if len(products)==0 or symbol in products:
                add_fund_quote(date,record)

        elif record[0]=='37': #Type '07' with prices in decimal
            if len(products)==0 or symbol in products:
                dividend_quote(date, record)

def __main__() :
    if len( sys.argv ) > 1:
        filename = sys.argv[1]
        products = []
        for i in range(3,len(sys.argv)):
            products.append(sys.argv[i])
    else:
        print 'python daily_update.py file:canada/f-indices/funds/futures/indices/uk-stocks/us-stocks lookback product1 product2 ... productn'
        sys.exit(0)
    daily_update( filename, products ,int(sys.argv[2]))

if __name__ == '__main__':
    __main__();
