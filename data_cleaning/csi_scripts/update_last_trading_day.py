#!/usr/bin/env python
import traceback
import sys
import os
from sets import Set
import imp
import gzip
import smtplib
import MySQLdb
from datetime import datetime, timedelta, date

#from exchange_symbol_manager import ExchangeSymbolManager 

futures_contract_list = {'VX':[1,2,3,4,5,6,7]}
table = {}
product_type = {}
server = None
db_cursor = None
db = None
mappings = {'TU':'ZT','FV':'ZF','TY':'ZN','US':'ZB','NK':'NKD','NIY':'NIY','ES':'ES','EMD':'EMD','NQ':'NQ','YM':'YM','AD':'6A','BP':'6B','CD':'6C','CU1':'6E','JY':'6J','MP':'6M','NE2':'6N','SF':'6S','GC':'GC','SI':'SI','HG':'HG','PL':'PL','PA':'PA','LH':'LH','ZW':'ZW','ZC':'ZC','ZS':'ZS','ZM':'ZM','ZL':'ZL','EBS':'FGBS','EBM':'FGBM','EBL':'FGBL','SXE':'FESX','FDX':'FDAX','SMI':'FSMI','SXF':'SXF','CGB':'CGB','FFI':'LFZ','FLG': 'LFR','AEX': 'FTI','KC':'KC','CT':'CT','CC':'CC','SB':'SB','JTI':'TOPIX','JGB':'JGBL','JNI':'JNK','SIN':'SIN','SSG':'SG','HCE':'HHI','HSI':'HSI','ALS':'ALSI','YAP':'SPI','MFX':'MFX','KOS':'KOSPI','VX':'VX','SP':'SP'}

forex_mappings = { 'US$_46' : ('JPYUSD',True) ,'US$_39' : ('CADUSD',True), 'GB2_60' : ('GBPUSD',False), 'EU2_60' : ('EURUSD',False), 'US$_37': ('AUDUSD', True), 'US$_51': ('NZDUSD', True), 'US$_53': ('CHFUSD', True), 'US$_58': ('SEKUSD', True), 'US$_49': ('NOKUSD', True), 'FX1_39': ('TRYUSD', True), 'US$_56': ('MXNUSD', True), 'US$_57': ('ZARUSD', True), 'U2$_55': ('ILSUSD', True), 'US$_52': ('SGDUSD', True), 'US$_44': ('HKDUSD', True), 'U2$_53': ('TWDUSD', True), 'U2$_39': ('BRLUSD', True), 'U2$_45': ('INRUSD', True) }

exchange_symbol_mamager = None

def db_connect():
    global db,db_cursor
    try:
        with open('/spare/local/credentials/write_credentials.txt') as f:
            credentials = [line.strip().split(':') for line in f.readlines()]
    except IOError:
        sys.exit('No credentials file found')
    try:
        for user_id,password in credentials:
            db = MySQLdb.connect(host='fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com', user=user_id, passwd=password, db='daily_qplum')
            db_cursor = db.cursor(MySQLdb.cursors.DictCursor) 
    except MySQLdb.Error:
        sys.exit("Error in DB connection")

def product_to_table_map():
    global table,product_type
    query = 'SELECT * FROM products'
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    for row in rows:
        table[row['product']] = row['table']
        product_type[row['product']] = row['type']      

def setup_db_esm_smtp():
    global exchange_symbol_manager
    module = imp.load_source('exchange_symbol_manager', os.path.expanduser('~/datapreproc/data_cleaning/exchange_symbol_manager.py'))
    exchange_symbol_manager = module.ExchangeSymbolManager()
    global server
    server = smtplib.SMTP("localhost")
    db_connect()

def update_last_trading_day(given_date):
    setup_db_esm_smtp()
    product_to_table_map()
    _date = datetime.strptime(given_date, "%Y-%m-%d")
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
                    server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'Seems to be a problem in update_last_trading_day %s'%(_base_symbol))
                    continue
        except Exception, err:
            print traceback.format_exc()
            print 'EXCEPTION in update_last_trading_day %s on %s'%(_base_symbol, _date)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in update_last_trading_day %s'%_base_symbol)
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
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in update_last_trading_day %s'%_base_symbol)
    server.quit()

if __name__ == '__main__':
    update_last_trading_day(sys.argv[1])
