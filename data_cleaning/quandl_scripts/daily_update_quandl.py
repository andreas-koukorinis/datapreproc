import argparse
import datetime
import os
import smtplib
import sys
import traceback
import Quandl
import MySQLdb
home_path = os.path.expanduser("~")
sys.path.append(home_path + '/datapreproc/')
from data_cleaning.exchange_symbol_manager import ExchangeSymbolManager
exchange_symbol_manager = ExchangeSymbolManager()

def db_connect():
    global db, db_cursor
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

def db_close():
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close() 

def fetch_tables(products):
    format_strings = ','.join(['%s'] * len(products))
    query = "SELECT * FROM products WHERE product IN (" +format_strings + ")"
    db_cursor.execute(query,tuple(products))
    rows = db_cursor.fetchall()
    tables = {}
    for row in rows:
        tables[row['product']] = row['table']
    return tables

def get_quandl_product_code(product, fetch_date):
    # Might be useful to cross-check CSI, Quandl and our future codes
    # mappings = {'TU':'ZT','FV':'ZF','TY':'ZN','US':'ZB','NK':'NKD','NIY':'NIY','ES':'ES','EMD':'EMD','NQ':'NQ',\
    #             'YM':'YM','AD':'6A','BP':'6B','CD':'6C','CU1':'6E','JY':'6J','MP':'6M','NE2':'6N','SF':'6S',\
    #             'GC':'GC','SI':'SI','HG':'HG','PL':'PL','PA':'PA','LH':'LH','ZW':'ZW','ZC':'ZC','ZS':'ZS','ZM':'ZM',\
    #             'ZL':'ZL','EBS':'FGBS','EBM':'FGBM','EBL':'FGBL','SXE':'FESX','FDX':'FDAX','SMI':'FSMI','SXF':'SXF',\
    #             'CGB':'CGB','FFI':'LFZ','FLG': 'LFR','AEX': 'FTI','KC':'KC','CT':'CT','CC':'CC','SB':'SB','JTI':'TOPIX',\
    #             'JGB':'JGBL','JNI':'JNK','SIN':'SIN','SSG':'SG','HCE':'HHI','HSI':'HSI','ALS':'ALSI','YAP':'SPI',\
    #             'MFX':'MFX','KOS':'KOSPI','VX':'VX','SP':'SP'}
    
    # futures = {'TU':'CME/TU', 'FV':'CME/FV', 'TY':'CME/TY', 'US':'CME/US', 'NK':'CME/NK',\
    #            'NIY': 'CME/N1Y', 'ES':'CME/ES', 'SP':'CME/SP', 'EMD:CME/MD', 'NQ:CME/NQ', 'YM:CME/YM', \
    #            'AD': 'CME/AD', 'BP': 'CME/BP', 'CD':'CME/CD', 'CU1':'CME/EC', 'JY':'CME/JY', \
    #            'MP':'CME/MP', 'NE2':'CME/NE', 'SF':'CME/SF', 'GC':'CME/GC', 'SI':'CME/SI', 'HG':'CME/HG',\
    #            'PL':'CME/PL', 'PA':'CME/PA', 'LH':'CME/LN', 'ZW':'CME/W', 'ZC':'CME/C', 'ZS':'CME/S',\
    #            'ZM':'CME/SM', 'ZL':'CME/BO', 'EBS':'EUREX/FGBS', 'EBM':'EUREX/FGBM', 'EBL':'EUREX/FGBM',\
    #            'SXE':'EUREX/FESX', 'FDX':'EUREX/FDAX', 'SMI':'EUREX/FSMI', 'SXF':'MX/SXF', 'CGB':'MX/CGB',\
    #            'FFI':'LIFFE/Z', 'FLG':'LIFFE/R', 'AEX':'LIFFE/FTI', 'KC':'ICE/KC', 'CT':'ICE/CT', 'CC':'ICE/CC',
    #            'SB':'ICE/SB', 'JTI':None, 'JGB':None, 'JNI':None, 'SIN':'SGX/IN', 'SSG':'SGX/SG', 'HCE':None,\
    #            'HSI':None, 'ALS':None, 'YAP':'ASX/AP', 'MFX':None, 'KOS':None, 'VX':'CBOE/VX'}
    global exchange_symbol_manager
    map_product_to_quandl_code = {'EMD': 'CME/MD', 'HSI': None, 'YM': 'CME/YM', 'FGBS': 'EUREX/FGBS', 'ZT': 'CME/TU',\
                                  'FDAX': 'EUREX/FDAX', 'VX': 'CBOE/VX', 'SIN': 'SGX/IN', 'HG': 'CME/HG', 'SXF': 'MX/SXF',\
                                  'CGB': 'MX/CGB', 'LH': 'CME/LN', 'KOSPI': None, 'SPI': 'ASX/AP', '6S': 'CME/SF',\
                                  'ES': 'CME/ES', 'LFR': 'LIFFE/R', 'MFX': None, 'NQ': 'CME/NQ', 'ZS': 'CME/S', 'PL': 'CME/PL',\
                                  'LFZ': 'LIFFE/Z', 'ZL': 'CME/BO', 'ZM': 'CME/SM', 'ZN': 'CME/TY', '6E': 'CME/EC', 'CC': 'ICE/CC',
                                  'ZF': 'CME/FV', 'ALSI': None, 'ZB': 'CME/US', 'ZC': 'CME/C', 'TOPIX': None, 'JGBL': None,\
                                  'GC': 'CME/GC', 'FTI': 'LIFFE/FTI', 'ZW': 'CME/W', 'FESX': 'EUREX/FESX', 'CT': 'ICE/CT',\
                                  'KC': 'ICE/KC', '6A': 'CME/AD', '6B': 'CME/BP', '6C': 'CME/CD', 'NKD': 'CME/NK', 'PA': 'CME/PA',\
                                  'FSMI': 'EUREX/FSMI', '6J': 'CME/JY', 'SP': 'CME/SP', '6M': 'CME/MP', '6N': 'CME/NE',\
                                  'SI': 'CME/SI', 'NIY': 'CME/N1Y', 'FGBM': 'EUREX/FGBM', 'JNK': None, 'SB': 'ICE/SB',\
                                  'HHI': None, 'FGBL': 'EUREX/FGBM', 'SG': 'SGX/SG'}
    fetch_date = datetime.datetime.strptime(fetch_date, "%Y%m%d").date()
    specific_ticker = exchange_symbol_manager.get_exchange_symbol(fetch_date, product)
    month = specific_ticker[-3]
    return map_product_to_quandl_code[specific_ticker[:-3]] + month + '20' + specific_ticker[-2:] # Will only work till 2099    

def fetch_quandl_futures_prices(product, fetch_date):
    try:
        with open('/spare/local/credentials/quandl_credentials.txt') as f:
            authorization_token = f.readlines()[0].strip()
    except IOError:
        sys.exit('No Quandl credentials file found')
    
    quandl_product_code = get_quandl_product_code(product, fetch_date)
    try:
        df = Quandl.get(quandl_product_code, authtoken=authorization_token, trim_start=fetch_date, trim_end=fetch_date)
    except Quandl.Quandl.ErrorDownloading:
        print "Couldn't download Quandl data for %s"%product
        server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in downloading Quandl data for %s'%prod)
        return None
    except Exception, err:
        print traceback.format_exc()
        print "EXCEPTION in fetching Quandl data for %s"%product
        return None
    
    if len(df.index) == 0:
        print "No data to download today for %s"%product
        return None

    record = {}
    record[product] = product
    record['open'] = df.iloc[0][0]
    record['high'] = df.iloc[0][1]
    record['low'] = df.iloc[0][2]
    record['close'] = df.iloc[0][3]
    return record

def push_quandl_yield_rates(products, fetch_date):
    dataset = 'YC'
    tables = fetch_tables(products)
    try:
        with open('/spare/local/credentials/quandl_credentials.txt') as f:
            authorization_token = f.readlines()[0].strip()
    except IOError:
        sys.exit('No Quandl credentials file found')
    for prod in products:
        quandl_product_code = dataset + '/' + prod

        query = "SELECT max(date) as date FROM '%s' WHERE product='%s'"
        query = query % (tables[prod], prod)
        print query
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) > 0:
            latest_date = rows[0]['date']
            fetch_start_date = datetime.datetime.strptime(latest_date, '%Y-%m-%d').date() + datetime.timedelta(days=1)
            if fetch_date < fetch_start_date:
                continue
            try:
                df = Quandl.get(quandl_product_code, authtoken=authorization_token, trim_start=fetch_start_date, trim_end=fetch_date)
            except Quandl.Quandl.ErrorDownloading:
                print "Couldn't download Quandl data for %s"%prod
                server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in downloading Quandl data for %s'%prod)
                continue
            except Exception, err:
                print traceback.format_exc()
                print "EXCEPTION in fetching Quandl data for %s"%prod
                continue
            if len(df.index) == 0:
                print "No data to download today for %s"%prod
                continue
            dates = df.index.to_pydatetime()
            for i in xrange(len(df.index)):
                rate = df.iloc[i][0]
                query = "INSERT INTO %s VALUES ('%s','%s', '%s')"
                query = query % (tables[prod], dates[i], prod, rate)
                print query
                try:
                    db_cursor.execute(query)
                    db.commit()
                except Exception, err:
                    print traceback.format_exc()
                    db.rollback()
                    print('EXCEPTION in inserting Quandl data for %s'%prod)
                    server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in inserting Quandl data for %s'%prod)
 
        # Will be used when error correction policy from Quandl is known
        # query = "SELECT (1) FROM %s WHERE date='%s' AND product='%s' AND rate='%s' LIMIT 1"
        # query = query % (tables[prod], fetch_date, prod, rate)
        # print query
        # exact_match = db_cursor.execute(query)
        # # If record doesn't exist then only go ahead and insert record
        # if exact_match == 0:
        #     query = "SELECT (1) FROM %s WHERE date='%s' AND product='%s' LIMIT 1"
        #     query = query % (tables[prod], fetch_date, prod)
        #     print query
        #     # If record doesn't exist then only go ahead and insert record
        #     correction = db_cursor.execute(query)

        #     if correction:
        #         query = "UPDATE %s SET rate='%s' WHERE date='%s' AND product='%s'"
        #         query = query % (tables[prod],rate, fetch_date, prod)
        #         print query
        #         try:
        #             db_cursor.execute(query)
        #             db.commit()
        #         except Exception, err:
        #             print traceback.format_exc()
        #             db.rollback()
        #             print('EXCEPTION in updating Quandl data for %s in case of correction'%prod)
        #             server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in inserting Quandl data for %s'%prod)
        #     else:
        #         query = "INSERT INTO %s VALUES ('%s','%s', '%s')"
        #         query = query % (tables[prod], fetch_date, prod, rate)
        #         print query
        #         try:
        #             db_cursor.execute(query)
        #             db.commit()
        #         except Exception, err:
        #             print traceback.format_exc()
        #             db.rollback()
        #             print('EXCEPTION in inserting Quandl data for %s'%prod)
        #             server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in inserting Quandl data for %s'%prod)
            
def setup_db_smtp():
    global server
    server = smtplib.SMTP("localhost")
    db_connect()

def daily_update_quandl(cmd=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('fetch_date')
    parser.add_argument('-p', type=str, nargs='+', help='Products to be fetched from Quandl\nEg: -p BEL12M USA1M\n', default=None, dest='products')
    parser.add_argument('-t', type=str, help='Type of product\nEg: -t futures\n', default='yield_rates', dest='product_type')
    if cmd == None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(cmd.split())

    # By default try to get daily updates for all products in Quandl
    if args.products == None:
        products = ['AUS2Y', 'AUS3Y', 'AUS5Y', 'AUS10Y', 'BEL10Y', 'BEL12M', 'BEL12Y', 'BEL1M', 'BEL1Y', 'BEL2M', 'BEL2Y', 'BEL3M', 'BEL3Y', 'BEL4M', 'BEL4Y', \
                    'BEL5Y', 'BEL6M', 'BEL6Y', 'BEL7Y', 'BEL8Y', 'CAN10Y', 'CAN1M', 'CAN1Y', 'CAN2Y', 'CAN30Y', 'CAN3M', 'CAN3Y', 'CAN5Y', 'CAN6M', \
                    'CAN7Y', 'CHE10Y', 'CHE12M', 'CHE1M', 'CHE20Y', 'CHE2Y', 'CHE30Y', 'CHE3M', 'CHE3Y', 'CHE4Y', 'CHE5Y', 'CHE6M', 'CHE7Y', 'CHEON', \
                    'CHN10Y', 'CHN1Y', 'CHN2Y', 'CHN3Y', 'CHN4Y', 'CHN5Y', 'CHN6Y', 'CHN7Y', 'CRI1M', 'CRI1TO2M', 'CRI2TO3M', 'CRI3TO5Y', 'CRI3TO6M', \
                    'CRI3Y', 'CRI5Y', 'CRI6TO9M', 'CRI9TO12M', 'DEU10Y', 'DEU1Y', 'DEU2Y', 'DEU3Y', 'DEU4Y', 'DEU5Y', 'DEU6M', 'DEU6Y', 'DEU7Y', 'DEU8Y', \
                    'DEU9Y', 'ESP10Y', 'ESP12M', 'ESP15Y', 'ESP1TO2Y', 'ESP2TO3Y', 'ESP3M', 'ESP3Y', 'ESP4TO5Y', 'ESP5Y', 'ESP6M', 'ESP6TO12M', 'FIN10Y', \
                    'FIN5Y', 'FRA10Y', 'FRA1M', 'FRA1Y', 'FRA2Y', 'FRA30Y', 'FRA3M', 'FRA5Y', 'FRA6M', 'FRA9M', 'GBR10Y', 'GBR20Y', 'GBR5Y', 'GRC10Y', \
                    'GRC15Y', 'GRC20Y', 'GRC30Y', 'GRC3Y', 'GRC5Y', 'HKG10Y', 'HKG2Y', 'HKG3Y', 'HKG5Y', 'IDN10Y', 'IDN15Y', 'IDN1Y', 'IDN20Y', 'IDN2Y', \
                    'IDN30Y', 'IDN3Y', 'IDN4Y', 'IDN5Y', 'IDN6Y', 'IDN7Y', 'IDN8Y', 'IDN9Y', 'IND10Y', 'IND2Y', 'IND5Y', 'IND6M', 'ITA10Y', 'ITA30Y', \
                    'ITA3Y', 'ITA5Y', 'JPN10Y', 'JPN15Y', 'JPN1Y', 'JPN20Y', 'JPN25Y', 'JPN2Y', 'JPN30Y', 'JPN3Y', 'JPN40Y', 'JPN4Y', 'JPN5Y', 'JPN6Y', \
                    'JPN7Y', 'JPN8Y', 'JPN9Y', 'KOR10Y', 'KOR1Y', 'KOR20Y', 'KOR2Y', 'KOR3M', 'KOR3Y', 'KOR5Y', 'KOR6M', 'MEX10Y', 'MEX1M', 'MEX1Y', 'MEX20Y', \
                    'MEX30Y', 'MEX3M', 'MEX3Y', 'MEX5Y', 'MEX6M', 'MYS10Y', 'MYS15Y', 'MYS1Y', 'MYS20Y', 'MYS2Y', 'MYS3M', 'MYS3Y', 'MYS4Y', 'MYS5Y', 'MYS6M', \
                    'MYS6Y', 'MYS7Y', 'MYS8Y', 'MYS9Y', 'NOR10Y', 'NOR12M', 'NOR3M', 'NOR3Y', 'NOR5Y', 'NOR6M', 'NOR9M', 'NZL10Y', 'NZL1M', 'NZL1Y', 'NZL2Y', \
                    'NZL3M', 'NZL5Y', 'NZL6M', 'PAK10Y', 'PAK12M', 'PAK3M', 'PAK3Y', 'PAK5Y', 'PAK6M', 'PHL10Y', 'PHL1M', 'PHL1Y', 'PHL20Y', 'PHL25Y', 'PHL2Y', \
                    'PHL3M', 'PHL3Y', 'PHL4Y', 'PHL5Y', 'PHL6M', 'PHL7Y', 'ROU10Y', 'ROU12M', 'ROU3Y', 'ROU5Y', 'ROU6M', 'SGP10Y', 'SGP12M', 'SGP15Y', 'SGP20Y', \
                    'SGP2Y', 'SGP30Y', 'SGP3M', 'SGP5Y', 'SGP6M', 'SWE10Y', 'SWE12M', 'SWE1M', 'SWE2Y', 'SWE3M', 'SWE5Y', 'SWE6M', 'SWE7Y', 'THA10Y', 'THA15Y', \
                    'THA1M', 'THA1Y', 'THA2Y', 'THA3M', 'THA3Y', 'THA4Y', 'THA5Y', 'THA6M', 'THA6Y', 'THA7Y', 'THA8Y', 'THA9Y', 'USA10Y', 'USA1M', 'USA1Y', 'USA20Y', \
                    'USA2Y', 'USA30Y', 'USA3M', 'USA3Y', 'USA5Y', 'USA6M', 'USA7Y', 'VNM10Y', 'VNM15Y', 'VNM1Y', 'VNM2Y', 'VNM3Y', 'VNM5Y', 'VNM7Y', 'ZAF10Y', \
                    'ZAF12M', 'ZAF3M', 'ZAF3TO5Y', 'ZAF5TO10Y', 'ZAF6M', 'ZAF9M']
    else:
        products = args.products
    global server
    setup_db_smtp()
    if args.product_type == 'yield_rates':
        push_quandl_yield_rates(products, args.fetch_date)
    elif args.product_type == 'futures':
        push_quandl_futures_prices(products, args.fetch_date)
    db_close()

if __name__ == '__main__':
    daily_update_quandl()
