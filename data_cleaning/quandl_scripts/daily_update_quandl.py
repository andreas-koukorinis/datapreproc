import argparse
import smtplib
import sys
import traceback
import Quandl
import MySQLdb

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

def push_quandl_updates(products, fetch_date, dataset, fields, tables):
    try:
        with open('/spare/local/credentials/quandl_credentials.txt') as f:
            authorization_token = f.readlines()[0].strip()
    except IOError:
        sys.exit('No Quandl credentials file found')
    for prod in products:
        quandl_product_code = dataset + '/' + prod
        try:
            df = Quandl.get(quandl_product_code, authtoken=authorization_token, trim_start=fetch_date, trim_end=fetch_date)
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
        field_values = []
        for field in fields:
            field_values.append(df.iloc[0][field])

        format_strings = ','.join(['%s'] * len(fields))
        query = "INSERT INTO %s VALUES ('%s','%s', " + format_strings + ")"
        query = query % ((tables[prod], fetch_date, prod) + tuple(field_values))
        print query
        try:
            db_cursor.execute(query)
            db.commit()
        except Exception, err:
            print traceback.format_exc()
            db.rollback()
            print('EXCEPTION in inserting Quandl data for %s'%prod)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in inserting Quandl data for %s'%prod)

def daily_update_quandl(cmd=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('fetch_date')
    parser.add_argument('-p', type=str, nargs='+', help='Products to be fetched from Quandl\nEg: -p BEL12M USA1M\n', default=None, dest='products')
    parser.add_argument('-d', type=str, help='Dataset at Quandl\nEg: -d YC', default='YC', dest='dataset')
    parser.add_argument('-f', type=str, nargs='+', help='Fields to be fetched from Quandl\nEg. -f Rate\n', default=['Rate'], dest='fields')
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
    server = smtplib.SMTP("localhost") 
    db_connect()
    tables = fetch_tables(products)
    push_quandl_updates(products, args.fetch_date, args.dataset, args.fields, tables)
    db_close()

if __name__ == '__main__':
    daily_update_quandl()
