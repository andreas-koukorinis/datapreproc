import Quandl
import MySQLdb
import smtplib
import traceback

global db, db_cursor, server

def db_connect():
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
    return fetch_tables

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
        query = ("INSERT INTO %s VALUES (%s, %s, " + format_strings + ")") % (tables[prod], fetch_date, prod) + tuple(field_values)
        print query
        try:
            db_cursor.execute(query)
            db.commit()
        except Exception, err:
        	print traceback.format_exc()
            db.rollback()
            print('EXCEPTION in inserting Quandl data for %s'%prod)
            server.sendmail("sanchit.gupta@tworoads.co.in", "sanchit.gupta@tworoads.co.in;debidatta.dwibedi@tworoads.co.in", 'EXCEPTION in inserting Quandl data for %s'%prod)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('fetch_date')
    parser.add_argument('-p', type=str, nargs='+', help='Products to be fetched from Quandl\nEg: -p BEL12M USA1M\n', default=None, dest='products')
    parser.add_argument('-d', type=str, help='Dataset at Quandl\nEg: -d YC', default='YC', dest='dataset')
    parser,add_argument('-f', type=str, nargs='+', help='Fields to be fetched from Quandl\nEg. -f Rate\n', default='Rate', dest='fields')
    args = parser.parse_args()

    # By default try to get daily updates for all products in Quandl
    if args.products == None:
    	products = ['ESP12M', 'ESP4TO5Y', 'ESP10Y', 'ESP6TO12M', 'ESP3M', 'ESP2TO3Y', 'ESP3Y', 'ESP6M', 'ESP1TO2Y', 'ESP15Y', 'ESP5Y', 'DEU4Y', \
    	            'DEU1Y', 'DEU6M', 'DEU7Y', 'DEU6Y', 'DEU9Y', 'DEU2Y', 'DEU5Y', 'DEU10Y', 'DEU3Y', 'DEU8Y', 'FRA1M', 'FRA3M', 'FRA6M', 'FRA9M', \
    	            'FRA2Y', 'FRA1Y', 'FRA5Y', 'FRA30Y', 'FRA10Y', 'GBR5Y', 'GBR10Y', 'GBR20Y', 'FIN5Y', 'FIN10Y', 'BEL3M', 'BEL1M', 'BEL1Y', \
    	            'BEL12M', 'BEL6Y', 'BEL6M', 'BEL4M', 'BEL3Y', 'BEL5Y', 'BEL2Y', 'BEL4Y', 'BEL2M', 'BEL8Y', 'BEL10Y', 'BEL7Y', 'BEL12Y', 'SGP3M', \
    	            'SGP5Y', 'SGP6M', 'SGP2Y', 'SGP30Y', 'SGP15Y', 'SGP20Y', 'SGP12M', 'SGP10Y', 'AN3M', 'AN3Y', 'AN7Y', 'AN1M', 'AN30Y', 'AN1Y', \
    	            'AN2Y', 'AN6M', 'AN5Y', 'AN10Y', 'ZAF3M', 'ZAF5TO10Y', 'ZAF3TO5Y', 'ZAF9M', 'ZAF6M', 'ZAF10Y', 'ZAF12M', 'RI3Y', 'RI6TO9M', \
    	            'RI2TO3M', 'RI5Y', 'RI1M', 'RI3TO6M', 'RI9TO12M', 'RI1TO2M', 'RI3TO5Y', 'HE3M', 'HE10Y', 'HE30Y', 'HE3Y', 'HE6M', 'HE12M', \
    	            'HE1M', 'HE4Y', 'HE7Y', 'HE2Y', 'HE5Y', 'HE20Y', 'HEON', 'HKG3Y', 'HKG5Y', 'HKG2Y', 'HKG10Y', 'GRC5Y', 'GRC3Y', 'GRC20Y', \
    	            'GRC10Y', 'GRC30Y', 'GRC15Y', 'ITA5Y', 'ITA3Y', 'ITA10Y', 'ITA30Y', 'JPN1Y', 'JPN3Y', 'JPN2Y', 'JPN5Y', 'JPN4Y', 'JPN6Y', \
    	            'JPN7Y', 'JPN10Y', 'JPN9Y', 'JPN8Y', 'JPN15Y', 'JPN20Y', 'JPN25Y', 'JPN30Y', 'JPN40Y', 'MEX6M', 'MEX3M', 'MEX20Y', 'MEX10Y', \
    	            'MEX1M', 'MEX1Y', 'MEX5Y', 'MEX3Y', 'MEX30Y', 'NOR3M', 'NOR3Y', 'NOR9M', 'NOR5Y', 'NOR12M', 'NOR6M', 'NOR10Y', 'NZL3M', 'NZL6M', \
    	            'NZL1Y', 'NZL1M', 'NZL10Y', 'NZL2Y', 'NZL5Y', 'ROU3Y', 'ROU5Y', 'ROU12M', 'ROU6M', 'ROU10Y', 'PAK12M', 'PAK3M', 'PAK6M', 'PAK3Y', \
    	            'PAK10Y', 'PAK5Y', 'SWE6M', 'SWE2Y', 'SWE10Y', 'SWE7Y', 'SWE5Y', 'SWE3M', 'SWE12M', 'SWE1M', 'USA2Y', 'USA6M', 'USA1M', 'USA3Y', \
    	            'USA1Y', 'USA7Y', 'USA30Y', 'USA3M', 'USA10Y', 'USA5Y', 'USA20Y', 'HN4Y', 'HN1Y', 'HN6Y', 'HN7Y', 'IDN3Y', 'IDN4Y', 'HN3Y', \
    	            'IDN1Y', 'HN5Y', 'IDN5Y', 'HN2Y', 'HN10Y', 'IDN6Y', 'IDN7Y', 'IDN2Y', 'IDN9Y', 'IDN8Y', 'IDN10Y', 'IDN30Y', 'IDN20Y', 'KOR3M', \
    	            'IDN15Y', 'KOR3Y', 'KOR6M', 'KOR1Y', 'KOR2Y', 'KOR5Y', 'KOR10Y', 'KOR20Y', 'MYS6M', 'MYS3M', 'MYS3Y', 'MYS1Y', 'MYS2Y', 'MYS8Y', \
    	            'MYS4Y', 'MYS5Y', 'MYS6Y', 'MYS7Y', 'MYS10Y', 'MYS9Y', 'MYS15Y', 'MYS20Y', 'PHL1M', 'PHL1Y', 'PHL3M', 'PHL6M', 'PHL5Y', 'PHL2Y', \
    	            'PHL10Y', 'PHL4Y', 'PHL20Y', 'PHL3Y', 'THA3M', 'THA1Y', 'PHL7Y', 'THA6M', 'PHL25Y', 'THA4Y', 'THA3Y', 'THA6Y', 'THA5Y', 'THA10Y', \
    	            'THA8Y', 'THA9Y', 'THA7Y', 'THA15Y', 'THA1M', 'VNM3Y', 'VNM1Y', 'VNM2Y', 'VNM5Y', 'VNM7Y', 'VNM10Y', 'VNM15Y', 'THA2Y', 'IND5Y', \
    	            'IND6M', 'IND2Y', 'IND10Y', 'AUS2Y', 'AUS5Y', 'AUS3Y']
    else:
    	products = args.products
    server = smtplib.SMTP("localhost") 
    db_connect()
    tables = fetch_tables(products)
    push_quandl_updates(products, args.fetch_date, args.dataset, args.fields, tables)
    db_close()

if __name__ == '__main__':
    main()
