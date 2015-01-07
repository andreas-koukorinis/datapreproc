import sys
import datetime
import MySQLdb
import heapq
import numpy as np
from regular import is_future,get_base_symbol

#Connect to the database and return the db cursor if the connection is successful
def db_connect():
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="daily_qplum")
        return (db,db.cursor(MySQLdb.cursors.DictCursor))
    except MySQLdb.Error:
        sys.exit("Error In DB Connection")

def db_close(db):
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close()    

def get_last_trading_dates( products, _startdate, _enddate ):
    products = [ product.lstrip('f') for product in products if is_future(product)] # Consider only the futures
    (db,db_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    query = "SELECT * FROM products WHERE product IN (" +_format_strings + ")" 
    db_cursor.execute(query,tuple(products))
    rows = db_cursor.fetchall()
    tables = {}
    types = {}
    _last_trading_days = {}
    _basenames = list( set ([ 'f'+get_base_symbol(product) for product in products ]) )
    for _basename in _basenames:
        _last_trading_days[_basename] = []
    for row in rows:
        tables[row['table']] = []
        types[row['product']] = row['type']
    for row in rows:
        tables[row['table']].append(row['product'])
    for table in tables.keys():
        _format_strings = ','.join(['%s'] * len(tables[table]))
        query = "SELECT * FROM %s WHERE product IN (%s) AND date >= '%s' AND date <= '%s' AND is_last_trading_day= '1.0'" % (table,_format_strings,_startdate, _enddate)
        db_cursor.execute(query ,tuple(tables[table]))
        rows = db_cursor.fetchall()
        for row in rows:
            product = row['product']
            _product_type = types[product]
            if _product_type == 'future': # Do not need to check
                _last_trading_days['f'+get_base_symbol(product)].append(row['date']) 
        for _basename in _basenames:
            _last_trading_days[_basename] = sorted( list( set( _last_trading_days[_basename] ) ) )    
    db_close(db)
    return _last_trading_days

def push_all_end_of_day_events( heap, products, _startdate, _enddate ):
    products = [ product.lstrip('f') for product in products ]
    (db,db_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    query = "SELECT * FROM products WHERE product IN (" +_format_strings + ")" 
    db_cursor.execute(query,tuple(products))
    rows = db_cursor.fetchall()
    tables = {}
    types = {}
    close_time = datetime.time(23,30,0,0) # For sequencing of events
    for row in rows:
        tables[row['table']] = []
        types[row['product']] = row['type']
    for row in rows:
        tables[row['table']].append(row['product'])
    for table in tables.keys():
        _format_strings = ','.join(['%s'] * len(tables[table]))
        query = "SELECT * FROM %s WHERE product IN (%s) AND date >= '%s' AND date <= '%s'" % (table,_format_strings,_startdate, _enddate)
        db_cursor.execute(query ,tuple(tables[table]))
        rows = db_cursor.fetchall()
        for row in rows:
            _dt = datetime.datetime.combine ( row['date'], datetime.datetime.max.time() )
            _product_type = types[row['product']]
            if _product_type == 'etf':
                _price = float(row['backward_adjusted_close'])
                _dividend = float(row['dividend']) 
                _event = {'product':row['product'],'price': _price, 'dividend': _dividend, 'type':'ENDOFDAY', 'dt':_dt,'product_type': types[row['product']], 'is_last_trading_day': False}
            elif _product_type == 'fund':
                _price = float(row['backward_adjusted_close'])
                #_price = float(row['close'])
                _dividend = float(row['dividend'])
                _capital_gain = float(row['capital_gain'])
                _event = {'product':row['product'],'price': _price, 'dividend': _dividend, 'capital_gain':_capital_gain, 'type':'ENDOFDAY', 'dt':_dt,'product_type': types[row['product']], 'is_last_trading_day': False}
            elif _product_type == 'future':
                _price = float(row['close'])
                if float(row['is_last_trading_day'])==0:
                    _is_last_trading_day = False
                else:
                    _is_last_trading_day = True
               
                _event = {'product': 'f' + row['product'],'price': _price, 'type':'ENDOFDAY', 'dt':_dt,'product_type': types[row['product']], 'is_last_trading_day': _is_last_trading_day}                
            heapq.heappush ( heap, ( _dt, _event ) ) 
    db_close(db)

def push_all_tax_payment_events(heap, start_date, end_date):
    y1 = start_date.year
    y2 = end_date.year
    tax_payment_time = datetime.time(23,31,0,0) # For sequencing of events
    for y in range(y1, y2):
        dt = datetime.datetime.combine(datetime.date(y,12,31), tax_payment_time)
        _event = {'dt': dt, 'type': 'TAXPAYMENTDAY'}
        heapq.heappush(heap, (dt, _event)) 
    dt = datetime.datetime.combine(end_date, tax_payment_time)
    _event = {'dt': dt, 'type': 'TAXPAYMENTDAY'}
    heapq.heappush(heap, (dt, _event))

def get_currency_and_conversion_factors(products, start_date, end_date):
    conv_factor = {}
    product_to_currency = {}
    currencies = []
    currency_factor = {}
    dummy_value = {}
    product_type = {}
    _is_usd_present = False     
    products = [ product.lstrip('f') for product in products ]
    (db,db_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    db_cursor.execute("SELECT product,currency,conversion_factor,type FROM products WHERE product IN (%s)" % _format_strings,tuple(products))
    rows = db_cursor.fetchall()
    for row in rows:
        product_type[row['product']] = row['type']
        if product_type[row['product']] == 'future':
            _symbol = 'f' + row['product']
        else:
            _symbol = row['product']
        conv_factor[_symbol] = float(row['conversion_factor'])
        if row['currency'] != 'USD':
            _currency = row['currency'] + 'USD'
            currencies.append(_currency)
            dummy_value[_currency] = 0.0
            product_to_currency[_symbol] = _currency
            currency_factor[_currency] = {}
        else:
            _is_usd_present = True
            product_to_currency[_symbol] = 'USD'
            currency_factor['USD'] = {}
    currencies = list(set(currencies))
    if len(currencies) > 0:
        _format_strings = ','.join(['%s'] * len(currencies))
        query = "SELECT date,product,close FROM forex WHERE product IN (%s) AND date >= '%s' AND date <= '%s' ORDER BY date" % (_format_strings, start_date, end_date)
        db_cursor.execute(query, tuple(currencies))
        rows = db_cursor.fetchall()
        for row in rows:
            if dummy_value[row['product']] == 0.0:
                dummy_value[row['product']] = float(row['close'])
            currency_factor[row['product']][row['date']] = float(row['close'])
    if _is_usd_present:
        currencies.append('USD')
    _date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    delta = datetime.timedelta(days=1)
    while _date <= end_date:
        for _currency in currencies:
            if _currency == 'USD':
                currency_factor[_currency][_date] = 1.0
            else:
                _currency_val = currency_factor[_currency].get(_date, dummy_value[_currency])
                currency_factor[_currency][_date] = _currency_val
                dummy_value[_currency] = _currency_val
        _date += delta
    return conv_factor, currency_factor, product_to_currency

def fetch_prices(product, _startdate, _enddate):
    product = product.lstrip('f')
    (db,db_cursor) = db_connect()
    query = "SELECT * FROM products WHERE product = '%s'" % product
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    table = rows[0]['table']
    product_type = rows[0]['type']
    query = "SELECT * FROM %s WHERE product='%s' AND date >= '%s' AND date <= '%s'" % (table, product, _startdate, _enddate)
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    dates = []
    prices = []
    for row in rows:
        if product_type == 'etf' or product_type == 'fund':
            price = float(row['backward_adjusted_close'])
        elif product_type == 'future':
            price = float(row['close'])
        dates.append(row['date'])
        prices.append(price)
    return np.array(dates), np.array(prices)

#conv_factor, currency_factor = get_currency_conversion_factors(['ES_1','FGBL_1','SXF_1'], '2014-01-01', '2014-01-30')
#for key in sorted(currency_factor['EURUSD'].keys()):
#    print key,currency_factor['EURUSD'][key]
