import sys
import datetime
import MySQLdb
import heapq

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

def push_all_events( heap, products, _startdate, _enddate ):
    products = [ product.lstrip('f') for product in products ]
    (db,db_cursor) = db_connect()
    _format_strings = ','.join(['%s'] * len(products))
    query = "SELECT * FROM products WHERE product IN (" +_format_strings + ")" 
    db_cursor.execute(query,tuple(products))
    rows = db_cursor.fetchall()
    tables = {}
    types = {}
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

#Fetch the conversion factor for each product from the database
def conv_factor( products ):
    products = [ product.lstrip('f') for product in products ]
    (db,db_cursor) = db_connect()
    conv_factor = {}
    _format_strings = ','.join(['%s'] * len(products))
    db_cursor.execute("SELECT * FROM products NATURAL JOIN currency_rates WHERE product IN (%s)" % _format_strings,tuple(products))
    rows = db_cursor.fetchall()
    db_close(db)
    for row in rows:
        if row['type'] == 'future':
            symbol = 'f' + row['product']
        else:
            symbol = row['product']
        conv_factor[symbol] = float(row['conversion_factor'])*float(row['rate'])
    return conv_factor
