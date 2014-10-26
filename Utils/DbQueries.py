import sys
import datetime
import MySQLdb

#Connect to the database and return the db cursor if the connection is successful
def db_connect():
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity")
        return (db,db.cursor())
    except MySQLdb.Error:
        sys.exit("Error In DB Connection")

def db_close(db):
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close()

#Check whether today is the settlement day for the futures 'product'
def check_settlement_day(db_cursor,product,date):
    query = "SELECT Spec from "+product.rstrip('0123456789').lstrip('f')+" WHERE Date >='"+date+"' ORDER BY Date LIMIT 2"
    db_cursor.execute(query)
    data = db_cursor.fetchall()
    return data[0][0]!='#NA' and data[1][0]!='#NA' and data[0][0]!=data[1][0]

#Fetch the conversion factor for each product from the database
def conv_factor(products):
    (db,db_cursor) = db_connect()
    tick_factor = {}
    currency = {}
    currency_factor = {}
    conv_factor = {}
    for product in products:
        symbol = product.rstrip('0123456789').lstrip('f')
        query = "SELECT factor,currency FROM tick_conversion WHERE symbol='"+symbol+"'"
        db_cursor.execute(query)
        data = db_cursor.fetchall()                                                                                      #should check if data exists or not
        (tick_factor_product,currency_product) = (float(data[0][0]),str(data[0][1]))
        query = "SELECT factor from currency_conversion WHERE currency='"+currency_product+"'"
        db_cursor.execute(query)
        currency_factor_product = float(db_cursor.fetchall()[0][0])                                                      #should check if data exists or not
        conv_factor[product] = tick_factor_product*currency_factor_product
    db_close(db)
    return conv_factor
