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

#Return a datetime object  given a date
#This function is used to get timestamp for end of day events
#ASSUMPTION : All end of day events for a particular date occur at the same time i.e. HH:MM:SS:MSMS -> 23:59:59:999999
def getdtfromdate(date):
    date = date.strip().split('-')
    return datetime.datetime(int(date[0]),int(date[1]),int(date[2]),23, 59, 59, 999999)

#Check whether today is the settlement day for the futures 'product'
def check_settlement_day(db_cursor,product,date):
    query = "SELECT Spec from "+product.rstrip('0123456789').lstrip('f')+" WHERE Date >='"+date+"' ORDER BY Date LIMIT 2"
    db_cursor.execute(query)
    data = db_cursor.fetchall()
    return data[0][0]!='#NA' and data[1][0]!='#NA' and data[0][0]!=data[1][0]

#Check whether all events in the list are ENDOFDAY events
def checkEOD(events):
    ret = True
    for event in events:
        if(event['type']!='ENDOFDAY'): ret = False
    return ret

#Fetch the conversion factor for each product from the database
def conversion_factor(products):
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

#Getthe current worth of the portfolio based on the most recent daily closing prices
def get_worth(current_price,conversion_factor,current_portfolio):
    net_worth = current_portfolio['cash']
    num_shares = current_portfolio['num_shares']
    for product in current_price.keys():
        net_worth = net_worth + current_price[product]*conversion_factor[product]*num_shares[product]
    return net_worth

#Given the weights to assign to each product,calculate how many target number shares of the products we want (weight -ve implies short selling)
def get_positions_from_weights(weight,current_worth,current_price,conversion_factor):
    positions_to_take = {}
    for product in current_price.keys():
        money_allocated = weight[product]*current_worth
        positions_to_take[product] = money_allocated/(current_price[product]*conversion_factor[product])
    return positions_to_take

def print_portfolio_to_file(positions_file,portfolio,dt):
    text_file = open(positions_file, "a")
    text_file.write("Portfolio snapshot at EOD %s\nCash:%f\tPositions:%s\n\n" % (dt,portfolio['cash'],str(portfolio['num_shares'])))
    text_file.close()
