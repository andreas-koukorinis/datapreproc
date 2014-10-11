import MySQLdb
import sys
from numpy import *

#Returns 2 arrays of (Dates,Prices) corresponding to symbol 'product' between dates 'startdate' and 'enddate'
#Assumption: The price table's name is assumed to be the product symbol with the rightmost digits removed (ES for ES1,ES2)
def get_dated_price_data(product,startdate,enddate):
    sym = product.rstrip('1234567890')
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity")
        cur = db.cursor()
        query = "SELECT Date,"+product+" FROM "+sym+" WHERE Date >= '"+startdate+"' AND Date <= '"+enddate+"'"
        cur.execute(query)
        alldata = cur.fetchall()
        #print_cursor_content(cur)
        return ([(i[0]) for i in alldata],array([(i[1]) for i in alldata]).astype(float))
    except MySQLdb.Error:
        print "ERROR IN DB CONNECTION"
        return False

#Returns an n*1 array of (Specific Symbols) corresponding to symbol 'product' between dates 'startdate' and 'enddate'
#Assumption: The price table's name is assumed to be the product symbol with the rightmost digits removed (ES for ES1,ES2)
def get_spec(product,startdate,enddate):
    sym = product.rstrip('1234567890')
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity")
        cur = db.cursor()
        query = "SELECT Spec FROM "+sym+" WHERE Date >= '"+startdate+"' AND Date <= '"+enddate+"'"
        cur.execute(query)
        #print_cursor_content(cur)
        return ([(i[0]) for i in cur.fetchall()])
    except MySQLdb.Error:
        print "ERROR IN DB CONNECTION"
        return False

#Prints the dates and prices corresponding to the 'cur' database object
def print_cursor_content(cur):
    for row in cur.fetchall() :
        st = ""
        for i in xrange(0,len(row)):
            st = st + str(row[i]) + " "
        print(st)
