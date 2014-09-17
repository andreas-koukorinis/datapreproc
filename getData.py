#!/usr/bin/python
import MySQLdb
import sys
from numpy import *

#Returns 2 arrays of (Dates,Prices) corresponding to symbol 'product' between dates 'startdate' and 'enddate'
#Assumption: The price table's name is assumed to be the product symbol with the rightmost digits removed (ES for ES1,ES2) 
def getPrice(product,startdate,enddate):
    sym = product.rstrip('1234567890')
    try:
        db = MySQLdb.connect(host="fixed-income.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity") 
        cur = db.cursor() 
        query = "SELECT Date,"+product+" FROM "+sym+" WHERE Date >= '"+startdate+"' AND Date <= '"+enddate+"'"
        cur.execute(query)
        alldata = cur.fetchall()
        #printCursorContent(cur)
        return ([(i[0]) for i in alldata],array([(i[1]) for i in alldata]).astype(float))
    except MySQLdb.Error:
        print "ERROR IN DB CONNECTION"
        return False

#Returns an n*1 array of (Specific Symbols) corresponding to symbol 'product' between dates 'startdate' and 'enddate'
#Assumption: The price table's name is assumed to be the product symbol with the rightmost digits removed (ES for ES1,ES2) 
def getSpec(product,startdate,enddate):
    sym = product.rstrip('1234567890')
    try:
        db = MySQLdb.connect(host="fixed-income.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity") 
        cur = db.cursor() 
        query = "SELECT Spec FROM "+sym+" WHERE Date >= '"+startdate+"' AND Date <= '"+enddate+"'"
        cur.execute(query)
        #printCursorContent(cur)
        return ([(i[0]) for i in cur.fetchall()])
    except MySQLdb.Error:
        print "ERROR IN DB CONNECTION"
        return False    

#Prints the dates and prices corresponding to the 'cur' database object
def printCursorContent(cur):
    for row in cur.fetchall() :
        st = ""
        for i in xrange(0,len(row)):
            st = st + str(row[i]) + " "
        print(st)





#(d,p) = getPrice('TY1','20140901','20140905')
#getPrice(sys.argv[1],sys.argv[2],sys.argv[3])
#print getSpec('TY1','20140901','20140905')
