#!/usr/bin/python
import MySQLdb
import sys

#Returns an n*2 array of (Dates,Prices) corresponding to symbol 'product' between dates 'startdate' and 'enddate'
#The price table's name is assumed to be the product symbol with the rightmost digits removed (ES for ES1,ES2) 
def getPrice(product,startdate,enddate):
    sym = product.rstrip('1234567890')
    db = MySQLdb.connect(host="fixed-income.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity") 
    cur = db.cursor() 
    query = "SELECT Date,"+product+" FROM "+sym+" WHERE Date >= '"+startdate+"' AND Date <= '"+enddate+"'"
    cur.execute(query)
#    printPrices(cur)
    return cur.fetchall()

#prints the dates and prices corresponding to the 'cur' database object
def printPrices(cur):
    for row in cur.fetchall() :
        st = ""
        for i in xrange(0,len(row)):
            st = st + str(row[i]) + " "
        print(st)

#getPrice('TY1','20140901','20140905')
#getPrice(sys.argv[1],sys.argv[2],sys.argv[3])
