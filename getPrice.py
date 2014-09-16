#!/usr/bin/python
import MySQLdb
import sys

def getPrice(product,startdate,enddate):
    sym = product.rstrip('1234567890')
    db = MySQLdb.connect(host="fixed-income.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity") 
    cur = db.cursor() 
    query = "SELECT Date,"+product+" FROM "+sym+" WHERE Date >= '"+startdate+"' AND Date <= '"+enddate+"'"
    cur.execute(query)
    printPrices(cur)
    return cur.fetchall()

def printPrices(cur):
    for row in cur.fetchall() :
        st = ""
        for i in xrange(0,len(row)):
            st = st + str(row[i]) + " "
        print(st)

#getPrice('TY1','20140901','20140905')
getPrice(sys.argv[1],sys.argv[2],sys.argv[3])
