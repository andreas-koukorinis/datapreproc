import sys
import datetime
import MySQLdb

#Connect to the database and return the db cursor if the connection is successful
def db_connect():
    try:
        db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity")
        return db.cursor()
    except MySQLdb.Error:
        sys.exit("Error In DB Connection")

#Return a datetime object  given a date
#This function is used to get timestamp for end of day events
#ASSUMPTION : All end of day events for a particular date occur at the same time i.e. HH:MM:SS:MSMS -> 23:59:59:999999
def getdtfromdate(date):
    date = date.strip().split('-')
    return datetime.datetime(int(date[0]),int(date[1]),int(date[2]),23, 59, 59, 999999)
