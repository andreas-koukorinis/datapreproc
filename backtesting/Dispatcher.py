import sys
import datetime
import heapq
import MySQLdb
#import protocol

class Dispatcher:
    
    def __init__(self,start_date,end_date,products):
        self.start_dt = self.getdtfromdate(start_date)
        self.end_dt = self.getdtfromdate(end_date)
        self.products = products
        self.heap = []
        self.db_cursor = self.db_connect()

    def run(self):
        self.heap_initialize(self.products)
        for item in self.heap:
            print item
        current_dt = heapq.nsmallest(1,self.heap)[0] 
        while(current_dt<=self.end_dt):
            while(heapq.nsmallest(1,self.heap)[0][0]==current_dt):
                tup = heapq.heappop(self.heap)
                event = tup[1]
                if(event['type']=='ENDOFDAY'):
                    pass					#call strategy and push next
                if(event['type']=='INTRADAY'):
                    pass                                        #call book builder and push next
        
    def heap_initialize(self,products):
        #Push DB EOD sources
        for product in products:
            (date,price) = self.fetchnextdb(product.rstrip('1234567890'),product,self.start_dt)
            dt = self.getdtfromdate(date)
            event = {'price': price, 'product':product, 'type':'ENDOFDAY', 'dt':dt, 'table':product.rstrip('1234567890')} 
            heapq.heappush(self.heap,(dt,event))

        #Push FLAT FILE sources
        #TO BE DONE

    def fetchnextdb(self,table,product,dt):
        try:
            query = "SELECT Date,"+product+" FROM "+table+" WHERE Date >= '"+str(dt.date())+"' ORDER BY Date LIMIT 1"
            self.db_cursor.execute(query)
            data = self.db_cursor.fetchall()                  #should check if data exists or not
            return (str(data[0][0]),float(data[0][1]))
        except:
            sys.exit("Error In DB.fetchnext")            

    def getdtfromdate(self,date):
        date = date.strip().split('-')
        return datetime.datetime(int(date[0]),int(date[1]),int(date[2]),23, 59, 59, 999999)

    def db_connect(self):
        try:
            db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="risk_parity")
            return db.cursor()
        except MySQLdb.Error:
            sys.exit("Error In DB Connection") 

#print getdtfromdate('2014-08-08') 
disp = Dispatcher('2014-08-08' , '2014-08-20' , ['ES1','TY1','ED1'])
disp.run()
