import sys
import datetime
import re

#Return a datetime object  given a date
#This function is used to get timestamp for end of day events
#ASSUMPTION : All end of day events for a particular date occur at the same time i.e. HH:MM:SS:MSMS -> 23:59:59:999999
def getdtfromdate(date):
    date = date.strip().split('-')
    return datetime.datetime(int(date[0]),int(date[1]),int(date[2]),23, 59, 59, 999999)

#Check whether all events in the list are ENDOFDAY events
def checkEOD(events):
    ret = True
    for event in events:
        if(event['type']!='ENDOFDAY'): ret = False
    return ret

# if there is fES1 ... make sure fES2 is also there, if not add it
def add_complementary_future_pair(products):
    add_products=[]
    for product in products:
        if(product[0]!='f'): continue #Only add pair for future contracts
        sym = product.rstrip('0123456789') #Get the underlying symbol EG: fES for fES1
        if(sym!=product):
            num = int(next(re.finditer(r'\d+$', product)).group(0)) #num is the number at the end of a symbol.EG:1 for fES1
            pair = sym + str(num+1) # pair : fES2 for fES1
            add_products.append(pair) #append to a separate list to avoid infinite loop
    return list(set(products) | set(add_products)) #Take union of two lists and return
            

    

