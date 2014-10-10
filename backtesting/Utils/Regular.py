import sys
import datetime

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

