import sys
import pickle
from datetime import datetime
from dateutil.rrule import rrule, DAILY
from Regular import get_dt_from_date

def __main__():
    if len ( sys.argv ) < 4 :
        print "arguments <startdate enddate file_to_dump>"
        sys.exit(0)
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    start_dt = get_dt_from_date(start_date) 
    end_dt = get_dt_from_date(end_date)          
    dts = list(rrule(DAILY, dtstart=start_dt.date(), until=end_dt.date())) 
    dates = [dt.date() for dt in dts]
    ones = [1]*len(dates)
    dates_ones = zip(dates,ones)
    with open(sys.argv[3], 'w') as f:
        pickle.dump(dates_ones, f)

__main__()
