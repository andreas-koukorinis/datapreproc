#!/usr/bin/env python
import sys
import datetime
from calendar import monthrange

class ExchangeSymbolManager():
    def __init__( self ):
        self.month_codes = { '1' : 'F', '2' : 'G', '3' : 'H', '4' : 'J', '5' : 'K',  '6' : 'M', '7' : 'N', '8' : 'Q', '9' : 'U', '10' : 'V', '11' : 'X', '12' : 'Z' }
        self.day_codes = { 'SUNDAY': 6, 'MONDAY': 0, 'TUESDAY': 1, 'WEDNESDAY': 2, 'THURSDAY': 3, 'FRIDAY': 4, 'SATURDAY': 5 }

    def get_basename( self, product ):
        return product.rsplit('_',1)[0]

    def get_contract_number( self, product ):
        return int( product.rsplit('_',1)[1] )       

    def get_yy( self, yyyy ):
        yy = yyyy%100
        if yy < 10:
            return '0' + str(yy)
        else:
            return str(yy)

    def get_date_from_nth_day_of_month_year(self, n, day, month, year):
        n -= 1
        day = self.day_codes[day]
        begining_of_month_day, num_days_in_month = monthrange(year, month)
        firstmatch = (day - begining_of_month_day) % 7 + 1
        day = xrange(firstmatch, num_days_in_month + 1, 7)[n]
        return datetime.date(year=year,day=day,month=month)

    def get_exchange_for_product( self, product ):
        _basename = self.get_basename( product )
        if _basename in ['ZT','ZF','ZN','ZB','UB','NKD','NIY','GE','ES','NQ','YM','E7','J7','6A','6B','6C','6E','6J','6M','6N','6S','6R','GC','CL','IBV','ZW','ZC','ZS','XW']:
            return "cme"
        elif _basename in ['FGBS','FGBM','FGBL','FGBX','FBTS','FBTP','FBTM','FOAT','FOAM','CONF','FESX','FDAX','FSMI','FEXF','FESB','FSTB','FSTS','FSTO','FSTG','FSTI','FSTM','FXXP','OKS2','FEXD','FRDX','FVS','FEU3']:
            return "eurex"
        elif _basename in ['SXF','CGB','CGF','CGZ','BAX']:
            return "tmx"
        elif _basename in ['BR_DOL','BR_IND','BR_WIN','BR_WDO','BR_WEU','DI1']:
            return "bmf"
        elif _basename in ['JFFCE','KFFTI','LFZ','LFI','LFL','LFR','YFEBM','XFW','XFC','XFRC']:
            return "liffe"
        elif _basename in ['LFL','LFR','LFS']:
            return "ice" 
        elif _basename in ['Si','RI','BR','ED','GD']:
            return "rts"
        elif _basename in ['GFJGB','FFTPX','FFMTP','GFMJG']:
            return "tse"
        elif _basename in ['USD/RUB']:
            return "ebs"
        elif _basename in ['NKMF','DJI','JGBL']:
            return "ose"
        else:
            sys.exit('Unhandled symbol in get_exchange_for_product')

    # Given a contract eg: ES_1,this function finds out the last trading day we can trade the contract without being called for notice or expiry
    # Assuming that the current date is the 'date' argument
    def get_last_trading_date( self, date, product ):
        _exchange = self.get_exchange_for_product( product )
        _basename = self.get_basename( product )
        _contract_number = self.get_contract_number( product )
        _current_min_last_trading_date = date
        _current_month = date.month
        _current_year = date.year
        while( _contract_number > 0):
            if getattr( ExchangeSymbolManager, 'is_'+_exchange+'_month' )( self, _basename, _current_month ):
                _next_month = _current_month
                _next_year = _current_year
                _next_last_trading_date = getattr( ExchangeSymbolManager, 'get_'+_exchange+'_last_trading_date' )( self, _basename, _next_month, _next_year )
                if _next_last_trading_date < _current_min_last_trading_date:
                    _next_month,_next_year = self.get_next_month( _basename, _exchange, _next_month, _next_year )
                    _next_last_trading_date = getattr( ExchangeSymbolManager, 'get_'+_exchange+'_last_trading_date' )( self, _basename, _next_month, _next_year )
                _current_min_last_trading_date = _next_last_trading_date
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            else:
                _next_month = _current_month
                _next_year = _current_year
                _next_month,_next_year = self.get_next_month( _basename, _exchange, _next_month, _next_year )
                _next_last_trading_date = getattr( ExchangeSymbolManager, 'get_'+_exchange+'_last_trading_date' )( self, _basename, _next_month, _next_year )
                if _next_last_trading_date < _current_min_last_trading_date:
                    _next_month,_next_year = self.get_next_month( _basename, _exchange, _next_month, _next_year )
                    _next_last_trading_date = getattr( ExchangeSymbolManager, 'get_'+_exchange+'_last_trading_date' )( self, _basename, _next_month, _next_year )
                _current_min_last_trading_date = _next_last_trading_date
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            if _contract_number > 1:
                _current_min_last_trading_date = _current_min_last_trading_date + datetime.timedelta(days=3)
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            _contract_number = _contract_number - 1
        return _current_min_last_trading_date

    # Returns the date corresponding to 1 business day prior to the last trading date
    def get_last_date_for_overnight_positions( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        _last_trading_date = _last_trading_date + datetime.timedelta(days=-1)
        while _last_trading_date.weekday() == 5 or _last_trading_date.weekday() == 6: #If saturday of sunday,then go to previous day
            _last_trading_date = _last_trading_date + datetime.timedelta(days=-1) #TODO should check for holidays   
        return _last_trading_date

    def get_exchange_symbol( self, date, product ):
        _exchange = self.get_exchange_for_product( product )
        return getattr( ExchangeSymbolManager, 'get_exchange_symbol_'+_exchange )( self, date, product )

    def get_next_month( self, _basename, _exchange, _next_month, _next_year ):
        if getattr( ExchangeSymbolManager, 'is_'+_exchange+'_month' )( self, _basename, _next_month ):
            _next_month += 1
            if _next_month > 12:
                _next_month = 1
                _next_year +=1
        while not getattr( ExchangeSymbolManager, 'is_'+_exchange+'_month' )( self, _basename, _next_month ):
            _next_month += 1
            if _next_month > 12:
                _next_month = 1
                _next_year +=1 
        return ( _next_month,_next_year )

    def is_cme_month( self, _basename, _this_month ):
        if _basename == "CL" : # CL is a monthly contract
            return True
        if _basename == "GC" : # GC contract months are 1,3,5,7,9,11 
            return ( ( _this_month + 1 ) % 2 ) == 0
        if _basename == "IBV": # IBV contract months are 2,4,6,8,10,12
            return ( _this_month % 2 ) == 0
        if _basename in ['ZW', 'XW', 'ZC']:
	    if _this_month in [3,5,7,9,12]: 
                return True
	    else:
                return False
        if _basename == "ZS":
	    if _this_month in [1,3,5,7,8,9,11]:
                return True
            else:
                return False
        # Default case
        return ( _this_month % 3 ) == 0 # Quaterly contract months 3,6,9,12

    def is_eurex_month( self, _basename, _this_month ):
        if _basename == "FVS": # FVS is a monthly contract
            return True
        return ( _this_month % 3 ) == 0
        
    def is_liffe_month( self, _basename, _this_month ):
        if _basename in ['LFR','LFZ','LFI','LFL']:
            return ( _this_month % 3 ) == 0
        if _basename == 'YFEBM':
            if _this_month in [1,3,5,11]:
                return True
            else:
                return False
        if _basename == 'XFC':
            if _this_month in [3,5,7,9,12]:
                return True
            else:
                return False
        if _basename == 'XFW':
            if _this_month in [3,5,8,10,12]:
                return True
            else:
                return False
        if _basename == 'XFRC':
            if _this_month in [1,3,5,7,9,11]:
                return True
            else:
                return False
        return True # FCE is a monthly contract #TODO check why LFI,LFL written

    def is_tmx_month( self, _basename, _this_month ):
        return ( _this_month % 3 ) == 0
     
    def get_cme_last_trading_date( self, _basename, _next_cme_month, _next_cme_year ):
        #ES, NQ, YM
        #Settlement Date: The 3rd Friday of contract month
        #Last Trading Day: Same as Settlement Date
        #OBSERVATION: the volume shifts towards the next future contract one week before i.e 2nd Friday
        if _basename in ['ES', 'NQ', 'NKD', 'NIY', 'YM']:
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year) # 2nd friday of that month of that year       
            date = date + datetime.timedelta( days=-1 ) # Subtract one day since last trading day is the day when we have decent volume in 1st contract
            return date
        if _basename == 'NKD': # based on volume observation
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year) # 2nd friday of that month of that year       
            date = date + datetime.timedelta( days=-2 ) # Subtract two days since last trading day is the day when we have decent volume in 1st contract
            return date

        # ZT, ZF, ZN, ZB, UB : first notice date is the last day of the previous month
        # We expect to stop trading a day before
        if _basename in ['ZT', 'ZF', 'ZN', 'ZB', 'UB', 'ZW', 'ZC', 'ZS', 'XW']: 
            date = datetime.date(year=_next_cme_year,day=1,month=_next_cme_month) # 1st day of that month of that year
            #date = date + datetime.timedelta( days=-2) TODO how do they take care of holidays
            for i in range(0,2): #TODO ###
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date
        # Instead of Monday (Third Wednesday -2 business day ), the volume starts shifting on the previous Friday itself
        # We should make one day before (Thursday) as the last trading date
        if _basename in ['6A', '6B', '6E', '6J', '6M', '6N', '6S', '6C','E7','J7']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            date = date + datetime.timedelta( days=-6) #should skip exchange holidays
            return date
        if _basename == '6R': #TODO verify
            date = datetime.date(year=_next_cme_year,day=15,month=_next_cme_month) # 15th day of that month of that year
            #date = date + datetime.timedelta( days=-1) TODO how do they take care of holidays
            for i in range(0,1): #TODO ###
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date
        #TODO check if 6C should be handled separately
        # GE : second London bank business day prior to the third Wednesday of the contract expiry month
        # We expect to stop trading a day before
        if _basename == 'GE':
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            for i in range(0,3): # Go three business days back
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ): 
                    date = date + datetime.timedelta( days=-1)
            return date
        #Volume shifts on 19th
        if _basename == 'CL':
            date = datetime.date(year=_next_cme_year,day=18,month=_next_cme_month)
            weekday = date.weekday()
            # If 19th is SUNDAY,volume shifts on 17th,so 16th would be the last trading date
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-2)
            elif weekday == 4: #If friday
                date = date + datetime.timedelta( days=-1)
            elif weekday == 6: #If sunday #TODO ### if 18th is sunday,how do they handle
                 date = date + datetime.timedelta( days=-2)
            return date
        # volume shifts on second last trading day of expiry month
        if _basename == 'GC':
            date = datetime.date(year=_next_cme_year,day=monthrange(_next_cme_year,_next_cme_month)[1],month=_next_cme_month) # day = last day of this month
            weekday = date.weekday()
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-1)
            if weekday == 6: #If sunday
                date = date + datetime.timedelta( days=-2)
            #date = date + datetime.timedelta( days=-2) TODO they might be returning weekends
            for i in range(0,2): # Go two business days back #TODO ###
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date
        if _basename == 'IBV':
            date = datetime.date(year=_next_cme_year,day=15,month=_next_cme_month) 
            while date.weekday() != 2: # Keep moving back till we hit wednesday
                date = date + datetime.timedelta( days=-1)
            date_day = date.day
            date2 = datetime.date(year=_next_cme_year,day=15,month=_next_cme_month)     
            while date.weekday() != 2: # Keep moving forward till we hit wednesday
                date = date + datetime.timedelta( days=1)
            date2_day = date2.day
            if (date2_day - 15) < ( 15 - date_day): # Select the closest one
                date2 = date2 + datetime.timedelta( days=-1)
                return date2
            else:
                date = date + datetime.timedalta( days=-1 ) #TODO need to look at exchange holiday thing
                return date       
        print 'UnHandled case: Using default'
        # Default day is 10th of month
        date = datetime.date(year=_next_cme_year,day=10,month=_next_cme_month)
        #date = date + datetime.timedelta( days=-1 )
        for i in range(0,1): # Go one business days back #TODO ###
            date = date + datetime.timedelta( days=-1)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
        return date

    def get_eurex_last_trading_date( self, _basename, _next_eurex_month, _next_eurex_year ):
        if _basename in ['FESX','FEXD','FDAX','FRDX','FEXF','FESB','FSTB','FSTS','FSTO','FSTG','FSTI','FSTM','FXXP','FSMI']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_eurex_month, _next_eurex_year) # 3rd friday of that month of that year
            date = date + datetime.timedelta(days=-1)
            return date
        if _basename == 'FVS':
            date = datetime.date(year=_next_eurex_year,day=monthrange(_next_eurex_year,_next_eurex_month)[1],month=_next_eurex_month)
            while not date.weekday() == 4:
                date += datetime.timedelta(days=-1)
            date += datetime.timedelta(days=-10) # Tuesday before second last friday
            while not self.is_eurex_exchange_date(_basename, date):
                date += datetime.timedelta(days=-1)
            return date
        if _basename == 'FEU3':
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_eurex_month, _next_eurex_year) # 3rd wednesday of that month of that year
            for i in range(0,3): # Go three business days back #TODO ###
                date = date + datetime.timedelta( days=-1)
                while not self.is_eurex_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            #date += datetime.timedelta(days=-3) #TODO ask how can sunday be the last trading day
            return date
        if _basename == 'OKS2':
            date = self.get_date_from_nth_day_of_month_year(2, 'THURSDAY', _next_eurex_month, _next_eurex_year) # 3rd wednesday of that month of that year
            date += datetime.timedelta(days=-1)
            return date
        if _basename in ['FGBS','FGBM','FGBL','FGBX','FBTS','FBTP','FBTM','CONF','FOAT','FOAM']:
            date = datetime.date(year=_next_eurex_year,day=10,month=_next_eurex_month)
            while not self.is_eurex_exchange_date(_basename, date):
                date += datetime.timedelta(days=1)
            # Observation : Making it 4 days from 3 days based on volume observation manually
            # Docs says Last trading day is 2 days before delivery day
            # 2+1 exchange days prior to Delivery Day
            for i in range(0,4):
                date += datetime.timedelta(days=-1)
                while not self.is_eurex_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date       
        print 'UnHandled case: Using default' 
        #Default case
        date = datetime.date(year=_next_eurex_year,day=10,month=_next_eurex_month) 
        while not self.is_eurex_exchange_date(_basename, date): # TODO ###
            date += datetime.timedelta(days=-1)
        return date

    def get_liffe_last_trading_date( self, _basename, _next_liffe_month, _next_liffe_year ):
        if _basename in ['JFFCE','KFFTI','LFZ']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_liffe_month, _next_liffe_year) # 3rd friday of that month of that year
            date = date + datetime.timedelta(days=-1)
            return date
        if _basename == 'LFL':
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_liffe_month, _next_liffe_year) # 3rd wednesday of that month of that year
            date = date + datetime.timedelta(days=-1)
            return date
        if _basename == 'LFI':
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_liffe_month, _next_liffe_year) # 3rd wednesday of that month of that year
            #date = date + datetime.timedelta(days=-3)
            for i in range(0,3): # Go three business days back # TODO ###
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1) 
            return date
        if _basename == 'LFR':
            date = datetime.date(day=1, month=_next_liffe_month, year=_next_liffe_year)
            date = date + datetime.timedelta(days=-1)
            weekday = date.weekday()
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-1)
            if weekday == 6: #If sunday
                date = date + datetime.timedelta( days=-2)
            for i in range(0,2): # Go two business days back # TODO ###
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            #date = date + datetime.timedelta( days=-2) # TODO check -> can end up being saturday or sunday
            return date
        if _basename == 'XFC':
            date = datetime.date(day=monthrange(_next_liffe_year,_next_liffe_month)[1], month=_next_liffe_month, year=_next_liffe_year)
            skip_day = 0
            while skip_day < 11:
                weekday = date.weekday()
                if weekday == 5: #If saturday
                    date = date + datetime.timedelta( days=-1)
                if weekday == 6: #If sunday
                    date = date + datetime.timedelta( days=-2)
                date = date + datetime.timedelta( days=-1)
                skip_day += 1
            date += datetime.timedelta(days=-40) # Based on volume shift pattern
            weekday = date.weekday()
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-1)
            if weekday == 6: #If sunday
                date = date + datetime.timedelta( days=-2)
            return date
        if _basename == 'XFW':
            date = datetime.date(day=1, month=_next_liffe_month, year=_next_liffe_year)
            date = date + datetime.timedelta(days=-16) # Actual
            date = date + datetime.timedelta(days=-40) # Based on volume shift pattern
            weekday = date.weekday()
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-1)
            if weekday == 6: #If sunday
                date = date + datetime.timedelta( days=-2)
            return date
        if _basename == 'XFRC':
            date = datetime.date(day=monthrange(_next_liffe_year,_next_liffe_month)[1], month=_next_liffe_month, year=_next_liffe_year) # Actual
            date = date + datetime.timedelta(days=-40) # Based on volume shift pattern
            weekday = date.weekday()
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-1)
            if weekday == 6: #If sunday
                date = date + datetime.timedelta( days=-2)
            return date
        if _basename == 'YFEBM':
            date = datetime.date(day=10, month=_next_liffe_month, year=_next_liffe_year) # Actual
            date = date + datetime.timedelta(days=-3) # Based on volume shift pattern
            weekday = date.weekday()
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-1)
            if weekday == 6: #If sunday
                date = date + datetime.timedelta( days=-2)
            return date
        print 'UnHandled case: Using default'
        #Default
        return datetime.date(day=10, month=_next_liffe_month, year=_next_liffe_year)

    # SXF
    # Settlement Day : Third Friday of the respective quarterly month, if this day is an exchange day; otherwise, 1st preceeding day.
    # Last Trading Day : One trading day prior to the Settlement Day of the relevant maturity month.

    # CGB
    # Settlement, Last Trading Day: Trading finishes at 1 pm Montreal Time on 7th business day preceding last trading day of delivery month.

    # BAX
    # Settlement, Last Trading Day: Trading ceases at 10 am on the 2nd UK business day preceding third Wednesday of contract month; in case of holiday previous business day 
    def get_tmx_last_trading_date( self, _basename, _next_tmx_month, _next_tmx_year ):
        if _basename == 'SXF':
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_tmx_month, _next_tmx_year) # 3rd friday of that month of that year
            while not self.is_tmx_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            date = date + datetime.timedelta( days=-1) # last trading day is one day prior # TODO if two holidays in a week then this will return wrong
            date = date + datetime.timedelta( days=-2) # Roll two more days before the last trading date based on observation
            return date
        if _basename in ['CGB','CGF','CGZ']:
            date = datetime.date(day=1, month=_next_tmx_month, year=_next_tmx_year)
            while not self.is_tmx_exchange_date( _basename, date ): # First Business day of month
                date = date + datetime.timedelta( days=1 )
            for i in range(0,5):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_tmx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date
        if _basename == 'BAX':
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_tmx_month, _next_tmx_year) # 3rd wednesday of that month of that year
            for i in range(0,3): # Second london banking day prior + 1 day prior is the last trading day #TODO ###
                date = date + datetime.timedelta( days=-1 )
                while not self.is_tmx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            #date = date + datetime.timedelta( days=-1)
            return date
        print 'UnHandled case: Using default'
        # Default
        date = datetime.date(day=5, month=_next_tmx_month, year=_next_tmx_year)
        while not self.is_tmx_exchange_date( _basename, date ): #TODO ###
            date = date + datetime.timedelta( days=-1)
        return date 

    def get_cme_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ): 
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['ZT', 'ZF', 'ZN', 'ZB', 'UB', 'ZW', 'ZC', 'ZS', 'XW','CL']: # ZT, ZF, ZN, ZB, UB : first notice date is the last day of the previous month,we expect to stop trading a day before
            if _current_min_last_trading_date_mm == 12: # TODO check CL added is correct or not
                _current_min_last_trading_date_mm = 1 
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1 
        if _basename == 'GC':
            if _current_min_last_trading_date_mm == 9:
                _current_min_last_trading_date_mm += 3
            elif _current_min_last_trading_date_mm == 10:
                _current_min_last_trading_date_mm += 2
            elif _current_min_last_trading_date_mm % 2 != 0:
                _current_min_last_trading_date_mm += 1 
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)    

    def get_eurex_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)

    def get_liffe_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ):
        _this_local_trade_date = _current_min_last_trading_date
        _current_month = _current_min_last_trading_date.month
        _current_year = _current_min_last_trading_date.year
        if _basename in ['LFR','XFC','XFRC']:
            # Incrementing the Year is not required here, since the expiry based last trading date function will always return the correct Date from which we extract this expiry
            _current_month += 1
            if _current_month == 13:
                _current_month = 1
                _current_year += 1
            _this_local_trade_date = datetime.date(day=1, year=_current_year, month=_current_month)
        if _basename == 'XFW':
            # Incrementing the Year is not required here, since the expiry based last trading date function will always return the correct Date from which we extract this expiry
            _current_month += 2
            _this_local_trade_date = datetime.date(day=1, year=_current_year, month=_current_month)
        return _basename + self.month_codes[str(_current_month)] + self.get_yy(_current_year) # TODO liffe has a different convention for exchange symbol,need to change it: use _this_local_trade_date        
    def get_tmx_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        # For CGB : first notice date is the 3rd last day of the previous month
        # We expect to stop trading a 2 day before and rollover
        if _basename in ['CGB','CGF','CGZ']:
            if _current_min_last_trading_date_mm == 12: 
                _current_min_last_trading_date_mm = 1
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1 
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)

    def is_eurex_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_cme_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_liffe_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_tmx_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def get_exchange_symbol_cme( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_cme_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_eurex( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_eurex_symbol_from_last_trading_date( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_liffe( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_liffe_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_tmx( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_tmx_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

def __main__() :
    if len( sys.argv ) > 1:
        product = sys.argv[1]
        date = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
        exchange_symbol_manager = ExchangeSymbolManager()
        print 'Exchange Symbol: ',exchange_symbol_manager.get_exchange_symbol( date, product )
        print 'Last Trading Date: ',exchange_symbol_manager.get_last_trading_date( date, product )
        print 'Last day to have overnight positions: ',exchange_symbol_manager.get_last_date_for_overnight_positions( date, product )
    else:
        print 'python exchange_symbol_manager.py product date'
        sys.exit(0)

if __name__ == '__main__':
    __main__();
