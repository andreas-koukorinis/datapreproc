# cython: profile=True
#!/usr/bin/env python
import sys
import datetime
from calendar import monthrange

#gawk '$8 == 1.0 {print $1}' FS=, data/ZT_1.csv -> last trading day
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
        if _basename in ['ZT','ZF','ZN','ZB','NKD','NIY','ES','EMD','NQ','YM','6A','6B','6C','6E','6J','6M','6N','6S','GC','SI','HG','PL','PA','HO','RB','NG','ZW','ZC','ZS','ZM','ZL','LH']:
            return "cme"
        elif _basename in ['FGBS','FGBM','FGBL','FGBX','FESX','FDAX','FSMI']:
            return "eurex"
        elif _basename in ['SXF', 'CGB']: # TOADD BAX
            return "tmx"
        elif _basename in ['LFZ', 'LFR']: # TOADD FES,LFI,LFL
            return "liffe"
        elif _basename in ['KC','CT','CC','SB','G','BRN']:
            return "ice" 
        elif _basename in ['TOPIX']:
            return "tse"
        elif _basename in ['JGBL','JNK']:
	    return "ose"
        else:
            sys.exit('Unhandled symbol in get_exchange_for_product')
        #FCE,FTI -> EURONEXT
        #MFX -> MEFF
        #SPI -> ASX
        #ALSI -> SAFEX
        #SIN, SG ->SGX
        #HHI,HSI -> HKFE
        #KOSPI -> KRW

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
        #if _basename == "CL" : # CL is a monthly contract
        #    return True
        #if _basename == "GC" : # GC contract months are 1,3,5,7,9,11 
        #    return ( ( _this_month + 1 ) % 2 ) == 0
        if _basename in ['ZC']:
            if _this_month in [3,5,7,12]: # 3,5,7,9,12
                return True
            else:
                return False
        if _basename in ['ZW']:
            if _this_month in [3,5,7,9,12]: 
                return True
            else:
                return False
        if _basename in ['ZM','ZL']:
            if _this_month in [1,3,5,7,12]: #[1,3,5,7,8,9,10,12]:
                return True
            else:
                return False
        if _basename == "ZS":
            if _this_month in [1,3,5,7,11]: #[1,3,5,7,8,9,11]:
                return True
            else:
                return False
        # NIY has monthly contracts but 3,6,9,12 are most common
        if _basename == "LH":
            if _this_month in [2,4,5,6,7,8,10,12]:
                return True 
            else:
                return False
        if _basename in ['GC','SI','HG','PL','PA','HO','RB','NG']:
                return True
        # Default case
        return ( _this_month % 3 ) == 0 # Quaterly contract months 3,6,9,12 -> ES,EMD,NKD,NIY,YM,NQ,6A,6B,6C,6E,6M,6N,6S,ZT,ZF,ZN,ZB

    def is_eurex_month( self, _basename, _this_month ):
        return ( _this_month % 3 ) == 0
        
    def is_liffe_month( self, _basename, _this_month ):
        if _basename in ['LFR','LFZ','LFI','LFL']:
            return ( _this_month % 3 ) == 0
        return True # FCE is a monthly contract

    def is_tmx_month( self, _basename, _this_month ):
        return ( _this_month % 3 ) == 0 # SXF, CGB

    def is_ice_month( self, _basename, _this_month ):
        if _basename in ['KC','CC']:
            if _this_month in [3,5,7,9,12]:
                return True
            else:
                return False
        if _basename in ['CT']:
            if _this_month in [3,5,7,12]:#[3,5,7,10,12]:
                return True
            else:
                return False
        if _basename in ['SB']:
            if _this_month in [3,5,7,10]:
                return True
            else:
                return False
        if _basename in ['BRN', 'G']:
            return True     

    def is_tse_month( self, _basename, _this_month ):
        if _basename in ['TOPIX']:
            return ( _this_month % 3 ) == 0
        return True

    def is_ose_month( self, _basename, _this_month ):
        if _basename in ['JNK','JGBL']:
            return ( _this_month % 3 ) == 0
        return True


    def get_cme_last_trading_date( self, _basename, _next_cme_month, _next_cme_year ):
        #ES,EMD,YM,NQ
        #Settlement Date: The 3rd Friday of contract month or first earlier date when the index is published
        #Last Trading Day: Same as Settlement Date
        #OBSERVATION: the volume shifts towards the next future contract one week before i.e 2nd Friday -> 2014-03-14, 2014-06-13, 2014-09-12
        if _basename in ['ES', 'NQ', 'YM']:
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year) # 2nd friday of that month of that year       
            return date

        # Observation: For EMD volume shifts o 2nd thursday. -> 2014-03-13, 2014-06-12, 2014-09-11
        if _basename == 'EMD':
            date = self.get_date_from_nth_day_of_month_year(2, 'THURSDAY', _next_cme_month, _next_cme_year)
            return date

        #NIY -> Last trading day on the Thursday prior to the second Friday of the contract month
        if _basename == 'NIY': # based on volume observation
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year)
            date = date + datetime.timedelta( days=-2 )
            return date

        #NKD -> Last trading day on Business Day prior to 2nd Friday of the contract month
        #Observation : Volume shift on 2nd Monday. -> 2014-09-08, 2014-06-09, 2014-03-10
        if _basename == 'NKD': # based on volume observation
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year)
            for i in range(0,4):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # ZT, ZF, ZN, ZB : first notice date is the last day of the previous month
        # ZT,ZF -> Last trading day is Last business day of the calendar month. 
        # ZN,ZB -> Last trading day is Seventh business day preceding the last business day of the delivery month.
        # Observation : go 4 business days prior to 1st day of contract month -> 2013-11-25, 2014-02-25, 2014-05-27, 2014-08-26
        if _basename in ['ZT', 'ZF', 'ZN', 'ZB']:
            date = datetime.date(year=_next_cme_year,day=1,month=_next_cme_month) # 1st day of that month of that year
            for i in range(0,4):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # ZC,ZW,ZS,ZM,ZL -> Last trading day is the business day prior to the 15th calendar day of the contract month.
        if _basename in ['ZC','ZW','ZS','ZM','ZL']:
            date = datetime.date(year=_next_cme_year,day=1,month=_next_cme_month) # 1st day of that month of that year
            for i in range(0,14):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # 6A,6B,6E,6J,6M,6N,6S -> Last trading day is on the second business day immediately preceding the third Wednesday of the contract month (usually Monday)
        # Observation -> mostly volume shifts on 2nd tuesday
        # 6A -> 2013-03-11, 2013-06-11, 2013-09-11, 2013-12-10, 2014-03-12, 2014-06-11, 2014-09-09 
        if _basename in ['6A', '6B', '6E', '6J', '6M', '6N', '6S']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            for i in range(0,8):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # 6C -> Last trading day is on the business day immediately preceding the third Wednesday of the contract month (usually Tuesday).
        if _basename in ['6C']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            for i in range(0,7):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        if _basename == '6R': #TODO verify
            date = datetime.date(year=_next_cme_year,day=15,month=_next_cme_month) # 15th day of that month of that year
            #date = date + datetime.timedelta( days=-1) TODO how do they take care of holidays
            for i in range(0,1): #TODO ###
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

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
        # Last trading day is 3rd friday of contract month 
        if _basename in ['FESX','FDAX','FSMI']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_eurex_month, _next_eurex_year) # 3rd friday of that month of that year
            for i in range(0,4): # 3rd monday usually
                date += datetime.timedelta(days=-1)
                while not self.is_eurex_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

        # Last trading day is 2 days before delivery day(10th of contract month)
        if _basename in ['FGBS','FGBM','FGBL','FGBX']:
            date = datetime.date(year=_next_eurex_year,day=10,month=_next_eurex_month)
            while not self.is_eurex_exchange_date(_basename, date):
                date += datetime.timedelta(days=1)
            for i in range(0,5):
                date += datetime.timedelta(days=-1)
                while not self.is_eurex_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date       
        print 'UnHandled case: Using default' 

        #Default case
        date = datetime.date(year=_next_eurex_year,day=10,month=_next_eurex_month) 
        while not self.is_eurex_exchange_date(_basename, date):
            date += datetime.timedelta(days=-1)
        return date

    def get_liffe_last_trading_date( self, _basename, _next_liffe_month, _next_liffe_year ):
        # last trading day is 3rd friday of delivery month
        if _basename in ['LFZ']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_liffe_month, _next_liffe_year) # 3rd friday of that month of that year
            for i in range(0,4): # 3rd monday usually
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
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
        # Last trading day : Two business days prior to the last business day in the delivery month
        # First notice day : Two business days prior to the first day of the delivery month
        if _basename == 'LFR':
            date = datetime.date(day=1, month=_next_liffe_month, year=_next_liffe_year)
            for i in range(0,6):
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date
        print 'UnHandled case: Using default'
        #Default
        return datetime.date(day=10, month=_next_liffe_month, year=_next_liffe_year)

    def get_tmx_last_trading_date( self, _basename, _next_tmx_month, _next_tmx_year ):
        # Settlement Day : Third Friday of the respective quarterly month, if this day is an exchange day; otherwise, 1st preceeding day.
        # Last Trading Day : One trading day prior to the Settlement Day of the relevant maturity month.
        if _basename == 'SXF':
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_tmx_month, _next_tmx_year) # 3rd friday of that month of that year
            while not self.is_tmx_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,3): # Second london banking day prior + 1 day prior is the last trading day #TODO ###
                date = date + datetime.timedelta( days=-1 )
                while not self.is_tmx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # CGB -> Delivery notice ->third business day preceding the first business day of the delivery month and the third business day preceding the last business day of the delivery month, inclusively. 
        if _basename in ['CGB']:
            date = datetime.date(day=1, month=_next_tmx_month, year=_next_tmx_year)
            for i in range(0,3): # Notice day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_tmx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,1):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_tmx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # BAX -> Settlement, Last Trading Day: Trading ceases at 10 am on the 2nd UK business day preceding third Wednesday of contract month; in case of holiday previous business day 
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

    def get_ice_last_trading_date( self, _basename, _next_ice_month, _next_ice_year ):
        # KC: First Notice Day : Seven business days prior to first business day of delivery month
        if _basename == 'KC':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            while not self.is_ice_exchange_date( _basename, date ): # Last Business day of contract month
                date = date + datetime.timedelta( days=1 )
            for i in range(0,7): # First notice day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,9): # Adjustment based on volume
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # CT: First Notice Day : Five business days before the first delivery day of the spot contract month, which is the first business day of that month.
        if _basename == 'CT':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            while not self.is_ice_exchange_date(_basename, date): # End of spot month
                date = date + datetime.timedelta( days=-1 )
            for i in range(0,5):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,12): # Adjustment based on volume
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # SB: First Notice day : First business day after last trading day.
        # Last trading day : Last business day of the month preceding the delivery month 
        if _basename == 'SB':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            while not self.is_ice_exchange_date(_basename, date): # End of spot month
                date = date + datetime.timedelta( days=1 ) # First notice day
            for i in range(0,18):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # CC: First Notice Day : Ten business days prior to first business day of delivery month.
        if _basename == 'CC':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            for i in range(0,10):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,11):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date
        # BRN : Last trading day: 15th calendar day before the first calendar day of the contract month        
        if _basename == 'BRN':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            for i in range(0,15):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,7):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # 2 business days prior to the 14th calendar day of the delivery month.
        if _basename == 'G':
            date = datetime.date(day=14, month=_next_ice_month, year=_next_ice_year)
            for i in range(0,2):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,5):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        sys.exit('UnHandled case: Using default')

    def get_ose_last_trading_date( self, _basename, _next_ose_month, _next_ose_year ):
        # Last trading day : 7th business day prior to each delivery date ( 20th day of each contract month, move-down the date when it is not the business day )
        if _basename == 'JGBL':
            date = datetime.date(day=20, month=_next_ose_month, year=_next_ose_year)
            while not self.is_ose_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,7): # last trading day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ose_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,3):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ose_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # Last trading day : The business day preceding the second Friday of each contract month (When the second Friday is a non-business day, it shall be the preceding business day.)
        if _basename == 'JNK':
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_ose_month, _next_ose_year)
            while not self.is_ose_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,3): # last trading day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ose_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_tse_last_trading_date( self, _basename, _next_tse_month, _next_tse_year ):
        # Last trading day : The business day prior to the second Friday (or the preceding Thursday if the second Friday is a holiday)
        if _basename == 'TOPIX':
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_tse_month, _next_tse_year)
            while not self.is_tse_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,4): # last trading day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_tse_exchange_date( _basename, date ):
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

    def get_ice_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['KC','CT']:
            if _current_min_last_trading_date_mm == 12:
                _current_min_last_trading_date_mm = 1
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)

    def get_tse_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)

    def get_ose_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)

    def is_eurex_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_cme_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_liffe_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_tmx_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_ice_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_ose_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_tse_exchange_date( self, _basename, date ):
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

    def get_exchange_symbol_ice( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_ice_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_ose( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_ose_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_tse( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_tse_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

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
