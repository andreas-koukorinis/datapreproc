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
        if _basename in ['ZT','ZF','ZN','ZB','NKD','NIY','ES','SP', 'EMD','NQ','YM','6A','6B','6C','6E','6J','6M','6N','6S','GC','SI','HG','PL','PA','LH','ZW','ZC','ZS','ZM','ZL']: # RB,NG,HO,CL
            return "cme"
        elif _basename in ['FGBS','FGBM','FGBL','FESX','FDAX','FSMI']: # FGBX
            return "eurex"
        elif _basename in ['SXF', 'CGB']: # BAX
            return "tmx"
        elif _basename in ['LFZ', 'LFR', 'FTI']: # TOADD LFI,LFL,FES,FCE
            return "liffe"
        elif _basename in ['KC','CT','CC','SB']: # G, BRN
            return "ice" 
        elif _basename in ['TOPIX']:
            return "tse"
        elif _basename in ['JGBL','JNK']:
	    return "ose"
        elif _basename in ['SIN','SG']:
            return "sgx"
        elif _basename in ['HHI','HSI']:
            return "hkfe"
        elif _basename in ['ALSI']:
            return "safex"
        elif _basename in ['SPI']:
            return "asx"
        elif _basename in ['MFX']:
            return "meff"
        elif _basename in ['KOSPI']:
            return "krw"
        elif _basename in ['VX']:
            return "cfe"
        #elif _basename in ['YEA']:
        #    return "tfx"
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
                while _next_last_trading_date < _current_min_last_trading_date:
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
                while _next_last_trading_date < _current_min_last_trading_date:
                    _next_month,_next_year = self.get_next_month( _basename, _exchange, _next_month, _next_year )
                    _next_last_trading_date = getattr( ExchangeSymbolManager, 'get_'+_exchange+'_last_trading_date' )( self, _basename, _next_month, _next_year )
                _current_min_last_trading_date = _next_last_trading_date
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            if _contract_number > 1:
                _current_min_last_trading_date = _current_min_last_trading_date + datetime.timedelta(days=10)
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
        if _basename == "LH":
            if _this_month in [2,4,6,7,8,10,12]:
                return True 
            else:
                return False
        if _basename == "GC":
            if _this_month in [2,4,6,8,12]: #[1,2,3,4,5,6,7,8,9,10,11,12]:
                return True
            else:
                return False
        if _basename in ["SI","HG"]:
            if _this_month in [3,5,7,9,12]: #[1,2,3,4,5,6,7,8,9,10,11,12]:
                return True
            else:
                return False
        if _basename == "PL":
            if _this_month in [1,4,7,10]: #[1,2,3,4,5,6,7,8,9,10,11,12]:
                return True
            else:
                return False
        if _basename == "NG":
            return ( (_this_month + 1)  % 2 ) == 0 #[1,2,3,4,5,6,7,8,9,10,11,12]
        if _basename in ['CL','BRN','HO','RB','NG','G']:
                return True
        # Default case, forced -> PA, NIY
        return ( _this_month % 3 ) == 0 # Quaterly contract months 3,6,9,12 -> ES,SP,EMD,NKD,NIY,YM,NQ,6A,6B,6C,6E,6M,6N,6S,ZT,ZF,ZN,ZB

    def is_eurex_month( self, _basename, _this_month ):
        return ( _this_month % 3 ) == 0
        
    def is_liffe_month( self, _basename, _this_month ):
        if _basename in ['LFR','LFZ','FES']:
            return ( _this_month % 3 ) == 0
        return True # FTI is a monthly contract

    def is_tmx_month( self, _basename, _this_month ):
        return ( _this_month % 3 ) == 0 # SXF, CGB, BAX

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

    def is_sgx_month( self, _basename, _this_month ):
        return True # SG, SIN

    def is_hkfe_month( self, _basename, _this_month ):
        return True # HHI, HSI

    def is_safex_month( self, _basename, _this_month ):
        if _basename in ['ALSI']:
            return ( _this_month % 3 ) == 0
        return True

    def is_asx_month( self, _basename, _this_month ):
        if _basename in ['SPI']:
            return ( _this_month % 3 ) == 0
        return True

    def is_meff_month( self, _basename, _this_month ):
        return True # MFX

    def is_krw_month( self, _basename, _this_month ):
        if _basename in ['KOSPI']:
            return ( _this_month % 3 ) == 0
        return True

    def is_tfx_month( self, _basename, _this_month ):
        if _basename in ['YEA']:
            return ( _this_month % 3 ) == 0
        return True

    def is_cfe_month(self, _basename, _this_month):
        if _basename in ['VX']:
            return True

    def get_cme_last_trading_date( self, _basename, _next_cme_month, _next_cme_year ):
        #ES,EMD,YM,NQ
        #Settlement Date: The 3rd Friday of contract month or first earlier date when the index is published
        #Last Trading Day: Same as Settlement Date
        #OBSERVATION: the oi shifts towards the next future contract one week before i.e 2nd Friday -> 2014-03-14, 2014-06-13, 2014-09-12
        if _basename in ['ES','SP', 'NQ', 'YM']:
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year) # 2nd friday of that month of that year       
            return date

        # Observation: For EMD oi shifts o 2nd thursday. -> 2014-03-13, 2014-06-12, 2014-09-11
        if _basename == 'EMD':
            date = self.get_date_from_nth_day_of_month_year(2, 'THURSDAY', _next_cme_month, _next_cme_year)
            return date

        #NIY -> Last trading day on the Thursday prior to the second Friday of the contract month
        if _basename == 'NIY': # based on oi observation
            date = self.get_date_from_nth_day_of_month_year(2, 'FRIDAY', _next_cme_month, _next_cme_year)
            for i in range(0,3):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        #NKD -> Last trading day  n Business Day prior to 2nd Friday of the contract month
        #Observation : Volume shift on 2nd Monday. -> 2014-09-08, 2014-06-09, 2014-03-10
        if _basename == 'NKD': # based on oi observation
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
        # Observation -> mostly oi shifts on 2nd tuesday
        # 6A -> 2013-03-11, 2013-06-11, 2013-09-11, 2013-12-10, 2014-03-12, 2014-06-11, 2014-09-09 
        if _basename in ['6A', '6B', '6E', '6J', '6M']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            for i in range(0,6): # 2nd tuesday
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # 6A,6B,6E,6J,6M,6N,6S -> Last trading day is on the second business day immediately preceding the third Wednesday of the contract month (usually Monday)
        if _basename in ['6N', '6S']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            for i in range(0,5): # 2nd wednesday
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # 6C -> Last trading day is on the business day immediately preceding the third Wednesday of the contract month (usually Tuesday).
        if _basename in ['6C']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_cme_month, _next_cme_year) # 3rd wednesday of that month of that year       
            for i in range(0,5):
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
            # If 19th is SUNDAY,oi shifts on 17th,so 16th would be the last trading date
            if weekday == 5: #If saturday
                date = date + datetime.timedelta( days=-2)
            elif weekday == 4: #If friday
                date = date + datetime.timedelta( days=-1)
            elif weekday == 6: #If sunday #TODO ### if 18th is sunday,how do they handle
                 date = date + datetime.timedelta( days=-2)
            return date

        # GC: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'GC':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,27):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # SI: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'SI':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,29):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # HG: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'HG':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,33):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # PL: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'PL':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,27):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # PA: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'PA':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,26):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # HO: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'HO':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,32):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # RB: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'RB':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,33):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # NG: Last trading day : Trading terminates on the third last business day of the delivery month.
        if _basename == 'NG':
            date = datetime.date(day=monthrange(_next_cme_year,_next_cme_month)[1],year=_next_cme_year,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2): # Last trading day
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,35):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # LH: Last trading day : 10th business day of the contract month
        if _basename == 'LH':
            date = datetime.date(year=_next_cme_year,day=1,month=_next_cme_month)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=1)
            for i in range(0,9): # 10th business day
                date = date + datetime.timedelta( days=1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=1)
            for i in range(0,25):
                date = date + datetime.timedelta( days=-1)
                while not self.is_cme_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        print 'UnHandled case: Using default'
        # Default day is 10th of month
        date = datetime.date(year=_next_cme_year,day=10,month=_next_cme_month)
        for i in range(0,1):
            date = date + datetime.timedelta( days=-1)
            while not self.is_cme_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
        return date

    def get_eurex_last_trading_date( self, _basename, _next_eurex_month, _next_eurex_year ):
        # Last trading day is 3rd friday of contract month 
        if _basename in ['FESX']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_eurex_month, _next_eurex_year) # 3rd friday of that month of that year
            for i in range(0,3): # 3rd tuesday usually
                date += datetime.timedelta(days=-1)
                while not self.is_eurex_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

        # Last trading day is 3rd friday of contract month 
        if _basename in ['FDAX','FSMI']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_eurex_month, _next_eurex_year) # 3rd friday of that month of that year
            for i in range(0,4): # 3rd monday usually
                date += datetime.timedelta(days=-1)
                while not self.is_eurex_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

        # Last trading day is 2 days before delivery day(10th of contract month), if this day is an exchange day; otherwise, the exchange day immediately succeeding that day.
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

        '''if _basename == 'LFL':
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
            return date'''

        # Last trading day : Two business days prior to the last business day in the delivery month
        # First notice day : Two business days prior to the first day of the delivery month
        if _basename == 'LFR':
            date = datetime.date(day=1, month=_next_liffe_month, year=_next_liffe_year)
            for i in range(0,6):
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

        # FTI : Last Trading day : the third Friday of the delivery month, provided this is a business day. If not,last business day preceding the third Friday
        if _basename in ['FTI']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_liffe_month, _next_liffe_year) # 3rd friday of that month of that year
            while not self.is_liffe_exchange_date(_basename, date):
                date += datetime.timedelta(days=-1)
            for i in range(0,1): # 3rd thursday usually
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

        # FES : Last trading day : Two business days prior to the third Wednesday of the delivery month
        if _basename in ['FES']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_liffe_month, _next_liffe_year) # 3rd wednesday of that month of that year
            for i in range(0,2): # last trading day
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            for i in range(0,0):
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
            for i in range(0,9): # Adjustment based on oi
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # CT: First Notice Day : Five business days before the first delivery day of the spot contract month, which is the first business day of that month.
        if _basename == 'CT':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            while not self.is_ice_exchange_date(_basename, date): # End of spot month
                date = date + datetime.timedelta( days=1 )
            for i in range(0,5):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,12): # Adjustment based on oi
                date = date + datetime.timedelta( days=-1 )
                while not self.is_ice_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # SB: First Notice day : First business day after last trading day.
        # Last trading day : Last business day of the month preceding the delivery month 
        if _basename == 'SB':
            date = datetime.date(day=1, month=_next_ice_month, year=_next_ice_year)
            while not self.is_ice_exchange_date(_basename, date):
                date = date + datetime.timedelta( days=1 ) # First notice day
            for i in range(0,17):
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

    def get_sgx_last_trading_date( self, _basename, _next_sgx_month, _next_sgx_year ):
        # Last trading day : The second last business day of the contract month 
        if _basename == 'SG':
            date = datetime.date(day=monthrange(_next_sgx_year,_next_sgx_month)[1], month=_next_sgx_month, year=_next_sgx_year)
            while not self.is_sgx_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,1): # last trading day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_sgx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,3):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_sgx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

        # Last Trading day : Last Thursday of the month. If this happens to fall on an India holiday, the last trading day shall be the preceding business day.
        if _basename == 'SIN':
            date = datetime.date(day=monthrange(_next_sgx_year,_next_sgx_month)[1], month=_next_sgx_month, year=_next_sgx_year)
            while date.weekday() != 3: # Last thursday
                date = date + datetime.timedelta( days=-1)
            while not self.is_sgx_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,3): # last trading day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_sgx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_hkfe_last_trading_date( self, _basename, _next_hkfe_month, _next_hkfe_year ):
        # Last trading day : The second last business day of the contract month 
        if _basename in ['HSI','HHI']:
            date = datetime.date(day=monthrange(_next_hkfe_year,_next_hkfe_month)[1], month=_next_hkfe_month, year=_next_hkfe_year)
            while not self.is_hkfe_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,1): # last trading day
                date = date + datetime.timedelta( days=-1 )
                while not self.is_hkfe_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            for i in range(0,3):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_hkfe_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_safex_last_trading_date( self, _basename, _next_safex_month, _next_safex_year ):
        # Last trading day : Third Thursday of the settlement month
        if _basename in ['ALSI']:
            date = self.get_date_from_nth_day_of_month_year(3, 'THURSDAY', _next_safex_month, _next_safex_year)
            while not self.is_safex_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,4):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_safex_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_asx_last_trading_date( self, _basename, _next_asx_month, _next_asx_year ):
        # Last trading day : Third Thursday of the settlement month
        if _basename in ['SPI']:
            date = self.get_date_from_nth_day_of_month_year(3, 'THURSDAY', _next_asx_month, _next_asx_year)
            while not self.is_asx_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_asx_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_krw_last_trading_date( self, _basename, _next_krw_month, _next_krw_year ):
        # Last trading day : Second Thursday of the contract month
        if _basename in ['KOSPI']:
            date = self.get_date_from_nth_day_of_month_year(2, 'THURSDAY', _next_krw_month, _next_krw_year)
            while not self.is_krw_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,2):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_krw_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_meff_last_trading_date( self, _basename, _next_meff_month, _next_meff_year ):
        # Last trading day : The third Friday of the expiration month
        if _basename in ['MFX']:
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_meff_month, _next_meff_year)
            while not self.is_meff_exchange_date( _basename, date ):
                date = date + datetime.timedelta( days=-1)
            for i in range(0,3):
                date = date + datetime.timedelta( days=-1 )
                while not self.is_meff_exchange_date( _basename, date ):
                    date = date + datetime.timedelta( days=-1)
            return date

    def get_tfx_last_trading_date( self, _basename, _next_tfx_month, _next_tfx_year ):
        # Last trading day : Two business days prior to the third Wednesday of the contract month
        if _basename in ['YEA']:
            date = self.get_date_from_nth_day_of_month_year(3, 'WEDNESDAY', _next_liffe_month, _next_liffe_year) # 3rd wednesday of that month of that year
            for i in range(0,2): # last trading day
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            for i in range(0,0):
                date += datetime.timedelta(days=-1)
                while not self.is_liffe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

    def get_cfe_last_trading_date( self, _basename, _next_cfe_month, _next_cfe_year ):
        # VIX Last trading day : The Wednesday that is thirty days prior to the third Friday of the calendar month immediately following the month in which the contract expires 
        #  If the third Friday of the month subsequent to expiration of the applicable VIX futures contract is a CBOE holiday, the Final Settlement Date for the contract shall be thirty days prior to the CBOE business day immediately preceding that Friday.
        if _basename in ['VX']:
            if _next_cfe_month == 12:
                _next_cfe_month = 1
                _next_cfe_year += 1
            else:
                _next_cfe_month += 1
            date = self.get_date_from_nth_day_of_month_year(3, 'FRIDAY', _next_cfe_month, _next_cfe_year)
            while not self.is_cfe_exchange_date(_basename, date):
                date += datetime.timedelta(days=-1)
            date += datetime.timedelta(days=-30)
            for i in range(0,3):
                date += datetime.timedelta(days=-1)
                while not self.is_cfe_exchange_date(_basename, date):
                    date += datetime.timedelta(days=-1)
            return date

    def get_cme_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ): 
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['ZT', 'ZF', 'ZN', 'ZB', 'ZW', 'ZC', 'ZS','ZM','ZL','GC','SI','HG','PA','PL','HO','RB','NG','LH']: # ZT, ZF, ZN, ZB: first notice date is the last day of the previous month,we expect to stop trading a day before
            if _current_min_last_trading_date_mm == 12: # TODO check CL added is correct or not
                _current_min_last_trading_date_mm = 1 
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1 
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # ES,NQ,YM,EMD,NIY,NKD,6A,6B,6C,6E,6J,6M,6N,6S   

    def get_eurex_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['FESX','FDAX','FSMI']:
            return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)
        if _basename in ['FGBS','FGBM','FGBL','FGBX']:
            if _current_min_last_trading_date_mm in [3,6,9,12]:
                return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy)
            else:
                if _current_min_last_trading_date_mm == 12: # Should not come here
                    _current_min_last_trading_date_mm = 1 
                    _current_min_last_trading_date_yyyy += 1
                else:
                    _current_min_last_trading_date_mm += 1 

    def get_liffe_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ):
        _current_month = _current_min_last_trading_date.month
        _current_year = _current_min_last_trading_date.year
        if _basename in ['LFR']:
            # Incrementing the Year is not required here, since the expiry based last trading date function will always return the correct Date from which we extract this expiry
            _current_month += 1
            if _current_month == 13:
                _current_month = 1
                _current_year += 1
        return _basename + self.month_codes[str(_current_month)] + self.get_yy(_current_year) # FES, FTI, LFZ 
        # TODO liffe has a different convention for exchange symbol,need to change it: use _this_local_trade_date        

    def get_tmx_symbol_from_last_trading_date ( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['CGB']:
            if _current_min_last_trading_date_mm == 12: 
                _current_min_last_trading_date_mm = 1
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1 
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # BAX, SXF

    def get_ice_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['KC','CT','SB','BRN']:
            if _current_min_last_trading_date_mm == 12:
                _current_min_last_trading_date_mm = 1
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1
        if _basename in ['CC']:
            if _current_min_last_trading_date_mm == 12:
                _current_min_last_trading_date_mm = 1
                _current_min_last_trading_date_yyyy += 1
            else:
                _current_min_last_trading_date_mm += 1
            if _current_min_last_trading_date_mm not in [3,5,7,9,12]:
                if _current_min_last_trading_date_mm == 12:
                    _current_min_last_trading_date_mm = 1
                    _current_min_last_trading_date_yyyy += 1
                else:
                    _current_min_last_trading_date_mm += 1
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # G

    def get_tse_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # TOPIX

    def get_ose_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # JGBL {check}, JNK

    def get_sgx_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # SG, SIN

    def get_hkfe_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # HHI, HSI

    def get_safex_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # SAFEX

    def get_asx_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # SPI

    def get_meff_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # MFX

    def get_krw_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # KOSPI

    def get_tfx_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # YEA

    def get_cfe_symbol_from_last_trading_date( self, _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        return _basename + self.month_codes[str(_current_min_last_trading_date_mm)] + self.get_yy(_current_min_last_trading_date_yyyy) # VX

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

    def is_sgx_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_hkfe_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_safex_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_asx_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_meff_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_krw_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_tfx_exchange_date( self, _basename, date ):
        return date.weekday() != 5 and date.weekday() != 6 # Should check exchange holidays

    def is_cfe_exchange_date( self, _basename, date ):
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

    def get_exchange_symbol_sgx( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_sgx_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_hkfe( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_hkfe_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_safex( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_safex_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_asx( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_asx_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_meff( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_meff_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_krw( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_krw_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_tfx( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_tfx_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

    def get_exchange_symbol_cfe( self, date, product ):
        _last_trading_date = self.get_last_trading_date( date, product )
        return self.get_cfe_symbol_from_last_trading_date ( self.get_basename(product), _last_trading_date )

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
