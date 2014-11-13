#!/usr/bin/env python
import sys
import datetime
from calendar import monthrange

month_codes = { '1' : 'F', '2' : 'G', '3' : 'H', '4' : 'J', '5' : 'K',  '6' : 'M', '7' : 'N', '8' : 'Q', '9' : 'U', '10' : 'V', '11' : 'X', '12' : 'Z' }
day_codes = { 'SUNDAY': 6, 'MONDAY': 0, 'TUESDAY': 1, 'WEDNESDAY': 2, 'THURSDAY': 3, 'FRIDAY': 4, 'SATURDAY': 5 }

def get_date_from_nth_day_of_month_year(n, day, month, year):
    n -= 1
    day = day_codes[day]
    begining_of_month_day, num_days_in_month = monthrange(year, month)
    firstmatch = (day - begining_of_month_day) % 7 + 1
    day = xrange(firstmatch, num_days_in_month + 1, 7)[n]
    return datetime.date(year=year,day=day,month=month)

class ExchangeSymbolManager():
    def __init__( product, date ):
        self.product = product
        self.date = date
        self.month_codes = { '1' : 'F', '2' : 'G', '3' : 'H', '4' : 'J', '5' : 'K',  '6' : 'M', '7' : 'N', '8' : 'Q', '9' : 'U', '10' : 'V', '11' : 'X', '12' : 'Z' }
        self.day_codes = { 'SUNDAY': 6, 'MONDAY': 0, 'TUESDAY': 1, 'WEDNESDAY': 2, 'THURSDAY': 3, 'FRIDAY': 4, 'SATURDAY': 5 }

    def get_basename( product ):
        return product.rsplit('_',1)[0]

    def get_contract_number( product ):
        return int( product.rsplit('_',1)[1] )       

    def get_yy( yyyy ):
        yy = yyyy%100
        if yy < 10:
            return '0' + str(yy)
        else:
            return str(yy)

    def get_date_from_nth_day_of_month_year(n, day, month, year):
        n -= 1
        day = self.day_codes[day]
        begining_of_month_day, num_days_in_month = monthrange(year, month)
        firstmatch = (day - begining_of_month_day) % 7 + 1
        day = xrange(firstmatch, num_days_in_month + 1, 7)[n]
        return datetime.date(year=year,day=day,month=month)

    def is_cme_month( _basename, _this_month ):
        if _basename == "CL" :
            return True
        if _basename == "GC" : 
            return ( ( _this_month + 1 ) % 2 ) == 0
        if _basename == "IBV":
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
        return ( _this_month % 3 ) == 0 
        
    def get_cme_last_trading_date( _basename, _next_cme_month, _next_cme_year ):
        #ES, NQ, YM
        #Settlement Date: The 3rd Friday of IMM month
        #Last Trading Day: Same as Settlement Date
        #OBSERVATION: the volume shifts towards the next future contract one week before i.e 2nd Friday
        if _basename in ['ES', 'NQ', 'NKD', 'NIY', 'YM']:
            pass        
        if _basename in ['ZT', 'ZF', 'ZN', 'ZB', 'UB', 'ZW', 'ZC', 'ZS', 'XW']: 
            pass
        if _basename in ['6A', '6B', '6E', '6J', '6M', '6N', '6S', '6C']:
            pass
        if _basename== '6R': #To verify
            pass
        
    def set_to_next_cme_month( _basename, _next_cme_month, _next_cme_year ):
        if _basename == 'IBV':
            if _next_cme_month == 12:
                _next_cme_month = 1
                _next_cme_year += 1 
                while not is_cme_month( _basename, _next_cme_month ):
                    _next_cme_month += 1
            else:
                _next_cme_month += 1 
                while not is_cme_month( _basename, _next_cme_month ):
                    _next_cme_month += 1
            return ( _next_cme_month, _next_cme_year )
        if _basename == 'CL':
            if _next_cme_month  == 12:
                _next_cme_month = 1
                _next_cme_year += 1
            else:
                _next_cme_month += 1
            return ( _next_cme_month, _next_cme_year )
        if _basename == 'GC':
            if _next_cme_month  >= 12:      
                _next_cme_month = 1
                _next_cme_year += 1
            else:
                _next_cme_month += 2
            if _next_cme_month  > 12:
                _next_cme_month = 1
                _next_cme_year += 1
            return ( _next_cme_month, _next_cme_year )
        if _basename == 'ZS':
            if _next_cme_month  >= 11:
                _next_cme_month = 1
                _next_cme_year += 1
            else:
                _next_cme_month += 1
                while not is_cme_month( _basename, _next_cme_month ):
                    _next_cme_month += 1
            return ( _next_cme_month, _next_cme_year )
        if _next_cme_month  == 12:      
            _next_cme_month = 3
            _next_cme_year += 1     
            while not is_cme_month( _basename, _next_cme_month ):
                _next_cme_month += 1
        else:
            _next_cme_month += 1
            while not is_cme_month( _basename, _next_cme_month ):
                _next_cme_month += 1
        return ( _next_cme_month, _next_cme_year )

    def get_cme_symbol_from_last_trading_date ( _basename, _current_min_last_trading_date ):
        _current_min_last_trading_date_mm = _current_min_last_trading_date.month
        _current_min_last_trading_date_yyyy = _current_min_last_trading_date.year
        if _basename in ['ZT', 'ZF', 'ZN', 'ZB', 'UB', 'ZW', 'ZC', 'ZS', 'XW']: # ZT, ZF, ZN, ZB, UB : first notice date is the last day of the previous month,we expect to stop trading a day before
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
        return _basename + self.month_code[str(current_min_last_trading_date_mm)] + get_yy(_current_min_last_trading_date_yyyy)    
        
    def get_exchange_symbol_cme( date, product ):
        _basename = get_basename( product )
        _contract_number = get_contract_number( product )
        _current_min_last_trading_date = date
        _current_month = date.month
        _current_year = date.year
        while( _contract_number > 0):
            if is_cme_month( _basename, _current_month ):
                _next_cme_month = _current_month
                _next_cme_year = _current_year
                _next_last_trading_date = get_cme_last_trading_date( _basename, _next_cme_month, _next_cme_year )
                if _next_last_trading_date < _current_min_last_trading_date:
                    _next_cme_month,_next_cme_year = set_to_next_cme_month( _basename, _next_cme_month, _next_cme_year )
                    _next_last_trading_date = get_cme_last_trading_date( _basename, _next_cme_month, _next_cme_year )
                _current_min_last_trading_date = _next_last_trading_date
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            else:
                _next_cme_month = _current_month
                _next_cme_year = _current_year    
                _next_cme_month,_next_cme_year = set_to_next_cme_month( _basename, _next_cme_month, _next_cme_year )
                _next_last_trading_date = get_cme_last_trading_date( _basename, _next_cme_month, _next_cme_year )
                if _next_last_trading_date < _current_min_last_trading_date:
                    _next_cme_month,_next_cme_year = set_to_next_cme_month( _basename, _next_cme_month, _next_cme_year )
                    _next_last_trading_date = get_cme_last_trading_date( _basename, _next_cme_month, _next_cme_year )
                else:
                    pass
                _current_min_last_trading_date = _next_last_trading_date
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            if _contract_number > 0:
                _current_min_last_trading_date = 1#TODO
                _current_month = _current_min_last_trading_date.month
                _current_year = _current_min_last_trading_date.year
            _contract_number = _contract_number - 1
        return get_cme_symbol_from_last_trading_date ( _basename_, _current_min_last_trading_date )

def __main__() :
    if len( sys.argv ) > 1:
        #product = sys.argv[1]
        #date = sys.argv[2]
        n = int(sys.argv[1])
        day = sys.argv[2]
        month = int(sys.argv[3])
        year = int(sys.argv[4])
        print get_date_from_nth_day_of_month_year(n, day, month, year)
        #exchange_symbol_manager = ExchangeSymbolManager( product, date )
    else:
        print 'python exchange_symbol_manager.py product date'
        sys.exit(0)

if __name__ == '__main__':
    __main__();
