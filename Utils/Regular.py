import sys
from datetime import datetime
import re

# Returns the full list of products  with fES1 and fES2 trated separately
# Products which are not traded but have indicators based on them are also included 
def get_all_products( _config ):
    _trade_products = _config.get( 'Products', 'trade_products' ).split(',')
    _real_traded_products = get_real_traded_products( _trade_products )
    if _config.has_option('Products', 'mappings'):
        _mappings = _config.get( 'Products', 'mappings' ).split(' ') 
        _mapped_products = get_mapped_products( _mappings )
    else: 
        _mapped_products = []
    return list( set( _real_traded_products ) | set( _mapped_products ) ) #Take union of two lists

# Returns a list of products replacing each future contract by 1st and 2nd future contract in 'traded_products'
def get_real_traded_products( _traded_products ):
    _real_traded_products = []
    for product in _traded_products:
        if is_future_entity( product ):
            _real_traded_products.extend([product+'1',product+'2']) 
        else:
            _real_traded_products.append(product)
    return _real_traded_products

# Returns a list of products generated using the mapping in the config file
def get_mapped_products( _mappings ):
    _mapped_products = []
    for _mapping in _mappings:
        _map = _mapping.split(',')
        _base_name = _map[0]
        _aux_names = _map[1:]
        _mapped_products.extend( [ _base_name+_aux_name for _aux_name in _aux_names ] )    
    return _mapped_products

#Return a datetime object  given a date
#This function is used to get timestamp for end of day events
#ASSUMPTION : All end of day events for a particular date occur at the same time i.e. HH:MM:SS:MSMS -> 23:59:59:999999
def get_dt_from_date(date):
    return datetime.combine(datetime.strptime(date, "%Y-%m-%d").date(),datetime.max.time())

#Check whether all events in the list are ENDOFDAY events
def check_eod(events):
    ret = True
    for event in events:
        if(event['type']!='ENDOFDAY'): ret = False
    return ret

# Return true if product is a future entity like 'fES'
def is_future_entity( product ):
    return product[0] == 'f' and product[-1] not in range(0,10)
