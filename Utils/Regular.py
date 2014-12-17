import sys
import re
from datetime import datetime
import numpy as np

# Returns the full list of products  with fES_1 and fES_2 treated separately
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
            _real_traded_products.extend([get_first_futures_contract(product),get_second_futures_contract(product)]) 
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
def get_dt_from_date( date ):
    return datetime.combine( datetime.strptime( date, "%Y-%m-%d" ).date(), datetime.max.time() )

#Check whether all events in the list are ENDOFDAY events
def check_eod( events ):
    ret = True
    for event in events:
        if event['type']!='ENDOFDAY' : ret = False
    return ret

def parse_weights( wts ):
    weights = {}
    for wt in wts.split(' '):
        symbol = wt.split(',')[0]
        weight = float( wt.split(',')[1] )
        weights[symbol] = weight
    return weights

#Return true if the symbol is of a futures contract
def is_future( product ):
    return product[0] == 'f'

# Return true if product is a future entity like 'fES'
def is_future_entity( product ):
    return is_future(product) and '_' not in product # If prefixed by 'f' and '_' is not present in symbol,then it is a future entity

def get_base_symbol( product ):
    return product.rsplit('_',1)[0]

#Given a future entity symbol like fES, returns the symbol of the first futures contract like fES_1
def get_first_futures_contract( _base_symbol ):
    return _base_symbol + '_1'

def get_second_futures_contract( _base_symbol ):
    return _base_symbol + '_2'

def get_future_contract_number(product):
    return int(product.rsplit('_',1)[1])

# Given a futures contract symbol,return the symbol of the next futures contract
def get_next_futures_contract( product ):
    _base_symbol = get_base_symbol(product)
    num = get_future_contract_number(product) #num is the number at the end of a symbol.EG:1 for fES_1
    _next_contract_symbol = _base_symbol + '_' + str( num + 1 )     
    return _next_contract_symbol

# Given a futures contract symbol,return the symbol of the previous futures contract
def get_prev_futures_contract( product ):
    _base_symbol = get_base_symbol(product)
    num = get_future_contract_number(product) #num is the number at the end of a symbol.EG:1 for fES_1
    _prev_contract_symbol = _base_symbol + '_' + str( num - 1 )
    return _prev_contract_symbol

def get_future_mappings( all_products ):
    _base_symbols = list( set ( [ get_base_symbol( product ) for product in all_products if is_future( product ) ] ) )
    future_mappings = dict( [ ( symbol, [] ) for symbol in _base_symbols ] )
    for product in all_products:
        if is_future( product ):
            future_mappings[ get_base_symbol( product ) ].append( product )
    return future_mappings
    
def shift_future_symbols(_dictionary, future_contracts):
    _new_dictionary = dict([(product, 0) for product in future_contracts]) 
    for _product in future_contracts:
        _prev_futures_contract = get_prev_futures_contract(_product)
        if _prev_futures_contract in _dictionary.keys():
            _new_dictionary[_prev_futures_contract] = _dictionary[_product]
    for _product in future_contracts:
        _dictionary[_product] = _new_dictionary[_product]

def is_margin_product(product):
    '''Returns True if the product requires margin to be posted and not all cash'''
    return is_future(product)

def filter_series(dates_returns_1,dates_returns_2):
    dates1 = [item[0] for item in dates_returns_1]
    dates2 = [item[0] for item in dates_returns_2]
    returns1 = np.array([item[1] for item in dates_returns_1]).astype(float)
    returns2 = np.array([item[1] for item in dates_returns_2]).astype(float)
    all_dates = [dates1,dates2]
    all_series = [returns1,returns2]
    intersected_dates = list(set(all_dates[0]).intersection(*all_dates))
    intersected_dates.sort()
    filtered_series = []
    for i in xrange(0,len(all_series)):
        Indexes = np.sort(np.searchsorted(all_dates[i],intersected_dates))
        filtered_series.append(all_series[i][Indexes])
    filtered_series = (np.array(filtered_series).T).astype(float)
    return (filtered_series[:,0],filtered_series[:,1])
    
def adjust_file_path_for_home_directory(file_path):
    """Give a path like HOME/qplummodels/UnleveredDMF/model1.txt, change it to /home/cvdev/qplummodels/UnleveredDMF/model1.txt etc depending on the home directory of the user
    
    """
    #TODO
    return (file_path)

def get_weights_for_trade_products(_trade_products, _weights):
    _trade_products_weights = dict([(_product, 0.0) for _product in _trade_products])
    for _product in _weights.keys():
        if _product in _trade_products:
            _trade_products_weights[_product] += _weights[_product]
        elif is_future(_product):
            _trade_products_weights[get_base_symbol(_product)] += _weights[_product]
        else:
            sys.exit('Product specification in configs inconsistent')
    return _trade_products_weights

def dict_to_string(_dict):
    _str = ''
    for key in sorted(_dict.keys()):
        _str += '%s : %0.2f   ' % (key, _dict[key])
    return _str
