# cython: profile=True
import os
import sys
import re
import json
import cPickle
import ConfigParser
from datetime import datetime
from os.path import expanduser
import numpy
from numpy import hstack, vstack
from cvxopt import matrix, solvers
from cvxopt.solvers import qp
from vector_ops import shift_vec
import defaults
from global_variables import Globals

# Returns the full list of products  with fES_1 and fES_2 treated separately
# Products which are not traded but have indicators based on them are also included 
def get_all_products(_trade_products): 
    return get_real_traded_products( _trade_products )

def get_all_trade_products(_config):
    _trade_products = []
    if _config.has_option('Strategy', 'signal_configs'):
        _signal_configs = [adjust_file_path_for_home_directory(x) for x in _config.get('Strategy', 'signal_configs').split(',')]
        for _config_name in _signal_configs:
            _signal_config = ConfigParser.ConfigParser()
            _signal_config.readfp(open(_config_name, 'r'))
            _signal_products = get_trade_products(_signal_config)
            _trade_products.extend(_signal_products)
        _trade_products = sorted(list(set(_trade_products)))
        return _trade_products
    else:
        sys.exit('No signal configs')        

def get_trade_products(_config):
    _trade_products = []
    if _config.has_option('Products', 'trade_products'):
        _product_files = [adjust_file_path_for_home_directory(x) for x in _config.get('Products', 'trade_products').split(',')]
        for _product_file in _product_files:
            with open (_product_file, "r") as myfile:
                _products = myfile.read()
            _products = filter(None, _products.split('\n'))
            _trade_products.extend(_products)
        _trade_products = sorted(list(set(_trade_products)))
        return _trade_products
    else:
        sys.exit('No trade products in signal config')

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
    returns1 = numpy.array([item[1] for item in dates_returns_1]).astype(float)
    returns2 = numpy.array([item[1] for item in dates_returns_2]).astype(float)
    all_dates = [dates1,dates2]
    all_series = [returns1,returns2]
    intersected_dates = list(set(all_dates[0]).intersection(*all_dates))
    intersected_dates.sort()
    filtered_series = []
    for i in xrange(0,len(all_series)):
        Indexes = numpy.sort(numpy.searchsorted(all_dates[i],intersected_dates))
        filtered_series.append(all_series[i][Indexes])
    filtered_series = (numpy.array(filtered_series).T).astype(float)
    return (filtered_series[:,0],filtered_series[:,1])
    
def adjust_file_path_for_home_directory(file_path):
    """Give a path like ~/qplummodels/UnleveredDMF/model1.txt, change it to /home/cvdev/qplummodels/UnleveredDMF/model1.txt etc depending on the home directory of the user
    
    """
    return expanduser(file_path)

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

def get_dates_returns(filename):
    _file = open(filename,'rb')
    dates = cPickle.load(_file)
    returns = cPickle.load(_file)
    return dates, returns

def is_float_zero(val):
    return abs(val) < 0.000000001

def adjust_to_desired_l1norm_range(given_weights, minimum_leverage=0.001, maximum_leverage=100):
    """adjusts the given weights to the desired leverage range

    Arguments:
    given_weights(1d numpy array - float)
    minimum_leverage(float)
    maximum_leverage(gloat)
    
    Returns:
    1d numpy array of floats, which is a scaled version of the given weights to have l1norm or leverage in the desired region
    
    Raises:
    ValueError: if (minimum_leverage < 0.001) or (minimum_leverage > maximum_leverage
    """
    if (minimum_leverage < 0.001) or (minimum_leverage > maximum_leverage):
        raise ValueError("minimum leverage %f should be greater than 0.001 and maximum leverage %f should be greater than minimum leverage" %(minimum_leverage, maximum_leverage))
    given_leverage = numpy.sum(numpy.abs(given_weights))
    if (given_leverage > 0.001): # a very hacky way of checking divide by 0 problem !
        if given_leverage < minimum_leverage:
            given_weights = given_weights * (minimum_leverage / given_leverage)
        elif given_leverage > maximum_leverage:
            given_weights = given_weights * (maximum_leverage / given_leverage)
    return (given_weights)

def init_logs(_config, _log_dir, products):
    if not os.path.exists(_log_dir):
        os.makedirs(_log_dir)
    Globals.debug_level = defaults.DEBUG_LEVEL
    if _config.has_option('Parameters', 'debug_level'):
        Globals.debug_level = _config.getint('Parameters','debug_level')
    if Globals.debug_level > 0:
        Globals.positions_file = open(_log_dir + '/positions.txt', 'w')
    if Globals.debug_level > 1:
        Globals.leverage_file = open(_log_dir + '/leverage.txt', 'w')
        Globals.weights_file = open(_log_dir + '/weights.txt', 'w')
        Globals.leverage_file.write('date,leverage\n')
        Globals.weights_file.write('date,%s\n' % ( ','.join(products)))
    if Globals.debug_level > 2:
        Globals.amount_transacted_file = open(_log_dir + '/amount_transacted.txt', 'w')
        Globals.amount_transacted_file.write('date,amount_transacted\n')
    Globals.returns_file = open(_log_dir + '/returns.txt', 'wb')
    Globals.stats_file = open(_log_dir + '/stats.txt', 'w')   
    
def efficient_frontier(expected_returns, covariance, max_leverage, risk_tolerance, max_allocation=0.5):
    """ Function that calculates the efficient frontier
        by minimizing
        (Variance - risk tolerance * expected returns)
        with a given leverage and risk tolerance
        
        Args:
            returns(matrix) - matrix containing log daily returns for n securities
            covariance(matrix) - matrix containing covariance of the n securities
            max_leverage(float) - maximum value of levarage
            risk_tolerance(float) - parameter of risk tolrance used in MVO
            max_allocation(float) - maximum weight to be allocated to one security
                                    (set low threshold to diversify)
        Returns:
            portfolios(array) - portfolio of optimal weights
    """
    num_prods = expected_returns.shape[0]  # Number of products

    # Setup inputs for optimizer
    # Here b is the weight vector we are looking for
    # d is the expected returns vector
    # D is the covariance marix
    # A is the matrix of different constraints
    # min(-d^T b + 1/2 b^T D b) with the constraints A^T b <= b_0
    # Constraint: sum(abs(weights)) <= leverage is non-linear
    # To make it linear introduce a dummy weight vector y = [y1..yn]
    # w1<y1,-w1<y1,w2<y2,-w2<y2,...
    # y1,y2,..,yn > 0
    # y1 + y2 + ... + yn <= leverage
    # Optimization will be done to find both w and y i.e n+n weights
    # dmat entries for y kept low to not affect minimzing function as much as possible
    # Not kept 0 to still keep Dmat as semi-definite
    dummy_var_dmat = 0.000001*numpy.eye(num_prods)
    dmat = vstack((hstack((covariance, matrix(0., (num_prods, num_prods)))), hstack((matrix(0., (num_prods, num_prods)), dummy_var_dmat))))
    # Constraint:  y1 + y2 + ... + yn <= leverage
    amat = vstack((matrix(0, (num_prods, 1)), matrix(1, (num_prods, 1))))
    bvec = [max_leverage]
    # Constraints:   y1, y2 ,..., yn >= 0
    amat = hstack((amat, vstack((matrix(0, (num_prods, num_prods)), -1*numpy.eye(num_prods)))))
    bvec = bvec + num_prods*[0]
    # Constraints:  y1, y2 ,..., yn <= max_allocation
    amat = hstack((amat, vstack((matrix(0, (num_prods, num_prods)), numpy.eye(num_prods)))))
    bvec = bvec + num_prods*[max_allocation]
    # Constraints:  -w1 <= y1, -w2 <= y2, ..., -wn <= yn
    dummy_wt_constraint1 = [-1] + (num_prods-1)*[0] + [-1] + (num_prods-1)*[0]
    amat = hstack((amat, shift_vec(dummy_wt_constraint1, num_prods)))
    bvec = bvec + num_prods*[0]
    # Constraints:  w1 <= y1, w2 <= y2, ..., wn <= yn
    dummy_wt_constraint2 = [1] + (num_prods-1)*[0] + [-1] + (num_prods-1)*[0]
    amat = hstack((amat, shift_vec(dummy_wt_constraint2, num_prods)))
    bvec = bvec + num_prods*[0]
    # Convert all NumPy arrays to CVXOPT matrics
    dmat = matrix(dmat)
    bvec = matrix(bvec, (len(bvec), 1))
    amat = matrix(amat.T)
    dvec = matrix(hstack((expected_returns, num_prods*[0])).T)
    # Optimize
    solvers.options['show_progress'] = False
    portfolios = qp(dmat, -1*risk_tolerance*dvec, amat, bvec)['x']
    return portfolios[0:num_prods].T

def read_all_config_params_to_dict(_config):
    data = {}
    for _section in _config.sections():
        data[_section] = {}
        for _var in _config.options(_section):
            data[_section][_var] = _config.get(_section, _var)
    return data

def read_all_txt_params_to_dict(txt_file):
    data = {}
    with open(txt_file, 'r') as file_handle:
        for line in file_handle:
            words = line.split(' ')
            data[words[0]] = words[1:]
    return data

def dump_sim_to_json(config_file, returns, stats):
    data = {}
    config = ConfigParser.ConfigParser() # Read aggregator config
    config.optionxform = str
    config.readfp(open(config_file, 'r'))
    data = read_all_config_params_to_dict(config)
    data['config_name'] = config_file
    data['returns'] = returns
    data['stats'] = stats
    data['Strategy']['signal_configs'] = data['Strategy']['signal_configs'].split(',')
    signal_configs = data['Strategy']['signal_configs']
    risk_profile_path = data['RiskManagement']['risk_profile'].replace("~", os.path.expanduser("~"))
    data['RiskManagement']['risk_file'] = read_all_txt_params_to_dict(risk_profile_path)
    for i in range(len(signal_configs)):
        signal_config_path = signal_configs[i].replace("~", os.path.expanduser("~"))
        config = ConfigParser.ConfigParser() # Read aggregator config
        config.optionxform = str
        config.readfp(open(signal_config_path, 'r'))
        signal_label = 'signal %d' % (i+1)
        data[signal_label] = read_all_config_params_to_dict(config)
        paramfilepath = data[signal_label]['Parameters']['paramfilepath'].replace("~", os.path.expanduser("~"))
        modelfilepath = data[signal_label]['Strategy']['modelfilepath'].replace("~", os.path.expanduser("~"))
        data[signal_label]['Parameters']['params'] = read_all_txt_params_to_dict(paramfilepath)
        data[signal_label]['Strategy']['model'] = read_all_txt_params_to_dict(modelfilepath)
        data[signal_label]['Products']['trade_products'] = data[signal_label]['Products']['trade_products'].split(',') 
    print json.dumps(data)      
