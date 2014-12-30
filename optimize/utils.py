import os
import sys
import ConfigParser
import subprocess
from shutil import copyfile
import pandas as pd

def parse_results( _results ):
    _results = filter( None, _results.split( '\n' ) )
    _ret = dict( [ ( stat.split('=')[0].strip(' '), float( stat.split('=')[1].strip(' %') ) ) for stat in _results ] )
    return _ret

def get_logreturns( _config_file, _trade_products, _start_date, _end_date, _returns_data_filename ):
    #Make a new config with indicators as logreturns and rest of the info same
    _new_config_file = os.path.split( _config_file )[0] + '/temp.cfg'
    copyfile( _config_file, _new_config_file )
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _new_config_file, 'r' ) )
    s = 'DailyLogReturns.' + (' DailyLogReturns.').join(_trade_products)
    _config.set("DailyIndicators", "names", s)
    with open(_new_config_file, 'wb') as configfile:
        _config.write(configfile)
    # Run gendata on the new config to generate the indicator file
    proc = subprocess.Popen(['python', 'gendata.py', _new_config_file, _start_date, _end_date, _returns_data_filename ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.communicate()
    os.remove(_new_config_file)
    df = pd.DataFrame.from_csv(_returns_data_filename)
    products_order = [ symbol.split('.')[1] for symbol in df.columns ]
    return ( df.values, products_order )
    
# Write the new weights to the config file
def change_weights( _config_file, _trade_products, _w ):
    s=''
    for i in range(0,len(_trade_products)):
        s = s + _trade_products[i] + ',' + str(_w[i]) + ' '
    s.rstrip(' ')
    _config = ConfigParser.ConfigParser()
    _config.readfp( open( _config_file, 'r' ) )
    _config.set("Strategy", "weights", s)
    with open( _config_file, 'wb') as configfile:
        _config.write(configfile)
