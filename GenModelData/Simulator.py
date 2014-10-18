#!/usr/bin/env python
import os
import sys
import pickle
import ConfigParser
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from BookBuilder.BookBuilder import BookBuilder
from Utils.Regular import get_dt_from_date

#Print Date,IndicatorValues to a file in 'directory'
def print_indicators_readable_format(indicator_instances,directory,start_dt):
    if not os.path.exists(directory):
        os.makedirs(directory)
    for key in indicator_instances.keys():
        filename = key.replace(',','-')
        instance = indicator_instances[key]
        values = [item for item in instance.values if item[0]>=start_dt.date()] 
        f=open(directory+filename+'.txt','w')
        for item in values:
            f.write("%s %s\n"%(item[0],item[1])) 
        f.close()

#Save list of Date,IndicatorValues to a file in 'directory' for direct loading later
def print_indicators_pickle_format(indicator_instances,directory,start_dt):
    if not os.path.exists(directory):
        os.makedirs(directory)
    for key in indicator_instances.keys():
        filename = key.replace(',','-')
        instance = indicator_instances[key]
        values = [item for item in instance.values if item[0]>=start_dt.date()]
        with open(directory+filename,'wb') as f:
            pickle.dump(values,f)

def __main__() :
    # Command to run : python -W ignore Simulator.py config_file
    # Example        : python -W ignore Simulator.py test/config.cfg

    if len ( sys.argv ) < 2 :
        print "arguments <trading-startdate trading-enddate>"
        sys.exit(0)
    # Get handle of config file
    config_file = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file,'r'))

    # Read product list from config file
    products = config.get( 'Products', 'symbols' ).strip().split(",")
    # If there is fES1 ... make sure fES2 is also there, if not add it

    start_date = config.get('Dates','start_date')
    start_dt = get_dt_from_date(start_date)

    directory = config.get( 'Parameters', 'directory' )

    indicators = config.get( 'Indicators', 'names' ).strip().split(" ")
    indicator_instances={}
    for indicator in indicators:
        indicator_list = indicator.split(',')
        indicator_name = indicator_list[0]
        indicator_product = indicator_list[1]
        if(len(indicator_list)>2):
            indicator_params = indicator_list[2:]
        else:
            indicator_params = []
        indicator_params = [float(i) for i in indicator_params] # Assuming that all the parameters for an indicator will be float
        module = import_module ( 'Indicators.' + indicator_name )  # Import the module corresponding to the indicator name
        IndicatorClass = getattr ( module, indicator_name )  # Get the indicator class from the imported module
        if(len(indicator_params)>0):
            instance = IndicatorClass.get_unique_instance(indicator_product,indicator_params,config_file)
        else:
            instance = IndicatorClass.get_unique_instance(indicator_product,config_file)
        indicator_instances[indicator]=instance

    # Initialize Dispatcher using products list
    _dispatcher = Dispatcher.get_unique_instance( products, config_file )

    # Run the dispatcher to start the Indicator data generation
    _dispatcher.run()

    print_indicators_readable_format(indicator_instances,directory,start_dt)
    print_indicators_pickle_format(indicator_instances,directory,start_dt)

__main__();
