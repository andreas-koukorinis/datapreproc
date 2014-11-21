import sys
import pickle
import os
from importlib import import_module

from Utils.Regular import get_dt_from_date
from DailyIndicators.Indicator_Listeners import IndicatorListener

class PrintIndicators(IndicatorListener):

    instance=[]

    def __init__(self, products, _startdate, _enddate, _indicator_file, _config):
        self.directory = 'Data/'
        self.indicators_file = _indicator_file
        self.start_date = get_dt_from_date(_startdate).date()
        self.end_date = get_dt_from_date(_enddate).date()
        self.date = self.start_date
        self.products=products
        self.indicator_values={}
        self.update_flag = {}
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        # Read indicator list from config file
        indicators = _config.get( 'DailyIndicators', 'names' ).strip().split(" ")
        self.identifiers = sorted(indicators)

        #Instantiate daily indicator objects
        for indicator in indicators:
            self.update_flag[indicator] = False
            indicator_name = indicator.strip().split('.')[0]
            module = import_module( 'DailyIndicators.' + indicator_name )
            Indicatorclass = getattr( module, indicator_name )
            indicator_instance = Indicatorclass.get_unique_instance( indicator, _startdate, _enddate, _config )
            indicator_instance.add_listener(self)
            self.indicator_values[indicator]=0 # Default value for each indicator

        s='Date,' + ','.join(self.identifiers)
        self.write_data(self.indicators_file,s,'w') # Write the header to the file

    @staticmethod
    def get_unique_instance( products, _startdate, _enddate, _indicator_file, _config):
        if(len(PrintIndicators.instance)==0):
            new_instance = PrintIndicators(products, _startdate, _enddate, _indicator_file, _config )
            PrintIndicators.instance.append(new_instance)
        return PrintIndicators.instance[0]

    def print_indicators_readable_format(self):
        s = str(self.date)
        for identifier in self.identifiers:
            if not self.update_flag[identifier] and identifier.split('.')[0]=='DailyLogReturns': # If DailyLogReturns indicaqtor is not updated,then use 0 as dummy value
                s = s + ',0.0'
            else: # For other indicators use previous value
                s = s + ',' + str(self.indicator_values[identifier])
        self.write_data(self.indicators_file,s,'a') # Write one line of indicator values for a particular date

    #Save list of Date,IndicatorValues to a file in 'directory' for direct loading later
    def print_indicators_pickle_format(indicator_instances,directory,start_dt):
        for key in indicator_instances.keys():
            filename = key.replace(',','-')
            instance = indicator_instances[key]
            values = [item for item in instance.values if item[0]>=start_dt.date()]
            with open(directory+filename,'wb') as f:
                pickle.dump(values,f)

    def write_data(self,filename,data,mode):
        f=open(self.indicators_file,mode)
        f.write(data+'\n')
        f.close()

    def on_indicator_update(self,identifier,indicator_value):
        if(type(indicator_value) is list):
            indicator_value=indicator_value[-1] # Some indicators return full history of values as list,others return only the most recent value
        current_date = indicator_value[0]
        if(self.date < current_date):
            self.print_indicators_readable_format()
            #print_indicators_pickle_format()
            self.date = current_date
            for _identifier in self.identifiers:
                self.update_flag[_identifier] = False
        self.update_flag[identifier] = True
        self.indicator_values[identifier]=indicator_value[1]
