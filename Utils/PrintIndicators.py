import ConfigParser
import pickle
import os
from Utils.Regular import get_dt_from_date
from importlib import import_module
from DailyIndicators.Indicator_Listeners import IndicatorListener

class PrintIndicators(IndicatorListener):

    instance=[]

    def __init__(self,products,config_file):
        self.directory = 'Data/'
        self.indicators_file = self.directory+'print_indicators_'+os.path.splitext(config_file)[0].split('/')[-1]+'.txt'
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.start_date = get_dt_from_date(config.get( 'Dates', 'start_date' )).date()
        self.end_date = get_dt_from_date(config.get( 'Dates', 'end_date' )).date()
        self.date = self.start_date    
        self.products=products
        self.indicator_values={}
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        # Read indicator list from config file
        indicators = config.get( 'DailyIndicators', 'names' ).strip().split(" ")
        self.identifiers = sorted(indicators)

        #Instantiate daily indicator objects
        for indicator in indicators:
            indicator_name = indicator.strip().split('.')[0]    
            module = import_module('DailyIndicators.'+indicator_name)
            Indicatorclass = getattr(module,indicator_name)
            indicator_instance = Indicatorclass.get_unique_instance(indicator,config_file) 
            indicator_instance.add_listener(self)
            self.indicator_values[indicator]=0 # Default value for each indicator

        s='Date\t' + '\t'.join(self.identifiers)
        self.write_data(self.indicators_file,s,'w') # Write the header to the file

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(PrintIndicators.instance)==0):
            new_instance = PrintIndicators(products,config_file)
            PrintIndicators.instance.append(new_instance)
        return PrintIndicators.instance[0]

    def print_indicators_readable_format(self):
        s = str(self.date) 
        for identifier in self.identifiers:
            s = s + '\t' + str(self.indicator_values[identifier])
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
        current_date = indicator_value[0]
        if(self.date < current_date):
            self.print_indicators_readable_format() 
            #print_indicators_pickle_format()
            self.date = current_date 
        self.indicator_values[identifier]=indicator_value[1]
