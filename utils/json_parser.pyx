import os
import sys
import json
import ConfigParser


class JsonParser():

    def read_all_config_params_to_dict(self, config):
        data = {}
        for _section in config.sections():
            data[_section] = {}
            for _var in config.options(_section):
                data[_section][_var] = config.get(_section, _var)
        return data

    def read_all_txt_params_to_dict(self, txt_file):
        data = {}
        with open(txt_file, 'r') as file_handle:
            for line in file_handle:
                words = line.split(' ')
                data[words[0]] = words[1:]
        return data

    def sim_to_json(self, config_file, dates, returns, leverage, stats):
        data = {}
        config = ConfigParser.ConfigParser() # Read aggregator config
        config.optionxform = str
        config.readfp(open(config_file, 'r'))
        data = self.read_all_config_params_to_dict(config)
        data['config_name'] = config_file
        data['daily_stats'] = []
        for _date_idx in range(len(dates)):
            data['daily_stats'].append({'date': str(dates[_date_idx]), 'log_return': returns[_date_idx], 'leverage': leverage[_date_idx]})
        data['stats'] = stats
        data['Strategy']['signal_configs'] = data['Strategy']['signal_configs'].split(',')
        signal_configs = data['Strategy']['signal_configs']
        risk_profile_path = data['RiskManagement']['risk_profile'].replace("~", os.path.expanduser("~"))
        data['RiskManagement']['risk_file'] = self.read_all_txt_params_to_dict(risk_profile_path)
        data['signals'] = []
        for i in range(len(signal_configs)):
            signal_config_path = signal_configs[i].replace("~", os.path.expanduser("~"))
            config = ConfigParser.ConfigParser()
            config.optionxform = str
            config.readfp(open(signal_config_path, 'r'))
            signal_dict = self.read_all_config_params_to_dict(config)
            paramfilepath = signal_dict['Parameters']['paramfilepath'].replace("~", os.path.expanduser("~"))
            modelfilepath = signal_dict['Strategy']['modelfilepath'].replace("~", os.path.expanduser("~"))
            signal_dict['Parameters']['params'] = self.read_all_txt_params_to_dict(paramfilepath)
            signal_dict['Strategy']['model'] = self.read_all_txt_params_to_dict(modelfilepath)
            signal_dict['Products']['trade_products'] = signal_dict['Products']['trade_products'].split(',')
            data['signals'].append(signal_dict) 
        return json.dumps(data)    
