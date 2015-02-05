import os
import sys
import json
import yaml
import time
import MySQLdb
import datetime
import ConfigParser
from compiler.ast import flatten

class JsonParser():

    def read_all_config_params_to_dict(self, config):
        data = {}
        for _section in config.sections():
            data[_section] = {}
            for _var in config.options(_section):
                data[_section][_var] = config.get(_section, _var)
        return data

    def read_all_txt_params_to_list(self, txt_file):
        data = []
        with open(txt_file, 'r') as file_handle:
            data = file_handle.read().splitlines() 
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
        data['RiskManagement']['risk_file'] = self.read_all_txt_params_to_list(risk_profile_path)
        data['signals'] = []
        for i in range(len(signal_configs)):
            signal_config_path = signal_configs[i].replace("~", os.path.expanduser("~"))
            config = ConfigParser.ConfigParser()
            config.optionxform = str
            config.readfp(open(signal_config_path, 'r'))
            signal_dict = self.read_all_config_params_to_dict(config)
            paramfilepath = signal_dict['Parameters']['paramfilepath'].replace("~", os.path.expanduser("~"))
            modelfilepath = signal_dict['Strategy']['modelfilepath'].replace("~", os.path.expanduser("~"))
            signal_dict['Parameters']['param_file'] = self.read_all_txt_params_to_list(paramfilepath)
            signal_dict['Strategy']['model_file'] = self.read_all_txt_params_to_list(modelfilepath)
            signal_dict['Products']['trade_products'] = signal_dict['Products']['trade_products'].split(',')
            trade_products = []
            for i in range(len(signal_dict['Products']['trade_products'])):
                products_file = signal_dict['Products']['trade_products'][i].replace("~", os.path.expanduser("~"))
                with open(products_file) as f:
                    products = f.read().splitlines()
                trade_products.append(products)
            signal_dict['Products']['trade_products'] = trade_products
            data['signals'].append(signal_dict) 
        return json.dumps(data)

    def cfg_to_json(self, config_file):
        data = {}
        config = ConfigParser.ConfigParser() # Read aggregator config
        config.optionxform = str
        config.readfp(open(config_file, 'r'))
        data = self.read_all_config_params_to_dict(config)
        data["config_name"] = config_file
        data["Strategy"]["signal_configs"] = data["Strategy"]["signal_configs"].split(",")
        signal_configs = data["Strategy"]["signal_configs"]
        risk_profile_path = data["RiskManagement"]["risk_profile"].replace("~", os.path.expanduser("~"))
        data["RiskManagement"]["risk_file"] = self.read_all_txt_params_to_list(risk_profile_path)
        data["signals"] = []
        for i in range(len(signal_configs)):
            signal_config_path = signal_configs[i].replace("~", os.path.expanduser("~"))
            config = ConfigParser.ConfigParser()
            config.optionxform = str
            config.readfp(open(signal_config_path, "r"))
            signal_dict = self.read_all_config_params_to_dict(config)
            paramfilepath = signal_dict["Parameters"]["paramfilepath"].replace("~", os.path.expanduser("~"))
            modelfilepath = signal_dict["Strategy"]["modelfilepath"].replace("~", os.path.expanduser("~"))
            signal_dict["Parameters"]["param_file"] = self.read_all_txt_params_to_list(paramfilepath)
            signal_dict["Strategy"]["model_file"] = self.read_all_txt_params_to_list(modelfilepath)
            signal_dict["Products"]["trade_products"] = signal_dict["Products"]["trade_products"].split(",")
            trade_products = []
            for i in range(len(signal_dict["Products"]["trade_products"])):
                products_file = signal_dict["Products"]["trade_products"][i].replace("~", os.path.expanduser("~"))
                with open(products_file) as f:
                    products = f.read().splitlines()
                trade_products.append(products)
            signal_dict["Products"]["trade_products"] = trade_products
            data["signals"].append(signal_dict)
        return json.dumps(data)
        
    def dump_sim_to_db(self, config_file, dates, returns, leverage, stats):
        try:
            db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="webapp")
        except MySQLdb.Error:
            sys.exit("Error In DB Connection")
        dates = str([date.strftime("%Y-%m-%d") for date in dates]).replace("'",'"')
        query = "INSERT INTO strategies (name, params, stats, dates, daily_log_returns, leverage, created_at, updated_at) VALUES('%s','%s','%s','%s','%s','%s','%s','%s')" %(config_file, self.cfg_to_json(config_file), str(stats).replace("'",'"'), dates, list(returns), list(leverage), datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        try :
            db.cursor().execute(query)
            db.commit()
            db.cursor().close()
        except:
            db.rollback()
            print 'Alert! simulation not stored in DB'

    def write_list_to_text_file(self, paramlist, filepath):
        with open(filepath, 'w') as f:
            f.write('\n'.join(paramlist))

    def json_to_cfg(self, _json_file, _output_cfg_dir):
        cfg = yaml.load(open(_json_file).read())
        Config = ConfigParser.ConfigParser()
        aggconfig = open(_output_cfg_dir + 'agg.cfg', 'w')
        for section in cfg.keys():
            if section in ['Parameters','Strategy','RiskManagement']:
                Config.add_section(section)
                for key in cfg[section].keys():
                    if key not in ['risk_file']:
                        Config.set(section, key, cfg[section][key])
        signal_paths = [_output_cfg_dir + 'signal%s.cfg'%i for i in range(len(cfg['Strategy']['signal_configs']))]
        Config.set('Strategy', 'signal_configs', ','.join(signal_paths))
        Config.set('RiskManagement', 'risk_profile', _output_cfg_dir + 'risk.cfg')
        if 'signal_allocations' in cfg['Strategy']['signal_allocations']:
            Config.set('Strategy', 'signal_allocations', ','.join(cfg['Strategy']['signal_allocations']))
        Config.write(aggconfig)
        aggconfig.close()
        risk_file = _output_cfg_dir + 'risk.cfg'
        self.write_list_to_text_file(cfg['RiskManagement']['risk_file'], risk_file)
        for _signal_config_idx in range(len(cfg['Strategy']['signal_configs'])):
            Config = ConfigParser.ConfigParser()
            signal_filename = 'signal%d' % _signal_config_idx
            signalconfig = open(_output_cfg_dir + signal_filename + '.cfg', 'w')
            signal_dict = cfg['signals'][_signal_config_idx]
            for section in signal_dict.keys():
                if section in ['Parameters','Strategy','Products']:
                    Config.add_section(section)
                    for key in signal_dict[section].keys():
                        if key not in ['param_file', 'model_file']:
                            Config.set(section, key, signal_dict[section][key])
            param_file = _output_cfg_dir + signal_filename + 'param.txt'
            model_file = _output_cfg_dir + signal_filename + 'model.txt'
            products_file = _output_cfg_dir + signal_filename + 'products.txt'
            self.write_list_to_text_file(signal_dict['Parameters']['param_file'], param_file)
            self.write_list_to_text_file(signal_dict['Strategy']['model_file'], model_file)
            with open(products_file, 'w') as f:
                f.write('\n'.join(flatten(signal_dict['Products']['trade_products'])))        
            Config.set('Products', 'trade_products', products_file)
            Config.set('Parameters', 'paramfilepath', param_file)
            Config.set('Strategy', 'modelfilepath', model_file)
            Config.write(signalconfig)
            signalconfig.close()

#JsonParser().json_to_cfg('/home/cvdev/logs/A1_TRVP_all_rb1_model1_rmp_profile1/output.json', '/home/cvdev/logs/')                  
