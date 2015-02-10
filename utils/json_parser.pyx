import os
import sys
import json
import yaml
import time
import MySQLdb
import datetime
import ConfigParser
import hashlib
from compiler.ast import flatten
from utils.regular import stat_dict_to_string
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
        return sorted(data)

    '''def sim_to_json(self, config_file, dates, returns, leverage, stats):
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
        return json.dumps(data)'''

    def cfg_to_dict(self, config_file, start_date, end_date):
        config = ConfigParser.ConfigParser() # Read aggregator config
        config.optionxform = str
        config.readfp(open(config_file, 'r'))
        data = self.read_all_config_params_to_dict(config)
        data["Dates"] = {}
        data["Dates"]["start_date"] = start_date
        data["Dates"]["end_date"] = end_date
        data["config_name"] = os.path.basename(config_file)
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
                trade_products.append(sorted(products))
            signal_dict["Products"]["trade_products"] = sorted(trade_products)
            data["signals"].append(signal_dict)
        return data
        
    def dump_sim_to_db(self, config_path, params_json, config_hash, force, is_daily_update, dates, returns, leverage, stats):
        try:
            db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="webapp")
            db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:
            sys.exit("Error In DB Connection")
        dates = str([date.strftime("%Y-%m-%d") for date in dates]).replace("'",'"')
        config_name = os.path.basename(config_path)
        query = "SELECT id,name FROM strategies where config_hash='%s'" % config_hash
        try :
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            if len(rows) >= 1 and is_daily_update: # Is daily update
                pass
            elif len(rows) == 0 and is_daily_update: # This should not happen
                sys.exit('Trying to do daily update for simulation not present')
            elif len(rows) >= 1 and not force:
                print '%d simulations with same config are already present'%len(rows)
                for row_index in range(len(rows)):
                    print 'ID: %d, config: %s'%(rows[row_index]['id'], rows[row_index]['name'])
                    sys.exit('Aborting Simulation')
            else: # No config matches to this simulation, or forced to insert simulation
                query = "INSERT INTO strategies (name, config_hash, params, stats, dates, daily_log_returns, leverage, created_at, updated_at) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(config_name, config_hash, params_json, str(stats).replace("'",'"'), dates, list(returns), list(leverage), datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                try :
                    db_cursor.execute(query)
                    db.commit()
                    db_cursor.close()
                except:
                    db.rollback()
                    print 'Alert! simulation not stored in DB'
        except:
            print 'Alert! simulation not stored in DB 1'

    def is_sim_present(self, config_file, force, is_daily_update, start_date, end_date):
        try:
            db = MySQLdb.connect(host="fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com", user="cvmysql",passwd="fixedcvincome", db="webapp")
            db_cursor = db.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error:
            sys.exit("Error In DB Connection")
        params_dict = self.cfg_to_dict(config_file, start_date, end_date)
        params_json = json.dumps(params_dict)
        config_hash = self.get_config_hash(params_json)
        if force or is_daily_update:
            query = "SELECT id,name FROM strategies where config_hash='%s'" % config_hash
        else:
            query = "SELECT id,name,stats,params FROM strategies where config_hash='%s'" % config_hash
        try :
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
            if len(rows) >= 1:
                print '%d simulations with same config are already present'%len(rows)
                for row_index in range(len(rows)):
                    print 'ID: %d, config: %s'%(rows[row_index]['id'], rows[row_index]['name'])
                if not (force or is_daily_update):
                    print 'Fetching Simulation from DB...\n'
                    params = yaml.load(rows[0]['params'])
                    stats = yaml.load(rows[0]['stats'])
                    print 'Start date: %s\nEnd date: %s' % (params['Dates']['start_date'], params['Dates']['end_date'])
                    print stat_dict_to_string(stats)
                    sys.exit()
        except:
            sys.exit()
        db_cursor.close()
        del db_cursor
        db.close()
        return params_json, config_hash

    def get_config_hash(self, params_json): 
        params_json_copy = json.loads(params_json)
        del params_json_copy["config_name"]
        del params_json_copy["Dates"]
        del params_json_copy["Strategy"]["signal_configs"]
        del params_json_copy["RiskManagement"]["risk_profile"]
        for i in range(len(params_json_copy["signals"])):
            del params_json_copy["signals"][i]["Strategy"]["modelfilepath"]
            del params_json_copy["signals"][i]["Parameters"]["paramfilepath"]
        return hashlib.md5(json.dumps(params_json_copy, sort_keys=True)).hexdigest()
        #return hash(json.dumps(params_json_copy, sort_keys=True))

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
