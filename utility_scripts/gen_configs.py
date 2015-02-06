#!/usr/bin/env/python
import sys
import os
import shutil
import numpy
import argparse
import fileinput
import subprocess
import itertools
import ConfigParser
import matplotlib.pyplot as plt

flatten = lambda lst: reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple)) else l + [i], lst, [])

count = 0

def findAll(file, searchExp):
    found = False
    handle = open(file, "r")
    for line in handle:
        if searchExp in line:
            found = True
            break
    handle.close()
    return found

def replaceAll(file, searchExp, replaceExp): # TODO change to efficient one
    found = False
    handle = fileinput.input(file, inplace=1)
    for line in handle:
        if searchExp in line:
            line = replaceExp + '\n'
            found = True
        sys.stdout.write(line)
    handle.close()
    return found

def is_optional_pattern(pattern):
    return len(pattern) >= 2 and pattern[0] == '<' and pattern[-1] == '>'

def is_range_pattern(pattern):
    return len(pattern) >= 2 and pattern[0] == '[' and pattern[-1] == ']'

def is_list_pattern(pattern):
    return len(pattern) >= 2 and pattern[0] == '{' and pattern[-1] == '}'

def process_optional_pattern(pattern):
    ret_val = [''] # By default an empty value for an optional pattern
    pattern = pattern[1:-1] # Skip brackets
    if is_range_pattern(pattern):
        ret_val.extend(process_range_pattern(pattern))
    elif is_list_pattern(pattern):
        ret_val.extend(process_list_pattern(pattern))
    else:
        sys.exit('something wrong')
    return ret_val    

def process_range_pattern(pattern):
    pattern = pattern[1:-1] # Skip the brackets
    values_list = pattern.split(':')
    if len(values_list) != 3:
        sys.exit('something wrong in [] specification')
    if '.' in pattern: # We are dealing with floats  
        start, end, incr = float(values_list[0]), float(values_list[1]), float(values_list[2])
    else:        
        start, end, incr = int(values_list[0]), int(values_list[1]), int(values_list[2])
    ret_val = list(numpy.arange(start, end + incr, incr)) # to include the end
    for i in range(len(ret_val)):
        ret_val[i] = str(ret_val[i])
    return ret_val

def process_list_pattern(pattern):
    ret_val = []
    pattern = pattern[1:-1] # Skip the brackets
    values_list = pattern.split('\'')
    values_list = filter(lambda x: x != ',', values_list) # Remove commas
    if len(values_list) != 1:
        values_list = filter(lambda x: x != '', values_list) # Remove empty
        return values_list
    values_list = filter(lambda x: x != '', values_list) # Remove empty
    values_list = values_list[0].split(',')
    for value in values_list:
        if is_range_pattern(value):
            ret_val.extend(process_range_pattern(value))
        else: 
            ret_val.append(value)   
    return ret_val

def parse_variable_values(pattern):
    _patterns = pattern.split(' ')
    ret_val = []
    for _pattern in _patterns:
        if is_optional_pattern(_pattern):
            _sub_pattern_vals = process_optional_pattern(_pattern)
        elif is_range_pattern(_pattern):
            _sub_pattern_vals = process_range_pattern(_pattern)
        elif is_list_pattern(_pattern):
            _sub_pattern_vals = process_list_pattern(_pattern)
        else:
            _pattern = _pattern.translate(None, '\'')
            _sub_pattern_vals = [_pattern] # Value without brackets
        ret_val.append(_sub_pattern_vals)
    ret_val = list(itertools.product(*ret_val))
    for i in range(len(ret_val)):
        ret_val[i] = (' '.join(list(ret_val[i]))).strip()
    return ret_val

def parse_dep_variable_values(dep_vars, _section, param_string):
    comb_strings = param_string.split('&')
    ret_val = []
    for comb_string in comb_strings:
        _patterns = comb_string.split('|')
        if len(_patterns) != len(dep_vars):
            sys.exit('Something wrong in dependency variable speicifcation')
        ret_val.append({})
        for i in range(len(_patterns)):
            ret_val[-1][(_section, dep_vars[i])] = parse_variable_values(_patterns[i])
    return ret_val

def get_name(filepath):
    return os.path.splitext(os.path.basename(filepath))[0].split('_',1)[0]

def copy_config_files(agg_config_path, dest_dir):
    """Copies the configs(agg and signal(+param, +model)) to the 'base' folder and returns the path to the new agg config

    Args:
        agg_config_file(string): The path to the config of the aggregator
        dest_dir(string): The destination directory for this permutparamfile.EG: /spare/local/logs/param_file/

    Returns: The path to the new agg config
    """
    signal_config_paths = []
    save_dir = dest_dir
    dest_dir = dest_dir + 'base/' # Destination directory is '/spare/local/logs/param_file/base/'
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    _agg_name = get_name(agg_config_path)
    new_agg_config_path = dest_dir + get_name(agg_config_path) + '_A' + os.path.splitext(agg_config_path)[1]
    shutil.copyfile(agg_config_path, new_agg_config_path) # Copy aggregator config to new destination
    agg_config = ConfigParser.ConfigParser() # Read aggregator config
    agg_config.optionxform = str
    agg_config.readfp(open(new_agg_config_path, 'r'))
    if not agg_config.has_option('Strategy','signal_configs'):
        sys.exit('something wrong. Signal config path not present in agg config')
    signal_configs = agg_config.get('Strategy','signal_configs').split(',')
    i = 0
    for _signal_config_path in  signal_configs:
        i += 1
        _signal_config_path_ = _signal_config_path.replace("~", os.path.expanduser("~"))
        new_signal_config_path = dest_dir + get_name(_signal_config_path) + '_S' + os.path.splitext(_signal_config_path)[1]
        signal_config_paths.append(new_signal_config_path)
        shutil.copyfile(_signal_config_path_, new_signal_config_path) # copy signal configs to new destination
        signal_config = ConfigParser.ConfigParser() # Read signal configs
        signal_config.optionxform = str
        signal_config.readfp(open(new_signal_config_path, 'r'))
        signal_dir = save_dir + 'signals/' + get_name(new_signal_config_path) + '/'
        if not os.path.exists(signal_dir):
            os.makedirs(signal_dir)
        if not signal_config.has_option('Parameters','paramfilepath'):
            sys.exit('something wrong! No paramfilepath in signal config')
        _old_paramfilepath = signal_config.get('Parameters','paramfilepath').replace("~", os.path.expanduser("~"))
        _new_paramfilepath = dest_dir + get_name(_old_paramfilepath) + '_P' + os.path.splitext(_old_paramfilepath)[1]
        shutil.copyfile(_old_paramfilepath, _new_paramfilepath) # copy paramfile of signal config to new destination
        _old_modelfilepath = signal_config.get('Strategy','modelfilepath').replace("~", os.path.expanduser("~"))
        _new_modelfilepath = dest_dir + get_name(_old_modelfilepath) + '_M' + os.path.splitext(_old_modelfilepath)[1]
        shutil.copyfile(_old_modelfilepath, _new_modelfilepath) # copy modelfile of signal config to new destination
        signal_config.set('Parameters', 'paramfilepath', _new_paramfilepath)
        signal_config.set('Strategy', 'modelfilepath', _new_modelfilepath)
        with open(new_signal_config_path, 'wb') as configfile: # change paramfile and modelfile path in signal configs
            signal_config.write(configfile)

    # Change the signals path in the new aggregator config
    _signal_config_string = ','.join(signal_config_paths)
    agg_config.set('Strategy','signal_configs', _signal_config_string)
    with open(new_agg_config_path, 'wb') as configfile:
        agg_config.write(configfile)
    return new_agg_config_path

def get_all_param_locations(base_agg_config_path, param_names):
    # param_location_map : Map from one of these 'Strategy' -> [(idx, section, param_name)],'Signali' to [ [(idx, section, param_name)], [(idx, param_name)], [(idx, param_name)] ]
    param_location_map = {}
    agg_config = ConfigParser.ConfigParser() # Read aggregator config
    agg_config.optionxform = str
    agg_config.readfp(open(base_agg_config_path, 'r'))
    signal_config_paths = agg_config.get('Strategy','signal_configs').split(',')
    signal_configs = []
    for i in range(len(signal_config_paths)):
        signal_config = ConfigParser.ConfigParser() # Read aggregator config
        signal_config.optionxform = str
        _signal_config_path = signal_config_paths[i].replace("~", os.path.expanduser("~"))
        signal_config.readfp(open(_signal_config_path, 'r'))
        if not signal_config.has_option('Strategy', 'modelfilepath'): # TODO modelfile should be optional
            sys.exit('something wrong! modelfilepath not there in signal config')
        _model_file_path = signal_config.get('Strategy', 'modelfilepath').replace("~", os.path.expanduser("~"))
        if not signal_config.has_option('Parameters', 'paramfilepath'):
            sys.exit('something wrong! paramfilepath not there in signal config')
        _param_file_path = signal_config.get('Parameters', 'paramfilepath').replace("~", os.path.expanduser("~"))
        signal_configs.append((signal_config, _param_file_path, _model_file_path))

    idx = 0
    for section, param in param_names:
        found = False
        if section not in param_location_map.keys():
            if section[0:6] == 'Signal':
                param_location_map[section] = [[],[],[]]
            else:
                param_location_map[section] = []
        if section == 'Strategy':
            for _section in agg_config.sections():
                for _var in agg_config.options(_section):
                    if _var == param:
                        param_location_map[section].append((idx, _section, _var))
                        found = True
                        break
                if found: break
        elif section[0:6] == 'Signal': # TODO change to better way
            signal_num = int(section[6:]) - 1
            for _section in signal_configs[signal_num][0].sections():
                for _var in signal_configs[signal_num][0].options(_section):
                    if _var == param:
                        param_location_map[section][0].append((idx, _section, _var))
                        found = True
                        break
                if found: break
            if not found:
                found = findAll(signal_configs[signal_num][1], param)
                if found:
                    param_location_map[section][1].append((idx, param))
                else:
                    found = findAll(signal_configs[signal_num][2], param)
                    if found:
                        param_location_map[section][2].append((idx, param))
        if not found:
            sys.exit('something wrong! could not find a param anywhere')
        idx += 1
    return param_location_map

def generate_all_combinations(permutparam_config_path):
    """Reads the permutparams file and generates all the pairs of values to be tested

    Args:
        permutparam_config_path(string): path to the permutparamfile

    Returns:
        test_dirs(list): Each element is a path to a test directory like '/spare/local/logs/permutparamfile/test_name/'
                         Each test dir further contains folders, each folder containing configs with one set of parameter values corresponding to that test 
    """
    permutparam_config = ConfigParser.ConfigParser() # Read permutparam config
    permutparam_config.optionxform = str
    permutparam_config.readfp(open(permutparam_config_path, 'r'))

    # Go through each test and generate its combinations
    dep_combinations, nondep_combinations = generate_combinations(permutparam_config)
    dep_params = []
    nondep_params = nondep_combinations.keys()
    dep_param_comb = []
    nondep_param_comb = []
    all_comb = []
    all_params = []
    dep_done = False
    for i in range(len(dep_combinations)):
        sub_list = dep_combinations[i]
        union_comb = []
        for _dict in sub_list:
            comb = []
            for key in _dict.keys():
                if not dep_done:
                    dep_params.append(key)
                comb.append(_dict[key])
            dep_done = True
            union_comb.extend(itertools.product(*comb))
        dep_param_comb.append(union_comb)
    for key in nondep_combinations.keys():
        nondep_param_comb.append(nondep_combinations[key])
    all_comb = []
    all_comb.extend(dep_param_comb)
    all_comb.extend(nondep_param_comb)
    all_params = []
    all_params.extend(dep_params)
    all_params.extend(nondep_params)
    all_comb = list(itertools.product(*all_comb))
    all_comb = map(flatten, all_comb)
    combinations = (all_params, all_comb)
    return combinations

def generate_combinations(permutparam_config):
    combinations = []
    variable_to_combination_map = {}
    for _section in permutparam_config.sections():
        for _var in permutparam_config.options(_section):
            if '|' not in _var:
                variable_to_combination_map[(_section, _var)] = parse_variable_values(permutparam_config.get(_section, _var))
            else: # If it is a dependency relation
                _vars = _var.split('|')
                dep_vars_comb = parse_dep_variable_values(_vars, _section, permutparam_config.get(_section, _var))
                combinations.append(dep_vars_comb)
    return combinations, variable_to_combination_map

def generate_test_configs(base_agg_config_path, combinations, dest_dir):
    
    '''Creates dir for each test and separate folder of each set of params for a given test'''
    agg_config_list = []
    paramsfile_exist = {}
    modelfile_exist = {}
    global count
    param_count = count
    model_count = count
    #print 'comb', combinations
    # find out the location of params for this test
    # param_location_map : Map from one of these 'Strategy' -> [(idx, section, param_name)],'Signali' to [ [(idx, section, param_name)], [(idx, param_name)], [(idx, param_name)] ]
    param_location_map = get_all_param_locations(base_agg_config_path, combinations[0])
    #print 'param loc map',param_location_map
                
    #sys.exit()
    for i in range(len(combinations[1])):
        new_agg_config_path = dest_dir + 'strategies/' + get_name(base_agg_config_path) + '_' + str(count) + os.path.splitext(base_agg_config_path)[1]
        agg_config_list.append(new_agg_config_path) # save the agg path
        shutil.copyfile(base_agg_config_path, new_agg_config_path)
        agg_config = ConfigParser.ConfigParser() # Read aggregator config
        agg_config.optionxform = str
        agg_config.readfp(open(new_agg_config_path, 'r'))
        if not agg_config.has_option('Strategy','signal_configs'):
            sys.exit('something wrong. Signal config path not present in agg config')
        _signal_configs = agg_config.get('Strategy','signal_configs').split(',')

        _signal_config_string = []
        for k in range(len(_signal_configs)):
            _old_signal_path = _signal_configs[k]
            #print _old_signal_path,get_name(_old_signal_path) 
            _new_signal_path = dest_dir + 'signals/' + get_name(_old_signal_path) + '/' + get_name(_old_signal_path) + '_' + str(count) + os.path.splitext(_old_signal_path)[1]
            shutil.copyfile(_old_signal_path, _new_signal_path)
            _signal_config_string.append(_new_signal_path)
        _signal_config_string = ','.join(_signal_config_string)
        agg_config.set('Strategy','signal_configs', _signal_config_string)

        # change variables in agg config
        if 'Strategy' in param_location_map.keys():
            for idx, section, param_name in param_location_map['Strategy']:
                if not agg_config.has_option(section, param_name):
                    sys.exit('something wrong in agg config section')
                agg_config.set(section, param_name, combinations[1][i][idx])
                # Write the agg config file with modifications
        with open(new_agg_config_path, 'wb') as configfile:
            agg_config.write(configfile)

        signal_configs = agg_config.get('Strategy','signal_configs').split(',')
        for j in range(len(signal_configs)):
            new_signal_config_path = signal_configs[j]
            signal_config = ConfigParser.ConfigParser() # Read signal configs
            signal_config.optionxform = str
            signal_config.readfp(open(new_signal_config_path, 'r'))
            signal_section = 'Signal%d' % (j+1)
            if signal_section in param_location_map.keys(): 
                for idx, section, param_name in param_location_map[signal_section][0]: # Signal config params
                    if not signal_config.has_option(section, param_name):
                        sys.exit('something wrong in signal config section')
                    signal_config.set(section, param_name, combinations[1][i][idx]) 
                val_comb = []
                for idx, param_name in param_location_map[signal_section][1]: # Signal paramfile params
                    val_comb.append(combinations[1][i][idx])
                val_comb_str = ''.join(val_comb)
                if val_comb_str not in paramsfile_exist.keys():
                    _old_paramfilepath = signal_config.get('Parameters','paramfilepath').replace("~", os.path.expanduser("~"))
                    _new_paramfilepath = dest_dir + 'paramfiles/' + get_name(_old_paramfilepath) + '_%d'%param_count + os.path.splitext(_old_paramfilepath)[1]
                    param_count += 1
                    shutil.copyfile(_old_paramfilepath, _new_paramfilepath)
                    signal_config.set('Parameters', 'paramfilepath', _new_paramfilepath)
                    paramsfile_exist[val_comb_str] = _new_paramfilepath
                    for idx, param_name in param_location_map[signal_section][1]:
                        new_line = '%s %s' % (param_name, combinations[1][i][idx])
                        status = replaceAll(_new_paramfilepath, param_name, new_line)
                        if not status:
                            sys.exit('something wrong! could not find param in paramfile')
                else:
                    signal_config.set('Parameters', 'paramfilepath', paramsfile_exist[val_comb_str])
          
                val_comb = []
                for idx, param_name in param_location_map[signal_section][2]: # Signal modelfile params
                    val_comb.append(combinations[1][i][idx])
                val_comb_str = ''.join(val_comb)
                if val_comb_str not in modelfile_exist.keys():
                    _old_modelfilepath = signal_config.get('Strategy','modelfilepath').replace("~", os.path.expanduser("~"))
                    _new_modelfilepath = dest_dir + 'signals/' + get_name(signal_configs[j]) + '/' + get_name(_old_modelfilepath) + '_%d'%model_count + os.path.splitext(_old_modelfilepath)[1]
                    model_count += 1
                    shutil.copyfile(_old_modelfilepath, _new_modelfilepath)
                    signal_config.set('Strategy', 'modelfilepath', _new_modelfilepath)
                    modelfile_exist[val_comb_str] = _new_modelfilepath
                    for idx, param_name in param_location_map[signal_section][2]:
                        new_line = '%s %s' % (param_name, combinations[1][i][idx])
                        status = replaceAll(_new_modelfilepath, param_name, new_line)
                        if not status:
                            sys.exit('something wrong! could not find param in modelfile')
                else:
                    signal_config.set('Parameters', 'modelfilepath', modelfile_exist[val_comb_str])

            # Write the signal file with modifications at the end
            with open(new_signal_config_path, 'wb') as configfile:
                signal_config.write(configfile)
        count += 1
    return agg_config_list

def get_perf_stats(agg_configs):
    """For each set of values to be tested,set up the config files and run simulator to get the perf stats"""
    performance_stats = []
    for agg_config in agg_configs:
        proc = subprocess.Popen(['python', '-W', 'ignore', 'run_simulator.py', agg_config ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        performance_stats.append(parse_results(proc.communicate()[0])) # TODO parse results should only parse final_order stats
    return performance_stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('agg_config_file')
    parser.add_argument('param_file')
    parser.add_argument('-dir',type=str, help='Directory to store configs\nEg: -dir ~/modeling/', default= "~/modeling/sample_strats/".replace("~", os.path.expanduser("~")), dest='dir')
    parser.add_argument('-c', type=int, help='Count to start from\nEg: -c 100', default=0, dest='count')
    parser.add_argument('-r', type=int, help='To run or not\nEg: -r 1', default=0, dest='run')
    args = parser.parse_args()
    agg_config_path = sys.argv[1].replace("~", os.path.expanduser("~"))
    permutparam_config_path = sys.argv[2].replace("~", os.path.expanduser("~"))
    global count
    count = args.count
    dest_dir = args.dir.replace("~", os.path.expanduser("~")) # directory to store output files
    dirs = [dest_dir + 'strategies/', dest_dir + 'signals/', dest_dir + 'paramfiles/', dest_dir + 'base/']
    for dir in dirs:
        if not os.path.exists(dir):
            os.makedirs(dir)

    # TODO empty directory before starting
    # TODO use ordered dict
    new_agg_config_path = copy_config_files(agg_config_path, dest_dir)
    combinations = generate_all_combinations(permutparam_config_path)
    agg_config_list = generate_test_configs(new_agg_config_path, combinations, dest_dir)
    if args.run == 1:
        perf_stats = get_perf_stats(agg_config_list)   

if __name__ == '__main__':
    main()
