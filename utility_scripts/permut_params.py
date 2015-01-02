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

stats = {'sharpe': ('Sharpe Ratio', '+'),  'ret_dd_ratio': ('Return_drawdown_Ratio', '+'), 'max_dd': ('Max Drawdown', '-'), 'net_pnl': ('Net PNL', '+'), 'ret_var_ratio': ('Return Var10 ratio', '+'),'ann_ret': ('Annualized_Returns','+'), 'gain_pain_ratio': ('Gain Pain Ratio', '+'), 'hit_loss_ratio': ('Hit Loss Ratio', '+'), 'turnover': ('Turnover', '-'), 'skewness': ('Skewness','?'), 'kurtosis': ('Kurtosis', '?'), 'corr_vbltx': ('Correlation to VBLTX', '?'), 'corr_vtsmx': ('Correlation to VTSMX', '?'), 'ann_std_ret': ('Annualized_Std_Returns', '-'), 'max_dd_dollar': ('Max Drawdown Dollar', '-'), 'ann_pnl': ('Annualized PNL', '+'), 'dml': ('DML', '+'), 'mml': ('MML', '+'), 'qml': ('QML', '+'), 'yml': ('YML', '+'), 'max_num_days_no_new_high' : ('Max num days with no new high', '-')}

final_order = ['Net Returns', 'Total Tradable Days','Sharpe Ratio', 'Return_drawdown_Ratio','Return Var10 ratio','Correlation to VBLTX', 'Correlation to VTSMX', 'Annualized_Returns', 'Annualized_Std_Returns', 'Initial Capital', 'Net PNL', 'Annualized PNL', 'Annualized_Std_PnL', 'Skewness','Kurtosis','DML','MML','QML','YML','Max Drawdown','Drawdown Period','Drawdown Recovery Period','Max Drawdown Dollar','Annualized PNL by drawdown','Yearly_sharpe','Hit Loss Ratio','Gain Pain Ratio','Max num days with no new high','Losing month streak','Turnover','Leverage','Trading Cost','Total Money Transacted','Total Orders Placed','Worst 5 days','Best 5 days','Worst 5 weeks','Best 5 weeks']

flatten = lambda lst: reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple)) else l + [i], lst, [])

def parse_results(results):
    """Parses the performance stats(output of simulator) and returns them as dict

    Args:
        results(string): The results shown by the simulator as a single string

    Returns: Dictionary from stat name to its value
    """
    results = results.split('\n')
    _dict_results = {}
    for result in results:
        if '=' in result:
            _result = result.split('=')
            _name = _result[0].strip()
            _val = _result[1].strip()
            _dict_results[_name] = _val
    return _dict_results

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

def copy_config_files(agg_config_path, dest_dir):
    """Copies the configs(agg and signal(+param, +model)) to the 'base' folder and returns the path to the new agg config

    Args:
        agg_config_file(string): The path to the config of the aggregator
        dest_dir(string): The destination directory for this permutparamfile.EG: /spare/local/logs/param_file/

    Returns: The path to the new agg config
    """
    signal_config_paths = []
    dest_dir = dest_dir + 'base/' # Destination directory is '/spare/local/logs/param_file/base/'
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    new_agg_config_path = dest_dir + os.path.basename(agg_config_path)
    shutil.copyfile(agg_config_path, new_agg_config_path) # Copy aggregator config to new destination
    agg_config = ConfigParser.ConfigParser() # Read aggregator config
    agg_config.optionxform = str
    agg_config.readfp(open(new_agg_config_path, 'r'))
    if not agg_config.has_option('Strategy','signal_configs'):
        sys.exit('something wrong. Signal config path not present in agg config')
    signal_configs = agg_config.get('Strategy','signal_configs').split(',')
    for _signal_config_path in  signal_configs:
        _signal_config_path_ = _signal_config_path.replace("~", os.path.expanduser("~"))
        new_signal_config_path = dest_dir + os.path.basename(_signal_config_path_)
        signal_config_paths.append(new_signal_config_path)
        shutil.copyfile(_signal_config_path_, new_signal_config_path) # copy signal configs to new destination
        signal_config = ConfigParser.ConfigParser() # Read signal configs
        signal_config.optionxform = str
        signal_config.readfp(open(new_signal_config_path, 'r'))
        if not signal_config.has_option('Parameters','paramfilepath'):
            sys.exit('something wrong! No paramfilepath in signal config')
        _old_paramfilepath = signal_config.get('Parameters','paramfilepath').replace("~", os.path.expanduser("~"))
        _new_paramfilepath = dest_dir + os.path.splitext(os.path.basename(new_signal_config_path))[0] + '-' + os.path.basename(_old_paramfilepath)
        shutil.copyfile(_old_paramfilepath, _new_paramfilepath) # copy paramfile of signal config to new destination
        _old_modelfilepath = signal_config.get('Strategy','modelfilepath').replace("~", os.path.expanduser("~"))
        _new_modelfilepath = dest_dir + os.path.splitext(os.path.basename(new_signal_config_path))[0] + '-' + os.path.basename(_old_modelfilepath)
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

def generate_test_combinations(permutparam_config_path):
    """Reads the permutparams file and generates all the pairs of values to be tested

    Args:
        base_agg_config_path(string): path to the agg config in the /spare/local/logs/permutparamfile/base/ dir
        permutparam_config_path(string): path to the permutparamfile
        dest_dir(string): path to the destination directory where all the files are to be created (/spare/local/logs/permutparamfile/)

    Returns:
        test_dirs(list): Each element is a path to a test directory like '/spare/local/logs/permutparamfile/test_name/'
                         Each test dir further contains folders, each folder containing configs with one set of parameter values corresponding to that test 
    """
    test_to_variable_map = {} # Map from test_name to list of params to be changed in this test
    test_to_combinations_map = {}
    permutparam_config = ConfigParser.ConfigParser() # Read permutparam config
    permutparam_config.optionxform = str
    permutparam_config.readfp(open(permutparam_config_path, 'r'))
    all_sections = permutparam_config.sections()
    if 'Tests' in all_sections:
        all_sections.remove('Tests')
    all_tests = dict(permutparam_config.items('Tests')) # TODO should check if section exists
    if not all_tests: # If no test is specified in permutparam config
        all_tests['all_combinations'] = '*'
    for test_name in all_tests.keys():
        all_tests[test_name] = all_tests[test_name].split(',')
    for test_name in all_tests.keys():
        variables = all_tests[test_name]
        processed_variables = []
        for variable in variables:
            if ':' in variable: # Section has been specified with the variable.Eg: Strategy:start_date
                elements = variable.split(':')
                section, var = elements[0], elements[1]
                if section not in all_sections:
                    sys.exit('Section specified incorrectly in Tests')
                processed_variables.append((section, var))
            elif variable == '*': # All params need to be permuted.Eg: *
                processed_variables.append((variable, variable))
                if len(variables) != 1:
                    sys.exit('something wrong in * test')
            else: # Section has not been specified with the variable.Eg: rebalance_frequency
                for section in all_sections:
                    processed_variables.append((section, variable))
        test_to_variable_map[test_name] = processed_variables

    # Go through each test and generate its combinations
    for test_name in test_to_variable_map.keys():
        dep_test_combinations, nondep_test_combinations = generate_combinations_for_test(test_to_variable_map[test_name], permutparam_config)
        dep_params = []
        nondep_params = nondep_test_combinations.keys()
        dep_param_comb = []
        nondep_param_comb = []
        all_comb = []
        all_params = []
        dep_done = False
        for i in range(len(dep_test_combinations)):
            sub_list = dep_test_combinations[i]
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
        for key in nondep_test_combinations.keys():
            nondep_param_comb.append(nondep_test_combinations[key])
        all_comb = []
        all_comb.extend(dep_param_comb)
        all_comb.extend(nondep_param_comb)
        all_params = []
        all_params.extend(dep_params)
        all_params.extend(nondep_params)
        all_comb = list(itertools.product(*all_comb))
        all_comb = map(flatten, all_comb)
        test_to_combinations_map[test_name] = (all_params, all_comb)
    return test_to_combinations_map

def generate_combinations_for_test(variables, permutparam_config):
    test_combinations = []
    variable_to_combination_map = {}
    for variable in variables:
        if variable[0] == '*':
            for _section in permutparam_config.sections():
                if _section == 'Tests':
                    continue
                for _var in permutparam_config.options(_section):
                    if '|' not in _var:
                        variable_to_combination_map[(_section, _var)] = parse_variable_values(permutparam_config.get(_section, _var))
                    else: # If it is a dependency relation
                        _vars = _var.split('|')
                        dep_vars_comb = parse_dep_variable_values(_vars, _section, permutparam_config.get(_section, _var))
                        test_combinations.append(dep_vars_comb)
        else:
            _section, var  = variable[0], variable[1]
            if permutparam_config.has_option(_section, var): # If variable is directly specified
                variable_to_combination_map[variable] = parse_variable_values(permutparam_config.get(_section, var))
            else: # Go through each section and check for variable piping
                for _section in permutparam_config.sections():
                    if _section == 'Tests':
                        continue
                    for _var in permutparam_config.options(_section):
                        if '|' in _var and var in _var.split('|'): # If it is a dependency relation
                            _vars = _var.split('|')
                            dep_vars_comb = parse_dep_variable_values(_vars, _section, permutparam_config.get(_section, _var))
                            test_combinations.append(dep_vars_comb)
    return test_combinations, variable_to_combination_map # TODO give better names

def generate_test_configs(base_agg_config_path, test_to_combinations_map, dest_dir):
    '''Creates dir for each test and separate folder of each set of params for a given test'''
    test_to_agg_config_list_map = {}
    test_dirs = {}
    for test_name in test_to_combinations_map.keys():
        test_dir = dest_dir + test_name + '/'
        test_to_agg_config_list_map[test_name] = []
        test_dirs[test_name] = test_dir
        if not os.path.exists(test_dir):
            os.makedirs(test_dir)

        # find out the location of params for this test
        # param_location_map : Map from one of these 'Strategy' -> [(idx, section, param_name)],'Signali' to [ [(idx, section, param_name)], [(idx, param_name)], [(idx, param_name)] ]
        param_location_map = get_all_param_locations(base_agg_config_path, test_to_combinations_map[test_name][0])

        for i in range(len(test_to_combinations_map[test_name][1])):
            expt_dir = test_dir + str(i) + '/'
            if not os.path.exists(expt_dir):
                os.makedirs(expt_dir)
            new_agg_config_path = expt_dir + os.path.basename(base_agg_config_path)
            test_to_agg_config_list_map[test_name].append(new_agg_config_path) # save the agg path
            shutil.copyfile(base_agg_config_path, new_agg_config_path)
            agg_config = ConfigParser.ConfigParser() # Read aggregator config
            agg_config.optionxform = str
            agg_config.readfp(open(new_agg_config_path, 'r'))
            if not agg_config.has_option('Strategy','signal_configs'):
                sys.exit('something wrong. Signal config path not present in agg config')
            _signal_configs = agg_config.get('Strategy','signal_configs').split(',')

            _signal_config_string = []            
            for k in range(len(_signal_configs)):
                _new_signal_path = expt_dir + os.path.basename(_signal_configs[k])
                _old_signal_path = _signal_configs[k]
                shutil.copyfile(_old_signal_path, _new_signal_path)
                _signal_config_string.append(_new_signal_path)
            _signal_config_string = ','.join(_signal_config_string)
            agg_config.set('Strategy','signal_configs', _signal_config_string)

            # change variables in agg config
            if 'Strategy' in param_location_map.keys():
                for idx, section, param_name in param_location_map['Strategy']:
                    if not agg_config.has_option(section, param_name):
                        sys.exit('something wrong in agg config section')
                    agg_config.set(section, param_name, test_to_combinations_map[test_name][1][i][idx])
                    # Write the agg config file with modifications
            with open(new_agg_config_path, 'wb') as configfile:
                agg_config.write(configfile)

            signal_configs = agg_config.get('Strategy','signal_configs').split(',')
            for j in range(len(signal_configs)):
                new_signal_config_path = signal_configs[j]
                signal_config = ConfigParser.ConfigParser() # Read signal configs
                signal_config.optionxform = str
                signal_config.readfp(open(new_signal_config_path, 'r'))
                if not signal_config.has_option('Parameters','paramfilepath'):
                    sys.exit('something wrong! No paramfilepath in signal config')
                _old_paramfilepath = signal_config.get('Parameters','paramfilepath').replace("~", os.path.expanduser("~"))
                _new_paramfilepath = expt_dir + os.path.splitext(os.path.basename(new_signal_config_path))[0] + '-' + os.path.basename(_old_paramfilepath)
                shutil.copyfile(_old_paramfilepath, _new_paramfilepath) # copy paramfile of signal config to new destination
                _old_modelfilepath = signal_config.get('Strategy','modelfilepath').replace("~", os.path.expanduser("~"))
                _new_modelfilepath = expt_dir + os.path.splitext(os.path.basename(new_signal_config_path))[0] + '-' + os.path.basename(_old_modelfilepath)
                shutil.copyfile(_old_modelfilepath, _new_modelfilepath) # copy modelfile of signal config to new destination
                signal_config.set('Parameters', 'paramfilepath', _new_paramfilepath)
                signal_config.set('Strategy', 'modelfilepath', _new_modelfilepath)
                signal_section = 'Signal%d' % (j+1)
                with open(new_signal_config_path, 'wb') as configfile:
                    signal_config.write(configfile)
                if signal_section in param_location_map.keys(): 
                    for idx, section, param_name in param_location_map[signal_section][0]: # Signal config params
                        if not signal_config.has_option(section, param_name):
                            sys.exit('something wrong in signal config section')
                        signal_config.set(section, param_name, test_to_combinations_map[test_name][1][i][idx]) 
                    for idx, param_name in param_location_map[signal_section][1]: # Signal paramfile params
                        new_line = '%s %s' % (param_name, test_to_combinations_map[test_name][1][i][idx])
                        status = replaceAll(_new_paramfilepath, param_name, new_line)
                        if not status:
                            sys.exit('something wrong! could not find param in paramfile')
                    for idx, param_name in param_location_map[signal_section][2]: # Signal modelfile params
                        new_line = '%s %s' % (param_name, test_to_combinations_map[test_name][1][i][idx])
                        status = replaceAll(_new_modelfilepath, param_name, new_line)
                        if not status:
                            sys.exit('something wrong! could not find param in modelfile')
                # Write the signal file with modifications at the end
                with open(new_signal_config_path, 'wb') as configfile:
                    signal_config.write(configfile)
    return test_dirs, test_to_agg_config_list_map

def get_perf_stats(agg_configs):
    """For each set of values to be tested,set up the config files and run simulator to get the perf stats"""
    performance_stats = []
    for agg_config in agg_configs:
        proc = subprocess.Popen(['python', '-W', 'ignore', 'simulator.py', agg_config ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        performance_stats.append(parse_results(proc.communicate()[0])) # TODO parse results should only parse final_order stats
    return performance_stats

def save_perf_stats(test_dir, perf_stats):
    """Save the perf stats corresponding to each param set to a file"""
    f = open(test_dir + 'stats', 'w')
    for i in range(len(perf_stats)):
        f.write('Expt %d:\n' % i)
        for elem in final_order: # TODO should output directly
            if elem in perf_stats[i].keys():
                f.write(elem + ': ' + perf_stats[i][elem] + '\n')
            else:
                print "Something wrong! missing %s"%elem
        f.write('\n\n')
    f.close()

def print_perf_stats(perf_stats):
    """Print out the perf stats in specified order to terminal"""
    for elem in final_order:
        print elem + ': ' + perf_stats[elem]

def impose_constraints(perf_stats, cons_greater, cons_less):
    """Filter out the param sets which do not satisfy the constraints"""
    success_indices = []
    for j in range(len(perf_stats)):
        cons_satisfied = True
        if cons_greater is not None:
            i = 0
            while i < len(cons_greater):
                key = stats[cons_greater[i]][0]
                val = float(perf_stats[j][key].strip(' ').strip('%').strip('\n'))
                if val < float(cons_greater[i+1]):
                    cons_satisfied = False
                i += 2
        if cons_less is not None:
            i = 0
            while i < len(cons_less):
                key = stats[cons_less[i]][0]
                val = float(perf_stats[j][key].strip(' ').strip('%').strip('\n'))
                if val > float(cons_less[i+1]):        
                    cons_satisfied = False
                i += 2
        if cons_satisfied:
            success_indices.append(j)
    return success_indices

def optimize_perf_stats(perf_stats, success_indices, stat):
    """Select optimum parameter set based in performance of 'stat' """
    if stat is None:
        return success_indices
    opt_idx = []
    sign = stats[stat][1]
    key = stats[stat][0]
    if sign == '+':
        opt_val = -1000000000000
    elif sign == '-':
        opt_val = 1000000000000
    else:
        sys.exit('Cannot optimize this stat')
    for idx in success_indices:
        val = float(perf_stats[idx][key].strip(' ').strip('%').strip('\n'))
        if sign == '+' and val > opt_val:
            opt_idx = [idx]
            opt_val = val
        elif sign == '-' and val < opt_val:
            opt_idx = [idx]
            opt_val = val
        elif val == opt_val:
            opt_idx.append(idx)
    return opt_idx

def plot_perf_stats(perf_stats, stat, dest_dir):
    x = []
    y = []
    key = stats[stat][0]
    for i in range(len(perf_stats)):
        y.append(float(perf_stats[i][key].strip(' ').strip('%').strip('\n')))
    x = range(len(y))
    plt.scatter(x,y)
    plt.xlabel("Expt No.")
    plt.ylabel(stat)
    plt.savefig(dest_dir + 'plot.png')

def main():
    """Following tasks are performed in order
    1) Make dir 'base' in /spare/local/logs/param_file/ and copy the following configs : agg_config, signal_configs(+param, +model) and get new_agg_config_path
    2) Read Tests section of permutparamfile and for each test generate combinations of all the variables mentioned in the test, make a folder for each generated combination in the /spare/local/logs/param_file/test_name/ and copy the new configs to that folder
    3) Run simulator for each folder like /spare/local/logs/param_file/test_name/xxx/, accumulate the perf stats
    4) Save all the perf stats, and mappings
    5) Optimize the perf stats for each set of perf stats(corresponding to each test)
    6) Show the results corresponding to each test
    """

    if len(sys.argv) < 2:
        print "Arguments needed: agg_config_file param_file\nStats Allowed:\n%s\n" % ('\n'.join(stats.keys()))
    
    parser = argparse.ArgumentParser()
    parser.add_argument('agg_config_file')
    parser.add_argument('param_file')
    parser.add_argument('-o', nargs=1, help='Optimize this parameter\nEg: -o sharpe', dest='optimize')
    parser.add_argument('-ge', nargs='*', help='Greater than equal to parameter constraint\nEg: -gt sharpe 1 ann_ret 4', dest='greater')
    parser.add_argument('-le', nargs='*', help='Less than equal to parameter constraint\nEg: -lt max_dd 10', dest='less')
    parser.add_argument('-p', nargs=1, help='Plot the param sets versus this stat\nEg: -p sharpe', dest='plot')
    args = parser.parse_args()

    agg_config_path = sys.argv[1].replace("~", os.path.expanduser("~"))
    permutparam_config_path = sys.argv[2].replace("~", os.path.expanduser("~"))

    dest_dir = '/spare/local/logs/' + os.path.splitext(os.path.basename(permutparam_config_path))[0]+'/' # directory to store output files
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # TODO empty directory before starting
    # TODO use ordered dict
    new_agg_config_path = copy_config_files(agg_config_path, dest_dir)
    test_to_combinations_map = generate_test_combinations(permutparam_config_path)
    test_dirs, test_to_agg_config_list_map = generate_test_configs(new_agg_config_path, test_to_combinations_map, dest_dir)
    for test_name in test_to_agg_config_list_map.keys(): # Select the best operf stat for each test
        perf_stats = get_perf_stats(test_to_agg_config_list_map[test_name])
        save_perf_stats(test_dirs[test_name], perf_stats)
        _param_string = [str(elem0) + ':' + str(elem1) for elem0,elem1 in test_to_combinations_map[test_name][0]]
        _test_results = 'Test Name %s\nParam Order: %s\n' % (test_name, _param_string)
        success_indices = impose_constraints(perf_stats, args.greater, args.less)
        if args.less is None and args.greater is None and args.optimize is None:
            args.optimize = ['sharpe']
        opt_indices = optimize_perf_stats(perf_stats, success_indices, args.optimize[0])
        if len(opt_indices) == 0:
            _test_results += 'No Params selected\n'
        else:
            _test_results += 'Selected Expt Nos: %s\n' % (opt_indices)
            for idx in opt_indices:
                _test_results += 'Expt %d Param Values: %s\n' % (idx, test_to_combinations_map[test_name][1][idx])
        print _test_results + '\n'
        f = open(test_dirs[test_name] + 'results', 'w')
        f.write(_test_results)
        f.close()
        if args.plot is not None:
            plot_perf_stats(perf_stats, args.plot[0], test_dirs[test_name])

if __name__ == '__main__':
    main()