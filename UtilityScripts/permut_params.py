#!/usr/bin/env/python
import sys
import os
import shutil
import numpy
import argparse
import subprocess
import itertools
import ConfigParser
import matplotlib.pyplot as plt

stats = {'sharpe': ('Sharpe Ratio', '+'),  'ret_dd_ratio': ('Return_drawdown_Ratio', '+'), 'max_dd': ('Max Drawdown', '-'), 'net_pnl': ('Net PNL', '+'), 'ret_var_ratio': ('Return Var10 ratio', '+'),'ann_ret': ('Annualized_Returns','+'), 'gain_pain_ratio': ('Gain Pain Ratio', '+'), 'hit_loss_ratio': ('Hit Loss Ratio', '+'), 'turnover': ('Turnover', '-'), 'skewness': ('Skewness','?'), 'kurtosis': ('Kurtosis', '?'), 'corr_vbltx': ('Correlation to VBLTX', '?'), 'corr_vtsmx': ('Correlation to VTSMX', '?'), 'ann_std_ret': ('Annualized_Std_Returns', '-'), 'max_dd_dollar': ('Max Drawdown Dollar', '-'), 'ann_pnl': ('Annualized PNL', '+'), 'dml': ('DML', '+'), 'mml': ('MML', '+'), 'qml': ('QML', '+'), 'yml': ('YML', '+'), 'max_num_days_no_new_high' : ('Max num days with no new high', '-')}

final_order = ['Net Returns', 'Total Tradable Days','Sharpe Ratio', 'Return_drawdown_Ratio','Return Var10 ratio','Correlation to VBLTX', 'Correlation to VTSMX', 'Annualized_Returns', 'Annualized_Std_Returns', 'Initial Capital', 'Net PNL', 'Annualized PNL', 'Annualized_Std_PnL', 'Skewness','Kurtosis','DML','MML','QML','YML','Max Drawdown','Drawdown Period','Drawdown Recovery Period','Max Drawdown Dollar','Annualized PNL by drawdown','Yearly_sharpe','Hit Loss Ratio','Gain Pain Ratio','Max num days with no new high','Losing month streak','Turnover','Leverage','Trading Cost','Total Money Transacted','Total Orders Placed','Worst 5 days','Best 5 days','Worst 5 weeks','Best 5 weeks']

dependencies = {} # To account for dependencies between sections

def parse_results(results):
    """Parses the performance stats(output of Simulator) and returns them as dict

    Args:
        results(string): The results shown by the Simulator as a single string

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
            _sub_pattern_vals = [_pattern] # Value without brackets
        ret_val.append(_sub_pattern_vals)
    ret_val = list(itertools.product(*ret_val))
    for i in range(len(ret_val)):
        ret_val[i] = (' '.join(list(ret_val[i]))).strip()
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

def generate_test_combinations(base_agg_config_path, permutparam_config_path, dest_dir):
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
    test_to_variable_map = {} # Map from test_name to list of params to be changed in this test
    for test_name in all_tests.keys():
        test_dir = dest_dir + test_name + '/'
        if not os.path.exists(test_dir):
            os.makedirs(test_dir)
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
        test_to_variable_map[test_name] = (test_dir, processed_variables)

    for test_name in test_to_variable_map.keys():
        generate_combinations_for_test(test_to_variable_map[test_name][0], test_to_variable_map[test_name][1], dest_dir + 'base/', os.path.basename(base_agg_config_path), permutparam_config)

    #print test_to_variable_map
    return test_to_variable_map
    '''
    param_handle = config_handles_names[-1][0]
    all_param_names = []
    all_param_values = []
    if param_handle.has_section('Strategy'):
        strategy_params = param_handle.options("Strategy")
        for param in strategy_params:
            all_param_names.append((config_handles_names[0][0], config_handles_names[0][1], param))
            _values = param_handle.get('Strategy', param).split('|')
            all_param_values.append(_values)
    for i in range(1, len(config_handles_names)-1):
        _signal_section = 'Signal%d' % i
        if param_handle.has_section(_signal_section):
            signal_params = param_handle.options(_signal_section)
            for param in signal_params:
                all_param_names.append((config_handles_names[i][0], config_handles_names[i][1], param))
                _values = param_handle.get(_signal_section, param).split('|')
                all_param_values.append(_values)
    all_value_combinations = list(itertools.product(*all_param_values))
    return all_param_names, all_value_combinations'''

def generate_combinations_for_test(test_dir, variables, _base_dir, base_agg_config_name, permutparam_config):
    variable_to_combination_map = {}
    for variable in variables:
        if variable[0] == '*':
            for _section in permutparam_config.sections():
                if _section == 'Tests':
                    continue
                for _var in permutparam_config.options(_section):
                    if permutparam_config.has_option(_section, _var):
                        variable_to_combination_map[(_section, _var)] = parse_variable_values(permutparam_config.get(_section, _var))
                    else:
                        pass
        else:
            _section, var  = variable[0], variable[1]
            if permutparam_config.has_option(_section, var): # If variable is directly specified
                variable_to_combination_map[variable] = parse_variable_values(permutparam_config.get(_section, var))
            else: # Go through each section and check for variable piping
                pass
    print variable_to_combination_map
    return variable_to_combination_map

def set_configs(param_names, values):
    """Change the configs to accomodate for this param set"""
    for i in range(len(param_names)):
        _handle = param_names[i][0]
        _config_name = param_names[i][1]
        _param = param_names[i][2]
        for _section in _handle.sections(): # TODO should not go through all sections
            if _handle.has_option(_section, _param):
                _handle.set(_section, _param, values[i])
                break
            else:
                pass # TODO Add in some section
        with open(_config_name, 'wb') as configfile:
            _handle.write(configfile)

def get_perf_stats(all_param_names, all_value_combinations, agg_config):
    """For each set of values to be tested,set up the config files and run Simulator to get the perf stats"""
    performance_stats = []
    for i in range(len(all_value_combinations)):
        set_configs(all_param_names, all_value_combinations[i])
        proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', agg_config ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        performance_stats.append(parse_results(proc.communicate()[0]))
    return performance_stats

def save_perf_stats(perf_stats, _param_string, all_value_combinations, dest_dir):
    """Save the perf stats corresponding to each param set to a file"""
    f = open(dest_dir + 'stats', 'w')
    f.write('Order: ' + _param_string + '\n')
    for i in range(len(all_value_combinations)):
        f.write('Param_set: ' + (' ').join(all_value_combinations[i]) + '\n')
        for elem in final_order:
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

def optimize_perf_stats(perf_stats, success_indices, _param_string, all_value_combinations, stat):
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

def plot_perf_stats(perf_stats, all_value_combinations, stat, dest_dir):
    x = []
    y = []
    key = stats[stat][0]
    for i in range(len(perf_stats)):
        x.append(' '.join(all_value_combinations[i]))
        y.append(float(perf_stats[i][key].strip(' ').strip('%').strip('\n')))
    x1 = range(len(x))
    plt.xticks(x1,x)
    plt.scatter(x1,y)
    plt.xlabel("Param set")
    plt.ylabel(stat)
    plt.savefig(dest_dir + 'plot.png')

def main():
    """Following tasks are performed in order
    1) Make dir 'base' in /spare/local/logs/param_file/ and copy the following configs : agg_config, signal_configs(+param, +model) and get new_agg_config_path
    2) Read Tests section of permutparamfile and for each test generate combinations of all the variables mentioned in the test, make a folder for each generated combination in the /spare/local/logs/param_file/test_name/ and copy the new configs to that folder
    3) Run Simulator for each folder like /spare/local/logs/param_file/test_name/xxx/, accumulate the perf stats
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

    new_agg_config_path = copy_config_files(agg_config_path, dest_dir)
    test_dirs = generate_test_combinations(new_agg_config_path, permutparam_config_path, dest_dir) # List of paths to each test directory
    #print test_dirs
    '''
    # perf_stats is a dict from test dir path to list of perf stats
    perf_stats = get_perf_stats(test_dirs) # TODO distribute this among different machines and collect results

    #print perf_stats
    save_perf_stats(perf_stats)
    success_indices = impose_constraints(perf_stats, args.greater, args.less)
    if args.less is None and args.greater is None and args.optimize is None:
        args.optimize = ['sharpe']
    opt_indices = optimize_perf_stats(perf_stats, success_indices, args.optimize[0])
    '''
    '''if len(opt_indices) == 0:
        print 'No Params selected'
    else:
        print 'Param Order: %s\n'%(_param_string)
        if len(opt_indices) == 1:
            print 'Selected Param_set: %s\n' % ((' ').join(all_value_combinations[opt_indices[0]]))
            print_perf_stats(perf_stats[opt_indices[0]])
        else:
            print 'Selected Param_sets:\n'
            for idx in opt_indices:
                print (' ').join(all_value_combinations[idx])
    if args.plot is not None:
        plot_perf_stats(perf_stats, all_value_combinations, args.plot[0], dest_dir)
    '''

if __name__ == '__main__':
    main()
