#!/usr/bin/env/python
import re
import sys
import os
import shutil
import argparse
import subprocess
import itertools
import ConfigParser
from io import StringIO
from datetime import date, timedelta

stats = {'sharpe': ('Sharpe Ratio', '+'),  'ret_dd_ratio': ('Return_drawdown_Ratio', '+'), 'max_dd': ('Max Drawdown', '-'), 'net_pnl': ('Net PNL', '+'), 'ret_var_ratio': ('Return Var10 ratio', '+'),'gain_pain_ratio': ('Gain Pain Ratio', '+'), 'hit_loss_ratio': ('Hit Loss Ratio', '+'), 'turnover': ('Turnover', '-'), 'skewness': ('Skewness','?'), 'kurtosis': ('Kurtosis', '?'), 'corr_vbltx': ('Correlation to VBLTX', '?'), 'corr_vtsmx': ('Correlation to VTSMX', '?'), 'ann_std_ret': ('Annualized_Std_Returns', '-'), 'max_dd_dollar': ('Max Drawdown Dollar', '-'), 'ann_pnl': ('Annualized PNL', '+'), 'dml': ('DML', '+'), 'mml': ('MML', '+'), 'qml': ('QML', '+'), 'yml': ('YML', '+'), 'max_num_days_no_new_high' : ('Max num days with no new high', '-')}

final_order = ['Net Returns', 'Total Tradable Days','Sharpe Ratio', 'Return_drawdown_Ratio','Return Var10 ratio','Correlation to VBLTX', 'Correlation to VTSMX', 'Annualized_Returns', 'Annualized_Std_Returns', 'Initial Capital', 'Net PNL', 'Annualized PNL', 'Annualized_Std_PnL', 'Skewness','Kurtosis','DML','MML','QML','YML','Max Drawdown','Drawdown Period','Drawdown Recovery Period','Max Drawdown Dollar','Annualized PNL by drawdown','Yearly_sharpe','Hit Loss Ratio','Gain Pain Ratio','Max num days with no new high','Losing month streak','Turnover','Leverage','Trading Cost','Total Money Transacted','Total Orders Placed','Worst 5 days','Best 5 days','Worst 5 weeks','Best 5 weeks']

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

def get_config_handles_and_names(config_file, param_file, dest_dir):
    """Copies the configs(agg and signal) to a separate folder and returns the handles to the configs(in new path)

    Args:
        config_file(string): The path to the config of the aggregator
        dest_dir(string): The destinatiopn directory for this parameter optimization files.EG: logs/param_file/

    Returns: The list of tuples (config handle, config_name), first handle corresponds to the aggregator, then signals in order, and last one is param file
    """
    config_handles_names = []
    signal_config_names = []
    dest_file = dest_dir + os.path.basename(config_file)
    shutil.copyfile(config_file, dest_file) # copy aggregator config to new destination
    config = ConfigParser.ConfigParser() # Read aggregator config
    config.readfp(open(dest_file, 'r'))
    config_handles_names.append((config, dest_file))
    if config.has_option('Strategy','signal_configs'):
        signal_configs = config.get('Strategy','signal_configs').split(',')
    else:
        sys.exit('something wrong')
    for _config_name in  signal_configs:
        _config_name_ = _config_name.replace("~", os.path.expanduser("~"))
        dest_file = dest_dir + os.path.basename(_config_name_)
        signal_config_names.append(dest_file)
        shutil.copyfile(_config_name_, dest_file) # copy signal configs to new destination
        config = ConfigParser.ConfigParser() # Read signal configs
        config.readfp(open(dest_file, 'r'))
        config_handles_names.append((config, dest_file))

    # Change the signals path in the new aggregator config
    _signal_config_string = ','.join(signal_config_names)
    config_handles_names[0][0].set('Strategy','signal_configs', _signal_config_string)
    with open(config_handles_names[0][1], 'wb') as configfile:
        config_handles_names[0][0].write(configfile)    

    # Read the paramfile
    config = ConfigParser.ConfigParser()
    config.readfp(open(param_file, 'r'))
    config_handles_names.append((config, param_file))
    return config_handles_names

def generate_test_combinations(config_handles_names):
    """Looks at the params file and generates all the pairs of values to be tested

    Args:
        config_handles(list of ConfigParser handles)

    Returns:
        all_param_names(list of tuples): Each tuple in the list is of the form (handle, values).
                                         Eg: [(signal1_handle, rebalance_frequency), (signal2_handle, rebalance_frequency)]
        all_value_combinations(list of tuples): Each tuple refers to one set of corresponding values
                                                Eg: [(1,21), (1,63), (1,252)]
                                                This means that signal1 rb=1 at all times, but signnal2 rb varies among 21,63,252
    """
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
    return all_param_names, all_value_combinations 

def set_configs(param_names, values):
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

def get_perf_stats(all_param_names, all_value_combinations):
    """For each set of values to be tested,set up the config files and run Simulator to get the perf stats"""
    performance_stats = []
    for i in range(len(all_value_combinations)):
        set_configs(all_param_names, all_value_combinations[i])
        proc = subprocess.Popen(['python', '-W', 'ignore', 'Simulator.py', all_param_names[0][1] ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        performance_stats.append(parse_results(proc.communicate()[0]))
    return performance_stats

def save_perf_stats(perf_stats, _param_string, all_value_combinations, dest_dir):
    f = open(dest_dir + 'stats', 'w')
    f.write('Order: ' + _param_string + '\n')
    for i in range(len(all_value_combinations)):
        f.write('Param_set: ' + (' ').join(all_value_combinations[i]) + '\n')
        for elem in final_order:
            f.write(elem + ': ' + perf_stats[i][elem] + '\n')
        f.write('\n\n')
    f.close()

def print_perf_stats(perf_stats):
    for elem in final_order:
        print elem + ': ' + perf_stats[elem]

def impose_constraints(perf_stats, cons_greater, cons_less):
    success_indices = []
    print cons_greater, cons_less
    for j in range(len(perf_stats)):
        cons_satisfied = True
        if cons_greater is not None:
            i = 0
            while i < len(cons_greater):
                key = stats[cons_greater[i]][0]
                val = float(perf_stats[j][key].strip(' ').strip('%').strip('\n'))
                print key, val
                if val < float(cons_greater[i+1]):
                    cons_satisfied = False
                i += 2
        if cons_less is not None:
            i = 0
            while i < len(cons_less):
                key = stats[cons_less[i]][0]
                val = float(perf_stats[j][key].strip(' ').strip('%').strip('\n'))
                print key, val
                if val > float(cons_less[i+1]):        
                    cons_satisfied = False
                i += 2
        if cons_satisfied:
            success_indices.append(j)
    return success_indices

def optimize_perf_stats(perf_stats, success_indices, _param_string, all_value_combinations, stat):
    if stat is None:
        return success_indices
    opt_idx = []
    sign = stats[stat][1]
    if sign == '+':
        opt_val = -1000000000000
    elif sign == '-':
        opt_val = 1000000000000
    else:
        sys.exit('Cannot optimize this stat')
    for idx in success_indices:
        _val = float(perf_stats[idx][key].strip(' ').strip('%').strip('\n'))
        if sign == '+' and _val > opt_val:
            opt_idx = [idx]
        elif sign == '-' and _val < opt_val:
            opt_idx = [idx]
        elif _val == opt_val:
            opt_idx.append(idx)
    return opt_idx

def main():
    if len(sys.argv) < 2:
        print "Arguments needed: agg_config_file param_file\nStats Allowed:\n%s\n" % ('\n'.join(stats.keys()))
    
    parser = argparse.ArgumentParser()
    parser.add_argument('agg_config_file')
    parser.add_argument('param_file')
    parser.add_argument('-o', nargs=1, help='Optimize this parameter', dest='optimize')
    parser.add_argument('-ge', nargs='*', help='Greater than equal to parameter constraint', dest='greater')
    parser.add_argument('-le', nargs='*', help='Less than equal to parameter constraint', dest='less')
    parser.add_argument('-p', nargs=1, help='Plot the param sets versus this stat', dest='plot')
    args = parser.parse_args()

    agg_config_file = sys.argv[1].replace("~", os.path.expanduser("~"))
    param_file = sys.argv[2].replace("~", os.path.expanduser("~"))
    performance_stats = []

    dest_dir = 'logs/' + os.path.splitext(os.path.basename(param_file))[0]+'/' # directory to store output files
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    config_handles_names = get_config_handles_and_names(agg_config_file, param_file, dest_dir)
    all_param_names, all_value_combinations = generate_test_combinations(config_handles_names)
    perf_stats = get_perf_stats(all_param_names, all_value_combinations)
    _param_string = '_'.join([_param_name[2] for _param_name in all_param_names])
    save_perf_stats(perf_stats, _param_string, all_value_combinations, dest_dir)    
    success_indices = impose_constraints(perf_stats, args.greater, args.less)
    if args.less is None and args.greater is None and args.optimize is None:
        args.optimize = ['sharpe']
    opt_indices = optimize_perf_stats(perf_stats, success_indices, _param_string, all_value_combinations, args.optimize[0])

    if len(opt_indices) == 0:
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
    #plot_perf_stats(perf_stats)

if __name__ == '__main__':
    main()
