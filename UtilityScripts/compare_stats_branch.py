#!/usr/bin/env/python
import re
import sys
import os
import subprocess
import commands
from prettytable import PrettyTable

def parse_results(results):
    results = results.split('\n')
    _dict_results = {}
    for result in results:
        if '=' in result:
            _result = result.split('=')
            _name = _result[0].strip()
            _val = _result[1].strip()
            _dict_results[_name] = _val
    return _dict_results

def main():
    if len(sys.argv) < 2:
        print "Arguments needed: branch_to_compare_with_beta"
        sys.exit(0)

    _data = PrettyTable(["Config", "Branch", "Sharpe", "Ann_Return", "Ann_Std_Return", "Max_dd", "Ret_dd_Ratio", "Corr_VBLTX", "Corr_VTSMX"])
    branch = sys.argv[1]
    default_branch = "beta"
    output1 = []
    output2 = []
    configs = ["~/modeling/IVWAS/A1_TRVP_all_rb1_tr10_std21.5_corr252.30_rmp.cfg", "~/modeling/IVWAS/A1_TRMSHC_all_rb5_tr10_std63.5_corr252.30.cfg"]
    cmd = 'git checkout %s'%(branch)
    output = commands.getoutput(cmd)
    if 'error' in output:
        print output
        sys.exit('Could not switch to %s.Please commit changes first'%(branch))
    for config in configs:
        cmd = "python Simulator.py %s" % (config)
        output1.append(parse_results(commands.getoutput(cmd)))

    cmd = 'git checkout %s'%(default_branch)
    output = commands.getoutput(cmd)
    if 'error' in output:
        sys.exit('Could not switch to %s.Please commit changes first'%(default_branch))
    for config in configs:
        cmd = "python Simulator.py %s" % (config)
        output2.append(parse_results(commands.getoutput(cmd)))

    for i in range(len(configs)):
        _data.add_row([configs[i], branch, output1[i]['Sharpe Ratio'], output1[i]['Annualized_Returns'], output1[i]['Annualized_Std_Returns'], output1[i]['Max Drawdown'], output1[i]['Return_drawdown_Ratio'], output1[i]['Correlation to VBLTX'], output1[i]['Correlation to VTSMX']])
        _data.add_row([configs[i], default_branch, output2[i]['Sharpe Ratio'], output2[i]['Annualized_Returns'], output2[i]['Annualized_Std_Returns'], output2[i]['Max Drawdown'], output2[i]['Return_drawdown_Ratio'], output2[i]['Correlation to VBLTX'], output2[i]['Correlation to VTSMX']])
    print _data

    cmd = 'git checkout %s'%(branch)
    output = commands.getoutput(cmd)
    if 'error' in output:
        sys.exit('Could not switch to %s.Please commit changes first'%(branch))

if __name__ == '__main__':
    main()
