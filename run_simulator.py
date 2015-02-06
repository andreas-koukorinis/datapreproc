#!/usr/bin/env python
import os
import sys
import argparse
from subprocess import call

def __main__():
    ret_code = call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    if ret_code != 0:
        sys.exit('COMPILATION TERMINATED')
    from simulator import Simulator
    # Get handle of config file
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('-sd', type=str, help='Sim Start date\nEg: -sd 2014-06-01\n Default is config_start_date',default=None, dest='sim_start_date')
    parser.add_argument('-ed', type=str, help='Sim End date\nEg: -ed 2014-10-31\n Default is config end_date',default=None, dest='sim_end_date')    
    parser.add_argument('-o', type=str, help='Json Output path\nEg: -o ~/logs/file.json\n Default is in log dir',default=None, dest='json_output_path')
    parser.add_argument('-logs', type=str, help='Logs Output path\nEg: -logs ~/logs/\n Default is in log dir',default=None, dest='logs_output_path')
    args = parser.parse_args()
    if args.logs_output_path is None:
        args.logs_output_path = '~/logs/'
    args.logs_output_path = (args.logs_output_path + os.path.splitext(args.config_file)[0].split('/')[-1] + '/').replace('~', os.path.expanduser('~'))
    if args.json_output_path is None:
        args.json_output_path = args.logs_output_path + 'output.json'
    sim = Simulator(args)
    sim.run()

if __name__ == '__main__':
    __main__()
