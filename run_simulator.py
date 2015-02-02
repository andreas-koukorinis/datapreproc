#!/usr/bin/env python
import os
import sys
import argparse
from subprocess import call

def __main__():
    call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    from simulator import Simulator
    # Get handle of config file
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('-sd', type=str, help='Sim Start date\nEg: -sd 2014-06-01\n Default is config_start_date',default=None, dest='sim_start_date')
    parser.add_argument('-ed', type=str, help='Sim End date\nEg: -ed 2014-10-31\n Default is config end_date',default=None, dest='sim_end_date')    
    parser.add_argument('-o', type=str, help='Json Output path\nEg: -o ~/logs/file.json\n Default is in log dir',default=None, dest='json_output_path')
    args = parser.parse_args()
    sim = Simulator(args.config_file, args.sim_start_date, args.sim_end_date, args.json_output_path)
    sim.run()

if __name__ == '__main__':
    __main__()
