#!/usr/bin/env python
import os
import sys
from subprocess import call

def __main__():
    call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    from simulator import Simulator
    if len(sys.argv) < 2 :
        sys.exit("config_file <trading-startdate trading-enddate>")
    # Get handle of config file
    _config_file = sys.argv[1]
    if len(sys.argv) >= 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
        sim = Simulator(_config_file, _start_date, _end_date)
    else:
        sim = Simulator(_config_file)
    sim.run()

if __name__ == '__main__':
    __main__()
