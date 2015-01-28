#!/usr/bin/env python
import os
import sys
from subprocess import call

def __main__():
    call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    from gendata import Gendata
    if len(sys.argv) < 2 :
        sys.exit("config_file <trading-startdate trading-enddate indicator-file>")
    # Get handle of config file
    _config_file = sys.argv[1]
    if len(sys.argv) >= 4 :
        _start_date = sys.argv[2]
        _end_date = sys.argv[3]
    else:
        _start_date = None
        _end_date = None
    if len(sys.argv) >= 5:
        _indicator_file = sys.argv[4]
    else:
        _indicator_file = None
    sim = Gendata(_config_file, _start_date, _end_date, _indicator_file)
    sim.run()

if __name__ == '__main__':
    __main__()
