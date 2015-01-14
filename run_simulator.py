#!/usr/bin/env/python
import os
import sys
from subprocess import call

def __main__():
    call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    from simulator import Simulator
    sim = Simulator(sys.argv[1])
    sim.run()

if __name__ == '__main__':
    __main__()
