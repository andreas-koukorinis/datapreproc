#!/usr/bin/env/python
import os
import sys
import time
from subprocess import call

def __main__():
    start = time.clock()
    elapsed1 = (time.clock() - start)
    call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    from simulator import Simulator
    elapsed2 = (time.clock() - start)
    sim = Simulator(sys.argv[1])
    elapsed3 = (time.clock() - start)
    sim.run()
    elapsed4 = (time.clock() - start)
    print elapsed1,elapsed2,elapsed3,elapsed4

if __name__ == '__main__':
    __main__()