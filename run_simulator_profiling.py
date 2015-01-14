#!/usr/bin/env/python
import os
import sys
from subprocess import call
import pstats, cProfile

def __main__():
    call(["python", "setup.py","build_ext","--inplace"],stdout=open(os.devnull, 'w'))
    from simulator import Simulator
    sim = Simulator(sys.argv[1])
    cProfile.runctx("sim.run()", globals(), locals(), "Profile.prof")
    s = pstats.Stats("Profile.prof")
    s.strip_dirs().sort_stats("time").print_stats()

if __name__ == '__main__':
    __main__()
