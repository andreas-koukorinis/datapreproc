#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.expanduser("~") + '/stratdev/')

from simulator import Simulator

def test_trvp_config1() :
    """ 
    Testing TRVP with config1 
    """
    # Set config file path
    _config_file = "../modeling/strategies/A1_TRVP_all_rb1_model1_rmp_profile1.cfg"
    # Run simulator
    sim1 = Simulator(_config_file)
    sim1.run()
    assert round(sim1._tradelogic_instance.performance_tracker.net_returns,2) == 262.97
    assert round(sim1._tradelogic_instance.performance_tracker.sharpe,2) == 0.85
    assert round(sim1._tradelogic_instance.performance_tracker.sortino,2) == 1.21
    