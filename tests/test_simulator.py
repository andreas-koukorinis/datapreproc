#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.global_variables import Globals
from simulator import Simulator

directory = os.path.abspath("..")

def test_mvo_config1():
    """ 
    Testing MVO with config1 
    """
    # Set config file path
    _config_file = directory + "/modeling/strategies/A1_MVO_all_rb1_model1_rmp_profile1.cfg"

    # Run simulator
    sim1 = Simulator(_config_file)
    sim1.run()
    assert round(sim1._tradelogic_instance.performance_tracker.net_returns,2) == 138.03
    assert round(sim1._tradelogic_instance.performance_tracker.sharpe,2) == 0.87
    assert round(sim1._tradelogic_instance.performance_tracker.sortino,2) == 1.22

def test_trvp_config1():
    """ 
    Testing TRVP with config1 
    """
    # Set config file path
    _config_file = directory + "/modeling/strategies/A1_TRVP_all_rb1_model1_rmp_profile1.cfg"
    # Run simulator
    sim1 = Simulator(_config_file)
    sim1.run()
    assert round(sim1._tradelogic_instance.performance_tracker.net_returns,2) == 161.08
    assert round(sim1._tradelogic_instance.performance_tracker.sharpe,2) == 0.84
    assert round(sim1._tradelogic_instance.performance_tracker.sortino,2) == 1.20
