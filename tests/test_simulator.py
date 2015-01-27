#!/usr/bin/env python

import os
import sys
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from simulator import Simulator

directory = os.path.abspath("..")

@pytest.fixture
def setup1():
    _config_file = directory + "/modeling/strategies/A1_MVO_all_rb1_model1_rmp_profile1.cfg"
    sim1 = Simulator(_config_file)
    sim1.run()
    return round(sim1._tradelogic_instance.performance_tracker.net_returns,2), \
           round(sim1._tradelogic_instance.performance_tracker.sharpe,2), \
           round(sim1._tradelogic_instance.performance_tracker.sortino,2)


def test_mvo_config1():
    """ 
    Testing MVO with config1 
    """
    net_returns, sharpe, sortino = setup1()

    assert net_returns == 135.92
    assert sharpe == 1.09
    assert sortino == 1.55

@pytest.fixture
def setup2():
    _config_file = directory + "/modeling/strategies/A1_TRVP_all_rb1_model1_rmp_profile1.cfg"
    sim1 = Simulator(_config_file)
    sim1.run()
    return round(sim1._tradelogic_instance.performance_tracker.net_returns,2), \
           round(sim1._tradelogic_instance.performance_tracker.sharpe,2), \
           round(sim1._tradelogic_instance.performance_tracker.sortino,2)


def test_trvp_config1():
    """ 
    Testing TRVP with config1 
    """
    net_returns, sharpe, sortino = setup2()

    assert net_returns == 190.53
    assert sharpe == 1.20
    assert sortino == 1.74
