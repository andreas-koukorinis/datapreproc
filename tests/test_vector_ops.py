#!/usr/bin/env python

import os
import sys
import numpy as np
from cvxopt import matrix

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils.vector_ops import lag, shift_vec

def test_lag_1():
    """ 
    Testing lag in vector_ops for correct result with corect input 
    """
    assert lag([1,2,3,4],2) == [0,0,1,2]

def test_lag_2():
    """ 
    Testing lag in vector_ops with more lags than length of list 
    """
    assert lag([1,2,3],5) == [0,0,0]

def test_lag_3():
    """ 
    Testing lag in vector_ops with negative lags 
    """
    assert lag([1,2,3],-5) == [1,2,3]

def test_shift_vec_1():
    """ 
    Testing shift_vec in vector_ops for correct result with corect input 
    """
    # Converting to NumPy as curently CVXOPT doesn't have matrix equality check
    assert (np.array(shift_vec([1,2,3],3)) == np.array([[1,0,0],[2,1,0],[3,2,1]])).all()