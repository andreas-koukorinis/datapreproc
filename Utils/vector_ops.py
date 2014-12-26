"""
Utilities related to vector operations.
As of now has elper functions to succinctly represent constraints for optimizer
"""

from cvxopt import matrix


# Helper functions to succinctly represent constraints for optimizer
def lag(vec, num_lag):
    """ Helper function lag(vec,num_lag)
    Produces a lag of k in a vector x with leading zeros
    Args:
        vec(list): Vector of numbers
        num_lag(int): Numbe of discrete steps of lag to be introduced
    Returns:
        List: Vector of numbers shifted by num_lag padded with zeros
    Example:
        >>> lag([1,2,3,4],2)
        [0,0,1,2]
    """
    if num_lag == 0:
        return vec
    elif num_lag > len(vec):
        return len(vec)*[0]
    else:
        return num_lag*[0]+vec[0:-num_lag]


def shift_vec(vec, num):
    """ Helper function shift_vec(vec,num)
    Produces a matrix of vectors each with a lag i where i ranges from 0 to m-1
    Args:
        vec(list): Vector of numbers
        num(int): Number of lagged vectors to be produced
    Returns:
        mat(matrix): Array of vectors each with a lag i where i ranges from 0 to num-1
    Example:
        >>> shift_vec([1,2,3],3)
        [[1,2,3],[0,1,2],[0,0,1]]'
    """
    length = len(vec)
    mat = [lag(vec, i) for i in range(0, num)]
    mat = matrix(mat, (length, num))
    return mat
