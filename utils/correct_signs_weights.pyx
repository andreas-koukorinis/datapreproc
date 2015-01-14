import numpy
def correct_signs_weights(_current_erc_weights, _zc_weight_vector):
    """Given two numpy arrays of equal length return the vector closest to the first that has the same signs as the second

    Arguments:
    _current_erc_weights : the weights we would have reported without checking for signs
    _zc_weight_vector : the vector with the right signs

    Algorithm:
    Find the smallest positive number k such that (_current_erc_weights + k*_zc_weight_vector)/(1+k) has the same signs as _zc_weight_vector
    by taking the max k_i where k_i does the trick for the ith entry.

    """
    _retval = _current_erc_weights
    if(len(_current_erc_weights) == len(_zc_weight_vector)):
        _zc_weight_vector = _zc_weight_vector * numpy.sum(numpy.abs(_current_erc_weights))/numpy.sum(numpy.abs(_zc_weight_vector)) # scale it to the same leverage
        #print ( "_zc_weight_vector * erc_weights %s" %([ str(x) for x in _zc_weight_vector * erc_weights ]) )
        #print ( "_zc_weight_vector * _zc_weight_vector %s" %([ str(x) for x in _zc_weight_vector * _zc_weight_vector ]) )
        y = -(_zc_weight_vector * _current_erc_weights)/(_zc_weight_vector * _zc_weight_vector)
        #print ( "-(_zc_weight_vector * erc_weights)/(_zc_weight_vector * _zc_weight_vector) %s" %([ str(x) for x in y ]) )
        _zc_weight_multiplier = max( -(_zc_weight_vector * _current_erc_weights)/(_zc_weight_vector * _zc_weight_vector))
        if _zc_weight_multiplier > 0.001:
            #print ( "zcmult %f" %(_zc_weight_multiplier))
            _retval = (_current_erc_weights + ( _zc_weight_multiplier * _zc_weight_vector ))/(1+_zc_weight_multiplier)
    return (_retval)
