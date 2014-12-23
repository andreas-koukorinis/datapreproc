#!/usr/bin/env python
import numpy

def correct_signs_weights(_current_erc_weights, _zc_weight_vector):
    """Given two numpy arrays of equal length return the vector closest to the first that has the same signs as the second"""
    _retval = _current_erc_weights
    if(len(_current_erc_weights) == len(_zc_weight_vector)):
        _zc_weight_vector = _zc_weight_vector * numpy.sum(numpy.abs(_current_erc_weights))/numpy.sum(numpy.abs(_zc_weight_vector)) # scale it to the same leverage
        #print ( "_zc_weight_vector * erc_weights %s" %([ str(x) for x in _zc_weight_vector * erc_weights ]) )
        #print ( "_zc_weight_vector * _zc_weight_vector %s" %([ str(x) for x in _zc_weight_vector * _zc_weight_vector ]) )
        y = -(_zc_weight_vector * erc_weights)/(_zc_weight_vector * _zc_weight_vector)
        #print ( "-(_zc_weight_vector * erc_weights)/(_zc_weight_vector * _zc_weight_vector) %s" %([ str(x) for x in y ]) )
        _zc_weight_multiplier = max( -(_zc_weight_vector * erc_weights)/(_zc_weight_vector * _zc_weight_vector))
        if _zc_weight_multiplier > 0.001:
            #print ( "zcmult %f" %(_zc_weight_multiplier))
            _retval = (_current_erc_weights + ( _zc_weight_multiplier * _zc_weight_vector ))/(1+_zc_weight_multiplier)
    return _retval
        

erc_weights = numpy.array([0.1]*10)
allocation_signs = numpy.ones(10)

allocation_signs[3]=-1
erc_weights[4]=-1
if sum(numpy.abs(numpy.sign(erc_weights)-numpy.sign(allocation_signs))) > 0:
    print ( "Sign-check-fail: weights %s" %([ str(x) for x in erc_weights ]) )

erc_weights=correct_signs_weights(erc_weights, allocation_signs)
print ( "corrected weights %s" %([ str(x) for x in erc_weights ]) )