#!/usr/bin/env python
import numpy

erc_weights = numpy.array([0.1]*10)
allocation_signs = numpy.ones(10)

allocation_signs[3]=-1
erc_weights[4]=-1
if sum(numpy.abs(numpy.sign(erc_weights)-numpy.sign(allocation_signs))) > 0:
    print ( "Sign-check-fail: weights %s" %([ str(x) for x in erc_weights ]) )
        