#!/usr/bin/env python
import numpy
class SignalAlgorithm(object):
    def __init__(self):
        self.products=['E','Z']
        self.weights = dict([(_product, 0.0) for _product in self.products])
        
class Alpha(object):
    def __init__(self):
        self.products=['E','Z']
        self.past_relative_contribution=[]
        self.past_relative_contribution.append(dict([(_product, 0.0) for _product in self.products])) #signal1
        self.past_relative_contribution.append(dict([(_product, 0.0) for _product in self.products])) #signal2
        
    def update_past_relative_contribution(self, _new_signal_contributions, _new_portfolio_weights, _new_portfolio_abs_weights):
        for i in range(len(_new_signal_contributions)):
            for _product in self.products:
                if _new_portfolio_abs_weights[_product] < 0.00000001:
                    self.past_relative_contribution[i][_product] = 0
                elif(_new_portfolio_abs_weights[_product] > 0.0000001 and abs(_new_portfolio_weights[_product]) < 0.0000001):
                    self.past_relative_contribution[i][_product] = _new_signal_contributions[i][_product]
                else:
                    self.past_relative_contribution[i][_product] = _new_signal_contributions[i][_product]/_new_portfolio_weights[_product]

    def get_new_portfolio_weights(self, _signal_rebalancing_day, _current_portfolio_weights, _signals, _signal_allocations):
        _new_signal_contributions = []
        _new_portfolio_weights = dict([(_product, 0.0) for _product in self.products])
        _new_portfolio_abs_weights = dict([(_product, 0.0) for _product in self.products])
        for i in range(len(_signals)):
            _new_signal_contributions.append(dict([(_product, 0.0) for _product in self.products]))
            if _signal_rebalancing_day[i]:
                for _product in self.products:
                    _new_signal_contributions[i][_product] = _signal_allocations[i] * _signals[i].weights.get(_product, 0.0)
                    _new_portfolio_weights[_product] += _new_signal_contributions[i][_product]
                    _new_portfolio_abs_weights[_product] += abs(_new_signal_contributions[i][_product])
            else:
                for _product in self.products:
                    if abs(_current_portfolio_weights[_product]) < 0.0000001 and self.past_relative_contribution[i][_product] != 0: #shoudl change the != 0 here
                        _new_signal_contributions[i][_product] = self.past_relative_contribution[i][_product]
                    else:
                        _new_signal_contributions[i][_product] = _current_portfolio_weights[_product] * self.past_relative_contribution[i][_product]
                    _new_portfolio_weights[_product] += _new_signal_contributions[i][_product]
                    _new_portfolio_abs_weights[_product] += abs(_new_signal_contributions[i][_product])
        self.update_past_relative_contribution(_new_signal_contributions, _new_portfolio_weights, _new_portfolio_abs_weights)
        return _new_portfolio_weights

def __main__():
    _alpha = Alpha()

    current_portfolio_weights={}
    signal_allocations = numpy.array([0.5,0.5])
    signals=[]
    signals.append(SignalAlgorithm())
    signals.append(SignalAlgorithm())
    
    #these are not needed since they are going to be rebalanced
    current_portfolio_weights['E'] = 0.5
    current_portfolio_weights['Z'] = 0.5
    #
    signals[0].weights['E']=0.2
    signals[0].weights['Z']=0.7
    signals[1].weights['E']=-0.2
    signals[1].weights['Z']=0.9
    signal_rebalancing_day = [1,1]
    _x = _alpha.get_new_portfolio_weights(signal_rebalancing_day, current_portfolio_weights, signals, signal_allocations)
    print ("%f %f" %(_x['E'], _x['Z']))
    
    _x['E'] = _x['E']*1.2
    _x['Z'] = _x['Z']*0.8

    # make them add upto 1
    _x['E'] = _x['E'] / ( _x['E'] + _x['Z'] )
    _x['Z'] = _x['Z'] / ( _x['E'] + _x['Z'] )

    # introduce some market movement
    current_portfolio_weights['E'] = _x['E']
    current_portfolio_weights['Z'] = _x['Z']
    
    signals[0].weights['E']=0.3
    signals[0].weights['Z']=0.7
    signals[1].weights['E']=-0.2
    signals[1].weights['Z']=0.9
    signal_rebalancing_day = [1,0]
    _x = _alpha.get_new_portfolio_weights(signal_rebalancing_day, current_portfolio_weights, signals, signal_allocations)
    print ("%f %f" %(_x['E'], _x['Z']))

__main__();
