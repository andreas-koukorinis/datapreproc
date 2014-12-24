import sys
import datetime
from Utils import defaults
from risk_manager_algorithm import RiskManagerAlgo

class RiskLevel(object):
    '''Variables that encapsulate current risk level'''
    def __init__(self, _capital_allocation_level, _max_historical_drawdown):
        self.capital_allocation_level = _capital_allocation_level
        self.max_historical_drawdown = _max_historical_drawdown

class RiskManagerPaper(RiskManagerAlgo):
    '''In this risk manager algorithm, we look at the cumulative log returns of simple_performance_tracker over the specified days.
    Depending on which risk level it falls in, we switch to that level, making sure that once we fall to a risk level,
    we do not raise risk again in the next reallocation_hysteris_days.
    
       Args:
       capital_allocation_levels=100,75,50,25,0
       drawdown_levels=15,20,25,30
       return_history=252
       reallocation_hysteris_days=45

    '''

    def init(self, _config):
        """
        1. read the parameters: return_history, drawdown_levels, capital_allocation_levels
        2. setup risk_level_vec
        """
        
        self.return_history = int(defaults.RETURN_HISTORY)
        if _config.has_option('RiskManagement', 'return_history'):
            self.return_history = _config.getint('RiskManagement', 'return_history')

        self.reallocation_hysteris_days = int(defaults.RISK_MANAGER_REALLOCATION_HYSTERISIS_DAYS)
        if _config.has_option('RiskManagement', 'reallocation_hysteris_days'):
            self.reallocation_hysteris_days = _config.getint('RiskManagement', 'reallocation_hysteris_days')
        
        _drawdown_levels = list(defaults.DRAWDOWN_LEVELS)
        if _config.has_option('RiskManagement', 'drawdown_levels'):
            _drawdown_levels = _config.get('RiskManagement', 'drawdown_levels').split(',')
        self.drawdown_levels = ([float(x) for x in _drawdown_levels])
        #check that drawdown levels are in increasing order
        for i in xrange(1, len(self.drawdown_levels)):
            if self.drawdown_levels[i-1] >= self.drawdown_levels[i]:
                # something wrong! Drawdown levels shoudl be in ascending order.
                print("Drawdown levels should be in increasing order. They seem to not be so! %s" %(','.join(_drawdown_levels)))
                sys.exit(0)

        _capital_allocation_levels = list(defaults.CAPITAL_ALLOCATION_LEVELS)
        if _config.has_option('RiskManagement', 'capital_allocation_levels'):
            _capital_allocation_levels = _config.get('RiskManagement', 'capital_allocation_levels').split(',')
        _capital_allocation_level_vec = [float(x) for x in _capital_allocation_levels] # convert to float
        #check that the last risk level is 0 and first allocation is 100
        if _capital_allocation_level_vec[0] < 100:
            _capital_allocation_level_vec.insert(0, 100.0) # Highest risk is 100% allocation
        if _capital_allocation_level_vec[-1] > 1:
            _capital_allocation_level_vec.append(0.0) # Liquidate on last level
        #check that the capital allocation levels are in descending order
        for i in xrange (1, len(_capital_allocation_level_vec)):
            if _capital_allocation_level_vec[i-1] <= _capital_allocation_level_vec[i]:
                # something wrong since the capital_allocation_levels should be in descending order
                print("Capital allocation levels should be in descending order. They seem to not be so! %s" %(','.join(_capital_allocation_levels)))
                sys.exit(0)

        #setup the risk level array
        self.risk_level_vec = []
        _current_max_drawdown = 100
        for i in xrange(0,len(_capital_allocation_level_vec)):
            _capital_allocation_level = _capital_allocation_level_vec[i]
            if i < len(self.drawdown_levels):
                _current_max_drawdown = self.drawdown_levels[i]
            else:
                _current_max_drawdown = 100
            _this_risk_level = RiskLevel(_capital_allocation_level, _current_max_drawdown)
            self.risk_level_vec.append(_this_risk_level)

        self.current_risk_level_index = 0 # highest risk level. Unfortunately the highest risk level has the smallest index
        self.current_capital_allocation_level = 100.0 # Fully allocated initially
        self.last_risk_level_updated_date = datetime.datetime.fromtimestamp(0).date() 

    def get_risk_level_index_from_drawdown(self,_current_drawdown):
        """private function, that returns the correct index of the risk level array based on the current drawdown level"""
        _retindex = len(self.risk_level_vec)-1
        # start at lowest risk level
        # and keep trying a higher risk level as long as the max drawdown of that level will be higher 
        while (_retindex > 0) and (self.risk_level_vec[(_retindex - 1)].max_historical_drawdown > _current_drawdown):
            _retindex = _retindex - 1
        # _retindex cannot be < 0
        # _retindex cannot be >= len(self.risk_level_vec)
        # Hence _retindex is a valid index to the array
        return (_retindex)
        
    def get_current_risk_level(self, _date):
        """public function, called by execlogic to compute the alloctaion level"""
        # first check if we have already done the computation today. If so then return previously computed value.
        if self.last_risk_level_updated_date == _date:
            return (self.current_capital_allocation_level)
        else:
            self.last_risk_level_updated_date = _date

        # get current drawdown from performance tracker
        _current_drawdown = self.simple_performance_tracker.get_current_drawdown()
        # what shoudl be our risk level now
        _target_risk_level_index = self.get_risk_level_index_from_drawdown(_current_drawdown)
        
        if _target_risk_level_index > self.current_risk_level_index:
            # if risk should be lower then switch to lower risk
            print ("%s switching to lower risk level %d (level:%d) since current dd= %f" %( _date, _target_risk_level_index, self.risk_level_vec[_target_risk_level_index].capital_allocation_level, _current_drawdown ))
            self.current_risk_level_index = _target_risk_level_index
            self.current_capital_allocation_level = self.risk_level_vec[self.current_risk_level_index].capital_allocation_level
            self.last_date_risk_level_change = _date
        elif _target_risk_level_index < self.current_risk_level_index:
            if _target_risk_level_index < 0:
                # this cannot happen since currently the function _get_risk_level_index_from_drawdown has
                # been written in such a way but always good to check.
                print ("error in risk_manager_paper.py") #TODO improve error reporting
                sys.exit(0)

            # if the risk should be higher than current level
            # check if reallocation_hysteris_days have passed since the last time we changed the risk level
            # if so then change the risk level
            if (_date - self.last_date_risk_level_change).days >= self.reallocation_hysteris_days:
                # Now we are allowed to switch to the higher risk level
                print ("%s switching to higher risk level %d (level:%d) since current dd= %f" %( _date, _target_risk_level_index, self.risk_level_vec[_target_risk_level_index].capital_allocation_level, _current_drawdown ))
                self.current_risk_level_index = self.current_risk_level_index - 1 #only raise one risk level at a time
                self.current_capital_allocation_level = self.risk_level_vec[self.current_risk_level_index].capital_allocation_level
                self.last_date_risk_level_change = _date

        _retval = self.current_capital_allocation_level
        if self.simple_performance_tracker.get_desired_leverage() > self.maximum_allowed_leverage:
            print ("didn't expect to see desired leverage %f to exceed maximum allowed leverage %f" %(self.simple_performance_tracker.get_desired_leverage(), self.maximum_allowed_leverage))
            _retval = min (_retval, (self.maximum_allowed_leverage/self.simple_performance_tracker.get_desired_leverage()))
        return (_retval)
