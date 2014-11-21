from numpy import *
import datetime
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder
from Utils.Regular import get_next_futures_contract,get_first_futures_contract,is_future_entity,is_future

# Track the daily log returns for the product
class DailyLogReturns( DailyBookListener ):

    instances = {}

    def __init__( self, _identifier, _startdate, _enddate, _config ):
        self.listeners = []
        self.values = []
        self.prices = [0,0]  # Remember last two prices for the product #prices[0] is latest
        self.identifier = _identifier
        params = self.identifier.strip().split('.')
        self.product = params[1]
        if is_future( self.product ):
            if is_future_entity( self.product ):
                _product1 = get_first_futures_contract( self.product )
            else:
                _product1 = self.product
            _product2 = get_next_futures_contract( _product1 )        
            self.product1 = _product1
            self.product2 = _product2
            self.prices2 = 0.0 # Remember the last trading day price for the next future contract
            BookBuilder.get_unique_instance( self.product1, _startdate, _enddate, _config ).add_dailybook_listener( self )
            BookBuilder.get_unique_instance( self.product2, _startdate, _enddate, _config ).add_dailybook_listener( self )
        else:
            BookBuilder.get_unique_instance ( self.product, _startdate, _enddate, _config ).add_dailybook_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config ):
        if identifier not in DailyLogReturns.instances.keys() :
            new_instance = DailyLogReturns ( identifier, _startdate, _enddate, _config )
            DailyLogReturns.instances[identifier] = new_instance
        return DailyLogReturns.instances[identifier]

    # Update the daily log returns on each ENDOFDAY event
    def on_dailybook_update( self, product, dailybook ):
        _updated = False
        if is_future( self.product ):
            if product == self.product1 : 
                self.prices[1] = self.prices[0]
                self.prices[0] = dailybook[-1][1]
                if(len(dailybook)>1):
                    _yesterday_last_trading_day = dailybook[-2][2]
                else:
                    _yesterday_last_trading_day = False
                if _yesterday_last_trading_day: # If yesterday was the last trading day and price for both kth and k+1th contract has been updated
                    p1 = self.prices[0]
                    p2 = self.prices2
                else:
                    p1 = self.prices[0]
                    p2 = self.prices[1]
                _updated=True
            else:
                if dailybook[-1][2]: 
                    self.prices2 = dailybook[-1][1] # The assumption is that prices of both contracts are avaliable on last trading day
        else:    
            _updated = True
            self.prices[1] = self.prices[0]
            self.prices[0] = dailybook[-1][1]
            p1 = self.prices[0]
            p2 = self.prices[1]
        if _updated:
            if p2 != 0:
                logret = log(p1/p2)
            else:
                logret = 0.0  # If last two prices not available for a product,let logreturn = 0
            self.values.append( ( dailybook[-1][0].date(), logret ) )
            for listener in self.listeners: listener.on_indicator_update( self.identifier, self.values )
