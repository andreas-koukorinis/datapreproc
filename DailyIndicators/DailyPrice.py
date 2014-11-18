from numpy import *
import datetime
from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder
from Utils.Regular import get_next_futures_contract,get_first_futures_contract,is_future_entity,is_future

# Track the daily price for the product (used in computing moving averages)
class DailyPrice( DailyBookListener ):

    instances = {}

    def __init__( self, _identifier, _startdate, _enddate, _config ):
        self.listeners = []
        self.values = []
        self.prices = [0,0]  # Remember last two prices for the product #prices[0] is latest
        self.dt = [ datetime.datetime.fromtimestamp(1),datetime.datetime.fromtimestamp(1) ] # Last update dt for futures pair
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
            self.prices2 = [0,0] # Remember the last price for the next future contract
            BookBuilder.get_unique_instance( self.product1, _startdate, _enddate, _config ).add_dailybook_listener( self )
            BookBuilder.get_unique_instance( self.product2, _startdate, _enddate, _config ).add_dailybook_listener( self )
        else:
            BookBuilder.get_unique_instance ( self.product, _startdate, _enddate, _config ).add_dailybook_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config ):
        if identifier not in DailyPrice.instances.keys() :
            new_instance = DailyPrice ( identifier, _startdate, _enddate, _config )
            DailyPrice.instances[identifier] = new_instance
        return DailyPrice.instances[identifier]

    # Update the daily price on each ENDOFDAY event
    def on_dailybook_update( self, product, dailybook ):
        updated = False
        if is_future( self.product ):
            if product == self.product1 : 
                self.dt[0] = dailybook[-1][0]
                self.prices[1] = self.prices[0]
                self.prices[0] = dailybook[-1][1]
            else:
                self.dt[1] = dailybook[-1][0]
                self.prices2[1] = self.prices2[0]
                self.prices2[0] = dailybook[-1][1]

            if(len(dailybook)>1):
                _yesterday_settlement = dailybook[-2][2]
            else:
                _yesterday_settlement = False

            if self.dt[0] == self.dt[1]:
                updated = True
                if _yesterday_settlement: # If yesterday was the settlement day and price for both kth and k+1th contract has been updated
                    p1 = self.prices[0]
                    p2 = self.prices2[1]
                else:
                    p1 = self.prices[0]
                    p2 = self.prices[1]
        else:    
            self.dt[0] = dailybook[-1][0]
            self.prices[1] = self.prices[0]
            self.prices[0] = dailybook[-1][1]
            p1 = self.prices[0]
            p2 = self.prices[1]
            updated=True
        if updated:            
            self.values.append( ( self.dt[0].date(), p1 ) )
            for listener in self.listeners: listener.on_indicator_update( self.identifier, self.values )
