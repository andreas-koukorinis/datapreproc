import sys
from numpy import *
import datetime
from bookbuilder.bookbuilder_listeners import DailyBookListener
from bookbuilder.bookbuilder import BookBuilder
from Utils.Regular import get_next_futures_contract,get_first_futures_contract,is_future_entity,is_future
from Utils.global_variables import Globals
from datetime import timedelta

# Track the daily log returns for the product
class DailyLogReturns( DailyBookListener ):
    """Tracks the dailylogreturns of a product

       Identifier: DailyLogReturns.product_name
                   Eg: DailyLogReturns.fES or DailyLogReturns.AQRIX 

       Listeners: Many other indicators like StdDev, Trend
                  Apart from other indicators, simple performance tracker also listens to the daily log returns of the products in its portfolio

       Listening to: BookBuilder for dailybook updates
    """

    instances = {}

    def __init__( self, _identifier, _startdate, _enddate, _config ):
        """Initializes the required variables like identifier, last two prices etc
           and starts listening to products whose closing prices are needed to compute the dailylogreturns of a given product
           
           Args:
               _identifier(string): The identifier for this indicator. Eg: DailyLogReturns.fES
               _startdate(date object): The start date of the simulation
               _enddate(date object): The end date of the simulation
               _config(ConfigParser handle): The handle to the config file of the strategy
            
           Note:
               1) DailyLogReturns indicator will listen to dailybook updates for 2 products in case of futures, otherwise 1.
                  Eg: DailyLogReturns.fES will listen to dailybook updates of fES_1 and fES_2 
                      DailyLogReturns.AQRIX will listen to dailybook updates of AQRIX only
        """

        self.listeners = []
        self.values = []
        self.prices = [0,0]  # Remember last two prices for the product #prices[0] is latest
        self.identifier = _identifier
        params = self.identifier.strip().split('.')
        self.product = params[1]
        self.currency_factor = Globals.currency_factor
        self.product_to_currency = Globals.product_to_currency
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
        """Used by other classes to register as on_indicator_update listener of the dispatcher

           Args:
               listener(object): The object of the class which wants to listen

           Returns: Nothing       
        """
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( identifier, _startdate, _enddate, _config ):
        """This static function is used by other classes to add themselves as a listener to the DailyLogReturns"""
        if identifier not in DailyLogReturns.instances.keys() :
            new_instance = DailyLogReturns ( identifier, _startdate, _enddate, _config )
            DailyLogReturns.instances[identifier] = new_instance
        return DailyLogReturns.instances[identifier]

    # Update the daily log returns on each ENDOFDAY event
    def on_dailybook_update( self, product, dailybook ):
        """On a dailybook update, this function is called by the bookbuilder and the new logreturn is computed here
           On computation of a new log return all the dailylogreturns history is passed onto the listeners of this indicator
           in the format : list of tuples (date, logreturn)

           Args:
               product(string): The product for which this new dailybook update is 
               dailybook(list of tuples (date,price,is_last_trading_day)): The dailybook of the product 

           Returns: Nothing
        """ 
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
                '''if is_future(product):
                    _product = self.product1
                else:
                    _product = self.product
                c1 = self.currency_factor[self.product_to_currency[_product]].get(dailybook[-1][0].date(), 1.0) # default 1.0
                c2 = self.currency_factor[self.product_to_currency[_product]].get(dailybook[-1][0].date()+timedelta(days=-1), c1)
                #print c1,c2
                logret = log((p1*c1)/(p2*c2))'''
            else:
                logret = 0.0  # If last two prices not available for a product,let logreturn = 0
            if isnan(logret) or isinf(logret):
                print ("something wrong in DailyLogReturns")
                sys.exit(0)
            self.values.append((dailybook[-1][0].date(), logret))
            for listener in self.listeners: # Pass the dailylogreturn history onto the listeners
                listener.on_indicator_update( self.identifier, self.values )
