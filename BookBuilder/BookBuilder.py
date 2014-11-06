from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import DailyEventListener
from Utils.Regular import get_all_products

'''BookBuilder listens to the dispatcher for daily update of its product
 Each product has a different bookbuilder
 The job of Book Builder is:
 Update the daily book[based on tuples(timestamp,closingprices)] on the 'ENDOFDAY' event corresponding to its product
 Call its Daily book listeners : Backtester,DailyLogReturn Indicator'''
class BookBuilder( DailyEventListener ):

    instances = {}

    def __init__( self, product, _startdate, _enddate, _config ):
        self.product=product
        self.dailybook=[]  # List of tuples (datetime,price,is_last_trading_day)
        self.intradaybook=[]  # List of tuples (type,size,price) # Type = 0 -> bid, type = 1 -> ask
        self.dailybook_listeners = []
        self.intradaybook_listeners = []
        self.settlement_listeners = []
        products = get_all_products( _config )
        dispatcher = Dispatcher.get_unique_instance( products, _startdate, _enddate, _config )
        dispatcher.add_event_listener( self, self.product )

    @staticmethod
    def get_unique_instance( product, _startdate, _enddate, _config ):
        if(product not in BookBuilder.instances.keys()):
            new_instance = BookBuilder( product, _startdate, _enddate, _config)
            BookBuilder.instances[product]=new_instance
        return BookBuilder.instances[product]

    def add_dailybook_listener(self,listener ):
        self.dailybook_listeners.append( listener )

    def add_intradaybook_listener( self, listener ):
        self.intradaybook_listeners.append( listener )

    def add_settlement_listener( self, listener ):
        self.settlement_listeners.append( listener )

    # Update the daily book with closing price and timestamp
    def on_daily_event_update( self, event ):
        self.dailybook.append( ( event['dt'], event['price'], event['is_last_trading_day'] ) ) 
        for listener in self.dailybook_listeners:
            listener.on_dailybook_update( self.product, self.dailybook )
        if len( self.dailybook ) > 1 and self.dailybook[-2][2] : # If the last trading day was a settlement day for this product
            for listener in self.settlement_listeners:
                listener.after_settlement_day( self.product )

    # TODO {sanchit}
    def on_intraday_event_update(self,event):
        pass
