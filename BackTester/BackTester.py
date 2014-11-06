from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder
from CommissionManager import CommissionManager
from Utils.DbQueries import conv_factor

# Backtester listens to the Book builder for daily updates
# OrderManager calls SendOrder and CancelOrder functions on it
# On any filled orders it calls the OnOrderUpdate on its listeners : Portfolio and Performance Tracker
# If the Backtester's product had a settlement day yesterday,then it will call AfterSettlementDay on its listeners so that the can account for the change in symbols
class BackTester( DailyBookListener ):

    instances = {}

    def __init__( self, product, _startdate, _enddate, _config):
        self.product=product
        self.pending_orders = []
        self.conversion_factor = conv_factor([product])[product]
        self.commission_manager = CommissionManager()
        self.listeners = []
        bookbuilder = BookBuilder.get_unique_instance( product, _startdate, _enddate, _config )
        bookbuilder.add_dailybook_listener( self )

    def add_listener( self, listener ):
        self.listeners.append( listener )

    @staticmethod
    def get_unique_instance( product, _startdate, _enddate, _config ):
        if product not in BackTester.instances.keys() :
            new_instance = BackTester( product, _startdate, _enddate, _config )
            BackTester.instances[product] = new_instance
        return BackTester.instances[product]

    # Append the orders to the pending_list.
    # ASSUMPTION:Orders will be filled on the next event
    def send_order( self, order ):
        self.pending_orders.append( order )

    def cancel_order( self, order ):
        pass

    # Check which of the pending orders have been filled
    # ASSUMPTION: all the pending orders are filled #SHOULD BE CHANGED
    def on_dailybook_update( self, product, dailybook ):
        filled_orders = []
        updated_pending_orders = []
        for order in self.pending_orders:
            if True :  # Should check if order can be filled based on current book,if yes remove from pending_list and add to filled_list
                cost = self.commission_manager.getcommission( order, dailybook )
                if dailybook[-2][2] : # If last trading day was a settlement day
                    fill_price = dailybook[-2][1]  # Estimated fill_price = price at which order is placed
                else:
                    #fill_price = dailybook[-1][1]*0.1 + dailybook[-2][1]*0.9  # Estimated fill_price = 0.9*(price at which order is placed) + 0.1*(price on next day)
                    fill_price = dailybook[-2][1]
                value = fill_price*order['amount']*self.conversion_factor  #Assuming that book is of the format [(dt,prices)]     # +ve for buy,-ve for sell
                filled_orders.append( { 'dt' : order['dt'], 'product' : order['product'], 'amount' : order['amount'], 'cost' : cost, 'value' : value, 'fill_price' : fill_price } )
            else:
                updated_pending_orders.append( order )
        self.pending_orders = updated_pending_orders
        current_dt = dailybook[-1][0]

        # Here the listeners will be portfolio and performance tracker
        for listener in self.listeners:
            listener.on_order_update( filled_orders, current_dt.date() )  # Pass control to the performance tracker,pass date to track the daily performance
