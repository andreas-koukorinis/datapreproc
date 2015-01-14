# cython: profile=True
from bookbuilder.bookbuilder_listeners import DailyBookListener
from bookbuilder.bookbuilder import BookBuilder
from commission_manager import CommissionManager
from utils.global_variables import Globals

# Backtester listens to the Book builder for daily updates
# OrderManager calls SendOrder and CancelOrder functions on it
# On any filled orders it calls the OnOrderUpdate on its listeners : Portfolio and Performance Tracker
# If the Backtester's product had a settlement day yesterday,then it will call AfterSettlementDay on its listeners so that the can account for the change in symbols
class BackTester(DailyBookListener):

    instances = {}

    def __init__(self, product, _startdate, _enddate, _config):
        self.product=product
        self.pending_orders = []
        self.conversion_factor = Globals.conversion_factor[product]
        _currency = Globals.product_to_currency[product]
        self.currency_factor = Globals.currency_factor[_currency]
        self.commission_manager = CommissionManager()
        self.listeners = []
        bookbuilder = BookBuilder.get_unique_instance(product, _startdate, _enddate, _config)
        bookbuilder.add_dailybook_listener(self)
        self.bb = bookbuilder # For filling aggressive order

    def add_listener(self, listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(product, _startdate, _enddate, _config):
        if product not in BackTester.instances.keys() :
            new_instance = BackTester(product, _startdate, _enddate, _config)
            BackTester.instances[product] = new_instance
        return BackTester.instances[product]

    # Append the orders to the pending_list.
    # ASSUMPTION:Orders will be filled on the next event
    def send_order(self, order):
        self.pending_orders.append(order)

    # TODO handle the case that order can be rejected
    def send_order_agg(self, order):
        _date = order['dt'].date()
        dailybook = self.bb.dailybook
        fill_price = dailybook[-1][1] # Use the most recent price for agg order
        order['value'] = fill_price * order['amount'] * self.conversion_factor * self.currency_factor[_date] # +ve for buy,-ve for sell
        cost = 0.0
        filled_order = { 'id': order['id'], 'dt' : order['dt'], 'product' : order['product'], 'amount' : order['amount'], 'cost' : cost, 'value' : order['value'], 'fill_price' : fill_price, 'type' : 'agg' }
        current_dt = dailybook[-1][0]
        # Here the listeners will be portfolio, performance tracker and order manager
        for listener in self.listeners:
            listener.on_order_update( [filled_order], current_dt )  # Pass control to the performance tracker,pass date to track the daily performance

    def cancel_order( self, order_id ):
        self.pending_orders[:] = [ order for order in self.pending_orders if order['id'] != order_id ] # TODO should use BST for pending order,currently this is O(n) for 1 cancel

    # Check which of the pending orders have been filled
    # ASSUMPTION: all the pending orders are filled #SHOULD BE CHANGED
    def on_dailybook_update(self, product, dailybook):
        _date = dailybook[-1][0].date()
        filled_orders = []
        updated_pending_orders = []
        for order in self.pending_orders:
            if True :  # Should check if order can be filled based on current book,if yes remove from pending_list and add to filled_list
                fill_price = dailybook[-1][3]
                order['value'] = fill_price * order['amount'] * self.conversion_factor * self.currency_factor[_date] # +ve for buy,-ve for sell
                cost = self.commission_manager.getcommission(order, dailybook)
                filled_orders.append( { 'id': order['id'], 'dt' : order['dt'], 'product' : order['product'], 'amount' : order['amount'], 'cost' : cost, 'value' : order['value'], 'fill_price' : fill_price , 'type' : 'normal'} )
            else:
                updated_pending_orders.append( order )
        self.pending_orders = updated_pending_orders
        current_dt = dailybook[-1][0]

        # Here the listeners will be portfolio, performance tracker and order manager
        for listener in self.listeners:
            listener.on_order_update( filled_orders, current_dt )  # Pass control to the performance tracker,pass date to track the daily performance
