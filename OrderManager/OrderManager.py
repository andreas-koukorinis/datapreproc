import os
from importlib import import_module
from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from Utils import defaults

''' OrderManager listens to the Strategy for any send/cancel orders
 The job of the order manager is to
 1)Get the orders from the strategy
 2)Pass the orders to the Backtester/Exchange
 3)Pring all the placed orders to a file : positions_file'''
class OrderManager():

    instance = []

    def __init__(self, products, _startdate, _enddate, _config, _log_filename):
        self.products = products
        self.log_filename = _log_filename
        if _config.has_option('Parameters', 'debug_level'):
            self.debug_level = _config.getint('Parameters','debug_level')
        else:
            self.debug_level = defaults.DEBUG_LEVEL  # Default value of debug level,in case not specified in config file
        if self.debug_level > 0:
            self.positions_file = 'logs/'+_log_filename+'/positions.txt'
            open( self.positions_file, 'w' ).close()  # Empty the positions_file,if not present create it
        self.all_orders = []  # List of all orders placed till now
        self.order_status = {} # Dict mapping an order id to its status : 0:placed but not filled, 1:placed and filled, 2:placed but cancelled
        self.order_id = 0	# The unique id that will be assigned to the next order
        self.listeners = []
        self.backtesters = {}
        self.to_be_filled = {} # The net amount(in number of shares) of orders for each product which have been placed but not yet filled
        for product in self.products:
            self.to_be_filled[product] = 0
            self.backtesters[product] = BackTester.get_unique_instance( product, _startdate, _enddate, _config )
            self.backtesters[product].add_listener( self ) # Listen for filled orders

    @staticmethod
    def get_unique_instance(products, _startdate, _enddate, _config, _log_filename):
        if len(OrderManager.instance) == 0 :
            new_instance = OrderManager(products, _startdate, _enddate, _config, _log_filename)
            OrderManager.instance.append(new_instance)
        return OrderManager.instance[0]

    def add_listener(self, listener):
        self.listeners.append(listener)

    def send_order(self, dt, product, amount):
        order = { 'id': self.order_id, 'dt' : dt, 'product' : product, 'amount' : amount }
        self.backtesters[order['product']].send_order(order) # Send the order to the corresponding BackTester
        if self.debug_level > 0:
            self.print_placed_order(order)
        self.all_orders.append(order) 
        self.order_status[self.order_id] = 0 # Order placed but not filled 
        self.to_be_filled[product] += amount
        self.order_id += 1

    #
    def send_order_agg(self, dt, product, amount):
        order = { 'id': self.order_id, 'dt' : dt, 'product' : product, 'amount' : amount }
        if self.debug_level > 0:
            self.print_placed_order(order)        
        self.backtesters[order['product']].send_order_agg(order) # Send the order to the corresponding BackTester
        self.all_orders.append(order)
        self.order_status[self.order_id] = 0 # Order placed but not filled 
        self.to_be_filled[product] += amount
        self.order_id += 1

    def cancel_order(self, current_dt, order_id):
        order = get_order_by_id(order_id)
        self.backtesters[order['product']].cancel_order(order_id)
        if self.debug_level > 0:
            self.print_cancelled_order(current_dt, order)
        self.order_status[order_id] = 2 # Order cancelled
        self.to_be_filled[order['product']] -= order['amount']

    # TODO should print filled orders here instead of performance tracker
    def on_order_update(self, filled_orders, dt):
        for order in filled_orders:
            self.order_status[order['id']] = 1 # Order filled 
            self.to_be_filled[order['product']] -= order['amount']
        if self.debug_level > 0:
            self.print_filled_orders(filled_orders, dt)

    def get_order_by_id( self, order_id ):
        for order in self.all_orders: # TODO should use BST,this is very inefficient
            if order['id'] == order_id:
                return order

    def print_filled_orders(self, filled_orders, dt):
        if len(filled_orders) == 0: return
        s = ''
        for order in filled_orders:
            s = s+ 'ORDER FILLED ON %s: '%dt.date()
            s = s + 'id: %d  product: %s  amount: %0.5f  cost: %0.2f  value: %0.2f  fill_price: %0.2f'%(order['id'], order['product'], order['amount'], order['cost'], order['value'], order['fill_price'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()

    # Print placed orders to positions_file
    def print_placed_order(self, order):
        s = 'ORDER PLACED ON %s: id: %d product: %s amount: %0.5f' % ( order['dt'].date(), order['id'], order['product'], order['amount'] )
        text_file = open( self.positions_file, "a" )
        text_file.write("%s\n" % s)
        text_file.close()

    # Print orders to positions_file
    def print_cancelled_order(self, current_dt, order):
        s = 'ORDER CANCELLED ON %s: datetime:%s id: %d product: %s amount: %0.5f' % ( current_dt.date(), order['dt'], order['id'], order['product'], order['amount'] )
        text_file = open( self.positions_file, "a" )
        text_file.write("%s\n" % s)
        text_file.close()
