import ConfigParser
from importlib import import_module
from BackTester.BackTester import BackTester

''' OrderManager listens to the Strategy for any send/cancel orders
 The job of the order manager is to
 1)Get the orders from the strategy
 2)Pass the orders to the Backtester/Exchange
 3)Pring all the placed orders to a file : positions_file'''
class OrderManager(object):

    instance=[]

    def __init__(self,products,config_file):
        _config = ConfigParser.ConfigParser()
        _config.readfp(open(config_file,'r'))
        self.products = products
        self.positions_file = _config.get('Files', 'positions_file')
        open(self.positions_file, 'w').close()  # Empty the positions_file,if not present create it
        self.all_orders=[]  # List of all orders placed till now
        self.count = 0	#Count of all orders placed till now
        self.listeners=[]
        self.backtesters={}
        for product in self.products:
            self.backtesters[product] = BackTester.get_unique_instance(product,config_file)

    @staticmethod
    def get_unique_instance(products,config_file):
        if(len(OrderManager.instance)==0):
            new_instance = OrderManager(products,config_file)
            OrderManager.instance.append(new_instance)
        return OrderManager.instance[0]

    def add_listener(self,listener):
        self.listeners.append(listener)

    def send_order(self,dt,product,amount):
        order = {'dt':dt,'product':product,'amount':amount}
        self.print_order(order)
        # Send the order to the relevant BackTester
        self.backtesters[order['product']].send_order(order)
        self.all_orders.append(order)
        self.count = self.count+1

    # TODO:
    def cancel_order(self):
        pass

    # Print orders to positions_file
    def print_order(self,order):
        s = 'ORDER PLACED : datetime:%s product:%s amount:%f'%(order['dt'],order['product'],order['amount'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()
