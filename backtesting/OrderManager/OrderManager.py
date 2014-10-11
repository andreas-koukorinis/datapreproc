from Algorithm.TradeAlgorithm_Listeners import TradeAlgorithmListener
from Algorithm.TradeAlgorithm import TradeAlgorithm
import ConfigParser
from importlib import import_module

#OrderManager listens to the Strategy for any send/cancel orders
#The job of the order manager is to 
#1)Get the orders from the strategy
#2)Pass the orders to the Backtester/Exchange
#3)Pring all the placed orders to a file : positions_file

class OrderManager(TradeAlgorithmListener):

    instance=[]

    def __init__(self,config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))        
        products = config.get('Products', 'symbols').strip().split(",")
        self.positions_file = config.get('Files', 'positions_file')
        open(self.positions_file, 'w').close()
        self.all_orders=[]                                                              #List of all orders placed till now
        self.count = 0									#Count of all orders placed till now
        self.listeners=[]
        name = config.get('Strategy', 'name')                                                              #Remove .py from filename
        module = import_module('Strategies.'+name)                                                                  #Import the module corresponding to the filename
        TradeLogic = getattr(module,name)                                                             #Get the strategy class from the imported module 
        tradelogic = TradeLogic.GetUniqueInstance(products,config_file,TradeLogic)
        tradelogic.AddListener(self)

    @staticmethod
    def GetUniqueInstance(config_file): 
        if(len(OrderManager.instance)==0):
            new_instance = OrderManager(config_file)
            OrderManager.instance.append(new_instance)
        return OrderManager.instance[0]

    def AddListener(self,listener):
        self.listeners.append(listener)

    def OnSendOrder(self,dt,product,amount):
        order = {'dt':dt,'product':product,'amount':amount}
        self.printOrder(order)

        # the listeners of OrderManager are BackTester
        for listener in self.listeners:              					#Send the order to the Backtester
            if(listener.product==order['product']):
                listener.OnSendOrder(order)
        self.all_orders.append(order)
        self.count = self.count+1
      
    #TO BE COMPLETED:
    def OnCancelOrder(self):
        pass

    #Print orders to positions_file
    def printOrder(self,order):
        s = 'ORDER PLACED : datetime:%s product:%s amount:%f'%(order['dt'],order['product'],order['amount'])
        text_file = open(self.positions_file, "a")
        text_file.write("%s\n" % s)
        text_file.close()

