import ConfigParser
from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from numpy import *
from Utils.DbQueries import conv_factor

class Portfolio(BackTesterListener):

    instance=[]

    def __init__(self,products,config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file,'r'))
        self.cash = config.getfloat('Parameters', 'initial_capital')
        self.products = products
        self.num_shares = {}
        self.value = {}
        for product in products:
            self.num_shares[product] = 0
            self.value[product]=0
            backtester = BackTester.GetUniqueInstance(product,config_file)
            backtester.AddListener(self)

    @staticmethod
    def GetUniqueInstance(products,config_file):
        if(len(Portfolio.instance)==0):
            new_instance = Portfolio(products,config_file)
            Portfolio.instance.append(new_instance)
        return Portfolio.instance[0]


    #Return the portfolio variables as a dictionary
    def get_portfolio(self):
        return {'cash':self.cash,'num_shares':self.num_shares,'products':self.products}

    #cost_of_order : the commission and execution cost combined
    #value_of order : price*number_of_shares_bought/sold (-ve for sold,+ve for bought)
    #num_shares : +ve for buy,-ve for sell
    def OnOrderUpdate(self,filled_orders,date):
        for order in filled_orders:
            self.cash = self.cash - order['value'] - order['cost']
            #assert self.cash >= 0
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']

    def AfterSettlementDay(self,product):
        p1 = product
        p2 = product.rstrip('1')+'2'
        assert self.num_shares[p1]==0
        self.num_shares[p1] = self.num_shares[p2]
        self.num_shares[p2] = 0
