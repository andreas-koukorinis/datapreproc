import sys

class TradeAlgorithm(object):
    '''
    Base class for strategy development
    User should inherit this class and override init and OnEventListener functions
    
    Example of an Algorithm:
    import sys
    from TradeAlgorithm import TradeAlgorithm
    from numpy import *

    class TradeLogic(TradeAlgorithm):

        def init(self):
            self.daily_indicators = []
            self.intraday_indicators = []
            self.indicator_values = {}
            self.day=0

        #'events' is a list of concurrent events
        def OnEventListener(self,events):
            if(events[0]['type']=='ENDOFDAY'):
                self.day +=1
            self.place_order(events[0]['dt'],events[0]['product'],10)                                #place an order to buy 10 shares of product corresponding to event 0

    '''
    def __init__(self,order_manager,portfolio,performance_tracker,products):
        self.order_manager = order_manager
        self.portfolio = portfolio
        self.performance_tracker = performance_tracker
        self.products=products
        self.init()

    #User is expected to write the function
    def onEventListener(self,data):
        pass										  

    #Place an order to buy/sell 'num_shares' shares of 'product'
    #If num_shares is +ve -> it is a buy trade
    #If num_shares is -ve -> it is a sell trade
    def place_order(self,dt,product,num_shares):
        self.order_manager.place_order(dt,product,num_shares)					   #Pass the order to the order manager

    #Place an order to make the total number of shares of 'product' = 'target'
    #It can be a buy or a sell order depending on the current number of shares in the portfolio and the value of the target
    def place_order_target(self,dt,product,target):
        current_num = self.portfolio.get_portfolio()['num_shares'][product]                        #check what is product : id or integer index
        to_place = target-current_num
        if(to_place!=0):
            self.place_order(dt,product,to_place)

    #TO BE COMPLETED
    def cancel_order():									         
        pass


