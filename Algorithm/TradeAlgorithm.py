import sys
from importlib import import_module
from Dispatcher.Dispatcher import Dispatcher
from Dispatcher.Dispatcher_Listeners import EventsListener
from BookBuilder.BookBuilder import BookBuilder
from Utils.DbQueries import conv_factor

class TradeAlgorithm(EventsListener):

    instance=[] # Doubt { gchak } Why do we need "instance" in UnleveredRP if there is an instance here already ?

    '''
    Base class for strategy development
    User should inherit this class and override init and OnEventListener functions
    '''
    def __init__(self,products,config_file,StrategyClass):
        self.products=products
        self.init(config_file)
        self.Daily_Indicators={}
        self.conversion_factor=conv_factor(products)
        self.listeners=[]
        for indicator in self.daily_indicators:                                                    #Initialize daily indicators
            module = import_module('Indicators.'+indicator)
            Indicatorclass = getattr(module,indicator)
            self.Daily_Indicators[indicator] = Indicatorclass.get_unique_instance(products,config_file)                  #Instantiate indicator objects

        self.bb_objects={}
        for product in products:
            self.bb_objects[product] = BookBuilder.get_unique_instance(product,config_file)

        dispatcher = Dispatcher.get_unique_instance(products,config_file)
        dispatcher.AddEventsListener(self)

    @staticmethod
    def get_unique_instance(products,config_file,StrategyClass):
        if(len(StrategyClass.instance)==0):
            new_instance = StrategyClass(products,config_file,StrategyClass)
            StrategyClass.instance.append(new_instance)
        return StrategyClass.instance[0]

    def AddListener(self,listener):
        self.listeners.append(listener)

    #User is expected to write the function
    def OnEventsUpdate(self,concurrent_events):
        pass

    #Return the portfolio variables as a dictionary
    def get_portfolio(self):
        return {'cash':self.portfolio.cash,'num_shares':self.portfolio.num_shares,'products':self.portfolio.products}

    #Place an order to buy/sell 'num_shares' shares of 'product'
    #If num_shares is +ve -> it is a buy trade
    #If num_shares is -ve -> it is a sell trade
    def place_order(self,dt,product,num_shares):
        for listener in self.listeners:	                           	   #Pass the order to the order manager
            listener.OnSendOrder(dt,product,num_shares)

    #Place an order to make the total number of shares of 'product' = 'target'
    #It can be a buy or a sell order depending on the current number of shares in the portfolio and the value of the target
    def place_order_target(self,dt,product,target):
        current_num = self.portfolio.get_portfolio()['num_shares'][product]                        #check what is product : id or integer index
        to_place = target-current_num
        if(to_place!=0):
            self.place_order(dt,product,to_place)

    #Shift the positions from first futures contract to second futures contract on the settlement day
    def adjust_positions_for_settlements(self,events,current_price,positions_to_take):
        settlement_products=[]
        for event in events:
            if(event['is_settlement_day'] and event['product'][-1]=='1'): settlement_products.append(event['product'])
        for product in settlement_products:
            p1 = product                                                                            #Example: 'ES1'
            p2 = product.rstrip('1')+'2'                                                            #Example: 'ES2'
            positions_to_take[p2] = (positions_to_take[p1]*current_price[p1]*self.conversion_factor[p1])/(current_price[p2]*self.conversion_factor[p2])
            positions_to_take[p1] = 0
        return positions_to_take

    #TO BE COMPLETED
    def cancel_order():
        pass
