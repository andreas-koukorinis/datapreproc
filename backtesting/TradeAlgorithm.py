import sys

class TradeAlgorithm(object):
    '''
    Base class for strategy development
    User should inherit this class and override init and OnEventListener functions
    '''
    def __init__(self,order_manager,portfolio,performance_tracker,products,conversion_factor):
        self.order_manager = order_manager
        self.portfolio = portfolio
        self.performance_tracker = performance_tracker
        self.products=products
        self.conversion_factor = conversion_factor
        self.pton = {}                                                                               #Product to number mapping
        self.ntop = {}                                                                               #Number to product mapping
        i=0
        for product in products:                                                                     #Mapping between products and number for easy calculation
            self.pton[product]=i
            self.ntop[str(i)]=product
            i=i+1
        self.init()

    #User is expected to write the function
    def onEventListener(self,data):
        pass										  

    #Place an order to buy/sell 'num_shares' shares of 'product'
    #If num_shares is +ve -> it is a buy trade
    #If num_shares is -ve -> it is a sell trade
    def place_order(self,dt,product,num_shares):
        self.order_manager.place_order(dt,product,num_shares)	                           	   #Pass the order to the order manager

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
