import TradeLogic
from CommissionManager import CommissionManager
      
class BackTester:
    def __init__(self,product,portfolio,performance_tracker):
        self.product=product
        self.portfolio = portfolio
        self.performance_tracker = performance_tracker
        self.pending_orders = []
        self.commission_manager = CommissionManager()

    #Append the orders to the pending_list.
    #ASSUMPTION:Orders will be filled on the next event
    def sendOrder(self,order):
        #self.printOrders(0,order)
        self.pending_orders.append(order)

    #Check which of the pending orders have been filled
    #ASSUMPTION: all the pending orders are filled #SHOULD BE CHANGED
    def updatePendingOrders(self,book,date,track):         
        filled_orders = []
        for order in self.pending_orders:
            if(True):										#should check if order can be filled based on current book,if yes remove from                                                                                                           pending_list and add to filled_list
                cost = self.commission_manager.getcommission(order,book)
                value = book[len(book)-1][1]*order['amount']					#assuming that book is of the format [(dt,prices)]     #+ve for buy,-ve for sell
                assert self.portfolio.get_portfolio()['cash']-value-cost >=0			#check whether the account has enough cash for the order to be filled		
                filled_orders.append({'dt':order['dt'],'product':order['product'],'amount':order['amount'],'cost':cost,'value':value})
                self.pending_orders.remove(order)						#should see what happens for duplicates/iteration
        if(len(filled_orders)>0):                                                            
            self.portfolio.update_portfolio(filled_orders)                                      #If some orders have been filled,then update the portfolio
            #self.printOrders(1,filled_orders)
        if(track==1):
            self.performance_tracker.analyze(filled_orders,date)                                #Pass control to the performance tracker,pass date to track the daily performance

    #TO BE COMPELETD
    def updateOrderManager(self,filled_orders):
        pass

    #Print orders for debugging
    def printOrders(self,typ,orders):
        if(len(orders)==0):
            return
        if(typ==0):
            print 'ORDER PLACED'
            print orders['dt'],orders['product'],orders['amount']
        else:
            print 'ORDER FILLED'
            for order in orders:
                print order['dt'],order['product'],order['amount'],order['cost'],order['value']  
