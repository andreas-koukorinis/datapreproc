from CommissionManager import CommissionManager
      
class BackTester:
    def __init__(self,product,portfolio,performance_tracker,conversion_factor):
        self.product=product
        self.portfolio = portfolio
        self.performance_tracker = performance_tracker
        self.conversion_factor = conversion_factor
        self.pending_orders = []
        self.commission_manager = CommissionManager()

    #Append the orders to the pending_list.
    #ASSUMPTION:Orders will be filled on the next event
    def sendOrder(self,order):
        self.printOrders(0,order)
        self.pending_orders.append(order)

    #Check which of the pending orders have been filled
    #ASSUMPTION: all the pending orders are filled #SHOULD BE CHANGED
    def updatePendingOrders(self,book,date,track,is_settlement):         
        filled_orders = []
        for order in self.pending_orders:
            if(True):										#should check if order can be filled based on current book,if yes remove from                                                                                                           pending_list and add to filled_list
                cost = self.commission_manager.getcommission(order,book)
                if(is_settlement):
                    fill_price = book[-2][1]                                                    #for settlement orders,estimated fill_price = price at which order is placed
                else:
                    fill_price = book[-1][1]*0.1 + book[-2][1]*0.9                              #estimated fill_price = 0.9*(price at which order is placed) + 0.1*(price on next day)
                value = fill_price*order['amount']*self.conversion_factor		        #assuming that book is of the format [(dt,prices)]     #+ve for buy,-ve for sell
                '''print order,date
                print self.portfolio.get_portfolio()['num_shares']
                print self.portfolio.get_portfolio()['cash'],value,cost'''
                #assert self.portfolio.get_portfolio()['cash']-value-cost >=0			#check whether the account has enough cash for the order to be filled		
                filled_orders.append({'dt':order['dt'],'product':order['product'],'amount':order['amount'],'cost':cost,'value':value})
                self.pending_orders.remove(order)						#should see what happens for duplicates/iteration
        if(len(filled_orders)>0):                                                            
            self.portfolio.update_portfolio(filled_orders)                                      #If some orders have been filled,then update the portfolio
            self.printOrders(1,filled_orders)
        if(track==1):
            self.performance_tracker.analyze(filled_orders,date)                                #Pass control to the performance tracker,pass date to track the daily performance
        if(is_settlement and self.product[-1]=='1'):
            self.switch_symbols_on_settlement(self.product)

    def switch_symbols_on_settlement(self,product):
        p1 = product
        p2 = product.rstrip('1')+'2'
        assert self.portfolio.num_shares[p1]==0
        self.portfolio.num_shares[p1] = self.portfolio.num_shares[p2]
        self.portfolio.num_shares[p2] = 0
        assert self.performance_tracker.num_shares[p1]==0
        self.performance_tracker.num_shares[p1]=self.performance_tracker.num_shares[p2]
        self.performance_tracker.num_shares[p2]=0

    #TO BE COMPELETD
    def updateOrderManager(self,filled_orders):
        pass

    #Print orders for debugging
    def printOrders(self,typ,orders):
        if(len(orders)==0):
            return
        if(typ==0):
            print 'ORDER PLACED : datetime:%s product:%s amount:%f'%(orders['dt'],orders['product'],orders['amount'])
        else:
            s = 'ORDER FILLED : '
            for order in orders:
                s = s + 'product:%s amount:%f cost:%f value:%f'%(order['product'],order['amount'],order['cost'],order['value'])
            print s  
