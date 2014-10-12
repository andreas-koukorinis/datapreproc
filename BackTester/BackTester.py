from BookBuilder.BookBuilder_Listeners import DailyBookListener
from BookBuilder.BookBuilder import BookBuilder
from CommissionManager import CommissionManager
from Utils.DbQueries import conv_factor

#Backtester listens to the Book builder for daily updates
#OrderManager calls SendOrder and CancelOrder functions on it
#On any filled orders it calls the OnOrderUpdate on its listeners : Portfolio and Performance Tracker
#If the Backtester's product had a settlement day yesterday,then it will call AfterSettlementDay on its listeners so that the can account for the change in symbols
class BackTester(DailyBookListener):

    instances={}

    def __init__(self,product,config_file):
        self.product=product
        self.pending_orders = []
        self.conversion_factor = conv_factor([product])[product]
        self.commission_manager = CommissionManager()
        self.listeners = []
        self.yesterday_settlement_day=False
        bookbuilder = BookBuilder.get_unique_instance(product,config_file)
        bookbuilder.AddDailyBookListener(self)

    def AddListener(self,listener):
        self.listeners.append(listener)

    @staticmethod
    def get_unique_instance(product,positions_file):
        if(product not in BackTester.instances.keys()):
            new_instance = BackTester(product,positions_file)
            BackTester.instances[product]=new_instance
        return BackTester.instances[product]

    #Append the orders to the pending_list.
    #ASSUMPTION:Orders will be filled on the next event
    def SendOrder(self,order):
        self.pending_orders.append(order)

    def CancelOrder(self,order):
        pass

    #Check which of the pending orders have been filled
    #ASSUMPTION: all the pending orders are filled #SHOULD BE CHANGED
    def OnDailyBookUpdate(self,product,dailybook,is_settlement_day):
        filled_orders = []
        for order in self.pending_orders:
            if(True):										#should check if order can be filled based on current book,if yes remove from                                                                                                           pending_list and add to filled_list
                cost = self.commission_manager.getcommission(order,dailybook)
                if(self.yesterday_settlement_day):
                    fill_price = dailybook[-2][1]                                               #for settlement orders,estimated fill_price = price at which order is placed
                else:
                    fill_price = dailybook[-1][1]*0.1 + dailybook[-2][1]*0.9                    #estimated fill_price = 0.9*(price at which order is placed) + 0.1*(price on next day)
                value = fill_price*order['amount']*self.conversion_factor		        #assuming that book is of the format [(dt,prices)]     #+ve for buy,-ve for sell
                filled_orders.append({'dt':order['dt'],'product':order['product'],'amount':order['amount'],'cost':cost,'value':value})
                self.pending_orders.remove(order)						#should see what happens for duplicates/iteration
        current_dt = dailybook[-1][0]

        # here the listeners will be portfolio and performance tracker
        for listener in self.listeners:
            listener.OnOrderUpdate(filled_orders,current_dt.date())                             #Pass control to the performance tracker,pass date to track the daily performance

        # The assumption here is that we only trade in the first futures contract
        # TODO : we need to change that
        # This seems to be something that needs to be moved to book.
        if(self.yesterday_settlement_day and self.product[-1]=='1'):                            #Only switch once for each product
            for listener in self.listeners:
                listener.AfterSettlementDay(self.product)
        self.yesterday_settlement_day=is_settlement_day
