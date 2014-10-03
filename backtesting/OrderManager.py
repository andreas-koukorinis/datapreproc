class OrderManager():
    def __init__(self,bt_objects):
        self.bt_objects = bt_objects
        self.all_orders=[]                                                              #List of all orders placed till now
        self.count = 0									#Count of all orders placed till now
    
    #Pass the order to the backtester
    def place_order(self,dt,product,amount):
        #execute order
        order = {'dt':dt,'product':product,'amount':amount}
        self.bt_objects[product].sendOrder(order)					#Send the order to the Backtester
        self.all_orders.append(order)
        self.count = self.count+1
      
    #TO BE COMPLETED:
    def cancel_order():
        pass
