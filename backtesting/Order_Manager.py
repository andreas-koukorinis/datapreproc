class Order_Manager():
    def __init__(self,algo):
        self.algo = algo
        self.all_orders=[]

    def place_order(self,product,amount):
        cost = self.algo.commission_mgr(product,amount)
        #execute order
        self.algo.new_orders.append([product,amount,cost])

    def place_order_target(self,product,target):
        current_num = self.algo.portfolio.get_portfolio(product,'number')
        to_place = target-current_num
        if(to_place!=0):
            place_order(product,to_place)      

    def cancel_order():
        pass
