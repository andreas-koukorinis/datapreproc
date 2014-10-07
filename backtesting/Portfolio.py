from numpy import *

class Portfolio:
    def __init__(self,initial_capital,products):
        self.cash = initial_capital
        self.products = products
        self.num_shares = {}
        self.value = {}
        for product in products:
            self.num_shares[product] = 0
            self.value[product]=0

    #Return the portfolio variables as a dictionary
    def get_portfolio(self):
        return {'cash':self.cash,'num_shares':self.num_shares,'products':self.products}

    #cost_of_order : the commission and execution cost combined
    #value_of order : price*number_of_shares_bought/sold (-ve for sold,+ve for bought)
    #num_shares : +ve for buy,-ve for sell
    def update_portfolio(self,filled_orders):
        for order in filled_orders:
            self.cash = self.cash - order['value'] - order['cost']
            #assert self.cash >= 0
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']
