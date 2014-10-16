from numpy import *

class CommissionManager:
    def getcommission(self,order,book):							#should check book
        return 0#self.execution_cost(order,book)+self.pershare(order)+self.pertrade(order)
  
    #Cost due to the price at which we were able to buy/sell the stock
    def execution_cost(self,order,book):
        return (abs(order['amount'])/1000.0)
   
    #Cost based on PerShare cost of the order
    def pershare(self,order):
        return 0.02*abs(order['amount'])

    #Cost based on PerTrade cost
    def pertrade(self,order):
        return 1
        
        

