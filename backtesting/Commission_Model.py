class Commission_Model:
    def getcommission(product,amount):
        return execution_cost(product,amount)+pershare(product,amount)+pertrade(product)
  
    def execution_cost(product,amount):
        return (amount*amount/1000.0)
   
    def pershare(product,amount):
        return 0.5*amount

    def pertrade(product):
        return 1
        
        

