import sys

class TradeAlgorithm(object):
    '''
    Base class for strategy development
    User should inherit this class and override init and tradelogic functions
    
    Example of an Algorithm:
     

    '''
    def __init__(self,*args,**kwargs):
        self.initial_capital = kwargs.get('initial_capital',DEFAULT_INITIAL_CAPITAL)
        start_date = kwargs.get('start_date') if 'start_date' in kwargs else sys.exit('Start Date should be passed to TradeAlgorithm()')
        enddate = kwargs.get('end_date') if 'end_date' in kwargs else sys.exit('End Date should be passed to TradeAlgorithm()')
        products =kwargs.get('products')  if 'products' in kwars and len(kwargs.get('products'))>0 else sys.exit('Non-empty list of products should be passed')
        order_mgr = Order_Manager()
        perf_tracker = Performance_Tracker()
        portfolio = Portfolio()
        commission_mgr = Commission_Model()
        sim_params = {'products':products,'initial_capital':initial_capital,'start_date':start_date,'end_date':end_date}
        simulator = Simulator(sim_params)

    def init(context): 
        pass

    def onEventListener(self,data):
        pass

    def execute(self):
        self.init()
        self.simulator.simulate()
